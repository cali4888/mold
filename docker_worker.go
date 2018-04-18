package main

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"runtime"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
)

// default timeout when trying to stop a container.
const defaultStopTimeout int = 5

var errAborted = fmt.Errorf("aborted")

// DockerWorker performs a docker based build per the config
type DockerWorker struct {
	mu sync.Mutex

	buildConfig   *MoldConfig     // overall moldconfig
	serviceStates containerStates // service containers
	buildStates   containerStates // build containers
	netID         string          // network id to connect all containers to

	docker *Docker // docker helper client

	done    chan bool // when all builds are completed
	abort   chan bool // cancelled channel
	aborted bool      // whether the worker has begun shutdown

	stopServiceWatcher chan bool // when need to stop service watcher

	log *Log
	// Auth config for registry operations
	authCfg *DockerAuthConfig
}

// NewDockerWorker instantiates a new worker. If no client is provided and env.
// based client is used.
func NewDockerWorker(dcli *Docker) (d *DockerWorker, err error) {
	d = &DockerWorker{
		docker:             dcli,
		log:                &Log{Writer: os.Stdout},
		abort:              make(chan bool, 1),
		stopServiceWatcher: make(chan bool, 1),
	}
	// set up registry auth. pushes will not happen if failed
	if d.authCfg, err = readDockerAuthConfig(""); err != nil {
		log.Println("WRN", err)
		err = nil
	}

	if d.docker == nil {
		d.docker, err = NewDocker("")
	}
	return
}

// Configure the job. This converts the MoldConfig to the docker required datastructure normalizing
// values as needed.
func (dw *DockerWorker) Configure(cfg *MoldConfig) error {
	dw.mu.Lock()
	defer dw.mu.Unlock()

	dw.buildConfig = cfg

	// Build service container contfigs
	sc, err := assembleServiceContainers(cfg)
	if err != nil {
		return err
	}

	dw.serviceStates = make([]*containerState, len(sc))

	sNames, err := validateUserServiceNames(sc)
	if err != nil {
		return err
	}

	var newName string
	var counter int
	for i, s := range sc {
		// Initialize state
		cs := &containerState{ContainerConfig: s, Type: ServiceContainerType}
		if cs.Name == "" {
			for {
				// applying a counter to the end of service name for running equal services
				newName = fmt.Sprintf("%s.%s.auto%d", nameFromImageName(s.Container.Image), cfg.RepoName, counter)
				// increment last counter and run check again if such service name already set explicitly
				counter++
				if _, ok := sNames[newName]; ok {
					continue
				}
				break
			}
			cs.Name = newName
		}

		// Attach network
		cs.Network = dw.defaultNetConfig()
		dw.serviceStates[i] = cs
	}

	// Build build container configs
	bc, err := assembleBuildContainers(cfg)
	if err != nil {
		return fmt.Errorf("Could not assemble build container: %v", err)
	}
	dw.buildStates = make([]*containerState, len(bc))
	for i, s := range bc {
		cs := &containerState{
			ContainerConfig: s,
			Type:            BuildContainerType,
			save:            dw.buildConfig.Build[i].Save,
		}
		cs.Name = fmt.Sprintf("%s-%d-%d", dw.buildConfig.Name(), i, time.Now().UnixNano())
		cs.shortName = shortContainerName(cs.Name)
		cs.Network = dw.defaultNetConfig()

		if dw.buildConfig.Build[i].Cache {
			hash, err := getBuildHash(cs.ContainerConfig)
			if err != nil {
				return err
			}
			cs.cache = &cache{
				Name: fmt.Sprintf("cache-%s", dw.buildConfig.RepoName),
				Tag:  hash,
			}
		}
		dw.buildStates[i] = cs
	}

	return nil
}

// validation values that the user has set explicitly
func validateUserServiceNames(sc []*ContainerConfig) (map[string]bool, error) {
	var serviceNames = make(map[string]bool)
	for _, s := range sc {
		if s.Name == "" {
			continue
		}
		if _, ok := serviceNames[s.Name]; ok {
			return nil, fmt.Errorf("duplicate name [%s]; names  need to be unique", s.Name)
		}
		serviceNames[s.Name] = true
	}
	return serviceNames, nil
}

func assembleServiceContainers(mc *MoldConfig) ([]*ContainerConfig, error) {
	bcs := make([]*ContainerConfig, len(mc.Services))
	for i, b := range mc.Services {
		cc := DefaultContainerConfig(b.Image)
		cc.Container.Cmd = b.Commands
		cc.Host.Binds = b.Volumes
		env, err := b.GetEnvStrings()
		if err != nil {
			return nil, err
		}
		cc.Container.Env = env
		cc.Name = b.Name
		bcs[i] = cc
	}
	return bcs, nil
}

// assembleBuildContainers assembles container configs from user provided build config
func assembleBuildContainers(mc *MoldConfig) ([]*ContainerConfig, error) {
	bconts := make([]*ContainerConfig, len(mc.Build))
	for i, b := range mc.Build {
		cc := DefaultContainerConfig(b.Image)
		cc.Container.WorkingDir = b.Workdir
		cc.Host.Binds = b.Volumes

		exposedPorts, portBindings, err := nat.ParsePortSpecs(b.Ports)
		if err != nil {
			return nil, err
		}
		cc.Container.ExposedPorts = exposedPorts
		cc.Host.PortBindings = portBindings

		cc.Container.Volumes = map[string]struct{}{b.Workdir: struct{}{}}
		cc.Container.Cmd = []string{b.Shell, "-cex", b.BuildCmds()}

		env, err := b.GetEnvStrings()
		if err != nil {
			return nil, err
		}
		cc.Container.Env = env

		src := mc.Context
		if runtime.GOOS == "windows" {
			src = toDockerWinPath(src)
		}
		cc.Host.Mounts = []mount.Mount{
			mount.Mount{Target: b.Workdir, Source: src, Type: mount.TypeBind},
		}
		bconts[i] = cc

		// Mount docker.sock in container if requested.
		if mc.AllowDockerAccess {
			bconts[i].Container.Volumes[dockerSockFile] = struct{}{}
			bconts[i].Host.Mounts = append(bconts[i].Host.Mounts,
				mount.Mount{Target: dockerSockFile, Source: dockerSockFile, Type: mount.TypeBind})
		}
	}
	return bconts, nil
}

func (dw *DockerWorker) defaultNetConfig() *network.NetworkingConfig {
	return &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			dw.buildConfig.Name(): &network.EndpointSettings{
				NetworkID: dw.netID,
			},
		},
	}
}

// GenerateArtifacts builds docker images
func (dw *DockerWorker) GenerateArtifacts(names ...string) error {
	var ics []ImageConfig
	if len(names) == 0 {
		ics = dw.buildConfig.Artifacts.Images
	} else {
		for _, name := range names {
			a := dw.buildConfig.Artifacts.GetImage(name)
			if a == nil {
				return fmt.Errorf("no such artifact: %s", name)
			}
			ics = append(ics, *a)
		}
	}
	var err error
	for _, ic := range ics {
		if dw.aborted {
			return errAborted
		}
		dw.log.Write([]byte(fmt.Sprintf("[artifacts/%s] Building\n", ic.Name)))
		if err = dw.generateArtifact(&ic); err != nil {
			// stop the process without creating other artifacts
			break
		}
		dw.log.Write([]byte(fmt.Sprintf("[artifacts/%s] DONE\n", ic.Name)))
	}
	return err
}

func (dw *DockerWorker) generateArtifact(ic *ImageConfig) error {
	dw.done = make(chan bool)
	err := dw.docker.BuildImageAsync(ic, dw.log, fmt.Sprintf("[artifacts/%s]", ic.Name), dw.done)
	if err != nil {
		return err
	}
	select {
	case ok := <-dw.done:
		if ok {
			dw.log.Write([]byte("[artifacts] Completing...\n"))
		} else {
			dw.log.Write([]byte("[artifacts] Completing with error(s)...\n"))
			if err == nil {
				err = fmt.Errorf("[artifacts] Failed to create image %s", ic.Name)
			}
		}
	case <-dw.abort:
		dw.RemoveArtifacts()
		dw.log.Write([]byte("[artifacts] Aborting...\n"))
	}
	return err
}

// RemoveArtifacts removes all local artifacts it as definted in the config
func (dw *DockerWorker) RemoveArtifacts() error {
	var err error
	for _, a := range dw.buildConfig.Artifacts.Images {
		err = mergeErrors(err, dw.docker.RemoveImage(a.Name, true, a.CleanUp))
	}
	return err
}

func (dw *DockerWorker) getRegistryAuth(registry string) *types.AuthConfig {
	var auth *types.AuthConfig

	if dw.authCfg != nil {
		if registry == "" {
			auth = dw.authCfg.DockerHubAuth()
		} else {
			for rh, av := range dw.authCfg.Auths {
				if strings.HasSuffix(rh, registry) {
					auth = &av
					if auth.ServerAddress == "" {
						auth.ServerAddress = rh
					}
					break
				}
			}
		}
	}

	if auth != nil && auth.Auth != "" && auth.Username == "" {
		if s, err := base64.StdEncoding.DecodeString(auth.Auth); err == nil {
			a := strings.Split(string(s), ":")
			if len(a) == 2 {
				auth.Username = a[0]
				auth.Password = a[1]
			}
		}
	}

	return auth
}

// Publish the artifact/s based on the config
func (dw *DockerWorker) Publish(names ...string) error {
	if dw.authCfg == nil || len(dw.authCfg.Auths) == 0 || dw.buildConfig == nil {
		//dw.log.Write([]byte("[publish] Not publishing.  registry auth not specified\n"))
		return fmt.Errorf("registry auth not specified")
	}

	if len(names) == 0 {
		for _, v := range dw.buildConfig.Artifacts.Images {
			if dw.aborted {
				return errAborted
			}
			auth := dw.getRegistryAuth(v.Registry)

			regPaths := v.RegistryPaths()
			for _, rp := range regPaths {

				if err := dw.docker.PushImage(rp, auth, os.Stdout, fmt.Sprintf("[publish/%s]", rp)); err != nil {
					return err
				}

			}
		}
	} else {
		for _, name := range names {
			if dw.aborted {
				return errAborted
			}
			a := dw.buildConfig.Artifacts.GetImage(name)
			if a == nil {
				return fmt.Errorf("no such artifact: %s", name)
			}
			auth := dw.getRegistryAuth(a.Registry)

			regPaths := a.RegistryPaths()
			for _, rp := range regPaths {

				if err := dw.docker.PushImage(rp, auth, os.Stdout, fmt.Sprintf("[publish/%s]", rp)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// Build starts the build.  This is a blocking call. index defines one or more
// build steps to run.  They are in the order as seen in teh config. If no index
// is provided all builds are run
func (dw *DockerWorker) Build() error {
	if len(dw.buildStates) == 0 {
		return nil
	}

	done, err := dw.StartBuildAsync(true)
	if err != nil {
		return err
	}

	select {
	case <-done:
		for _, b := range dw.buildStates {
			if b.status != "success" {
				err = mergeErrors(err, fmt.Errorf("build failed: %s %s", b.Name, b.Container.Image))
			} else {
				if e := dw.cacheImage(*b); e != nil {
					err = mergeErrors(err, fmt.Errorf("cache failed:: %s", e))
				}
			}
		}

	case <-dw.abort:
		dw.log.Write([]byte("[build] Aborting...\n"))
		if e := dw.stopBuildContainer(); e != nil {
			dw.log.Write([]byte("ERR Stopping build containers:" + e.Error() + "\n"))
		}
	}

	dw.stopServiceWatcher <- true

	return err
}

func (dw *DockerWorker) stopBuildContainer() error {
	var err error
	for _, bc := range dw.buildStates {
		dw.log.Write([]byte("[build] Stopping container: " + bc.ID() + "\n"))
		err = mergeErrors(err, dw.docker.StopContainer(bc.ID(), time.Duration(defaultStopTimeout)*time.Second))
	}
	return err
}

// Abort cancels a running build
func (dw *DockerWorker) Abort() error {
	dw.mu.Lock()
	dw.aborted = true
	dw.mu.Unlock()

	dw.abort <- true
	return nil
}

// Setup sets up services needed to perform the build.  These are additional containers
// that are spun up.  If any error occurs the whole build will bail out
func (dw *DockerWorker) Setup() error {
	var err error
	if dw.netID, err = dw.docker.CreateNetwork(dw.buildConfig.Name()); err != nil {
		return err
		// network exists - so move on.
	}
	dw.log.Write([]byte(fmt.Sprintf("[configure/network/%s] Created %s\n", dw.buildConfig.Name(), dw.netID)))

	go dw.watchServices()

	// Start service containers
	for _, cs := range dw.serviceStates {
		if err := dw.docker.StartContainer(cs.ContainerConfig, dw.log, fmt.Sprintf("[setup/service/%s]", cs.Name)); err != nil {
			return err
		}
		dw.log.Write([]byte(fmt.Sprintf("[setup/service/%s] Started %s\n", cs.Name, cs.Container.Image)))
	}
	return nil
}

// StartBuildAsync starts the build container/s
func (dw *DockerWorker) StartBuildAsync(tailLog bool) (chan bool, error) {

	dw.done = make(chan bool)

	go dw.watchBuild()

	for _, cs := range dw.buildStates {
		if cs.cache.IsSet() {
			cacheImgName := cs.cache.ToString()
			if dw.docker.ImageAvailableLocally(cacheImgName) {
				cs.ContainerConfig.Container.Image = cacheImgName
			}
		}

		err := dw.docker.StartContainer(cs.ContainerConfig, dw.log, "")
		if err == nil {
			dw.log.WithField("container", cs.Name).Write([]byte(fmt.Sprintf("[build/%s...] Started \n", cs.shortName)))
			if cs.Type == BuildContainerType && tailLog {
				go func(csID, prefix string) {
					// wait otherwise docker may return a 404
					<-time.After(1000 * time.Millisecond)
					if e := dw.docker.TailLogs(csID, dw.log, prefix); e != nil {
						log.Println("ERR Failed to tail log", e)
					}
				}(cs.ID(), fmt.Sprintf("[build/%s...]", cs.shortName))
			}
			continue
		}
		return dw.done, err
	}
	return dw.done, nil
}

// cacheImage pushes the build image a registry
func (dw *DockerWorker) cacheImage(cs containerState) error {
	if cs.cache.IsSet() {
		img := cs.cache.ToString()
		if err := dw.docker.BuildImageOfContainer(cs.ID(), img); err != nil {
			return err
		}
	}
	return nil
}

// Teardown stops and removes all services spun up before the build as part of cleanup
func (dw *DockerWorker) Teardown() error {
	var err error
	// remove service containers
	for _, cs := range dw.serviceStates {
		e := dw.docker.RemoveContainer(cs.ID(), true)
		err = mergeErrors(err, e)
	}
	// remove build containers.
	for _, cs := range dw.buildStates {
		if !cs.save {
			e := dw.docker.RemoveContainer(cs.ID(), true)
			err = mergeErrors(err, e)
		}
	}

	// remove build image if 'cleanup' flag was setted
	for _, bImage := range dw.buildConfig.Build {
		if bImage.CleanUp == true {
			id, err := dw.getImageID(bImage.Image)
			if err != nil {
				continue
			}
			if err = mergeErrors(err, dw.docker.RemoveImage(id, true, bImage.CleanUp)); err != nil {
				log.Printf("ERR [Teardown] Removing image %s: %s\n", bImage.Image, err.Error())
			}
		}
	}

	err = mergeErrors(err, dw.docker.RemoveNetwork(dw.netID))

	for _, a := range dw.buildConfig.Artifacts.Images {
		if a.CleanUp {
			id, err := dw.getImageID(a.Name)
			if err != nil {
				continue
			}
			if err = mergeErrors(err, dw.docker.RemoveImage(id, true, a.CleanUp)); err != nil {
				log.Println("ERR [Teardown] Removing images:", err.Error())
			}
		}
	}
	return err
}

// getImageID returns image  ID by repository name
func (dw *DockerWorker) getImageID(repoName string) (string, error) {
	// adding default tag "latest" if there are no tags
	if len(strings.Split(repoName, ":")) == 1 {
		repoName += ":latest"
	}
	imagesInfo, err := dw.docker.cli.ImageList(context.Background(), types.ImageListOptions{All: true})
	if err != nil {
		return "", err
	}
	for _, i := range imagesInfo {
		for _, repoTag := range i.RepoTags {
			if repoName == repoTag {
				return i.ID, nil
			}
		}
	}
	return "", errors.New("no such image")
}

// TODO: add locking???
// markContainerDone marks the container as done.  Return if all the build containers have completed
func (dw *DockerWorker) markContainerDone(id, status string, state *types.ContainerState) bool {
	for i, v := range dw.buildStates {
		if v.ID() == id {
			dw.mu.Lock()
			dw.buildStates[i].done = true

			if len(status) > 0 {
				dw.buildStates[i].status = status
			}
			if state != nil {
				dw.buildStates[i].state = state
			} else {
				dw.buildStates[i].state = &types.ContainerState{Running: false}
			}
			dw.mu.Unlock()
			dw.log.Write([]byte(fmt.Sprintf("[build/%s...] DONE\n", v.shortName)))
		}
	}
	// check if all builds are done
	for _, v := range dw.buildStates {
		if !v.done {
			return false
		}
	}
	dw.done <- true
	return true
}

func (dw *DockerWorker) watchBuild() {
	cli := dw.docker.Client()
	msgCh, errCh := cli.Events(context.Background(), types.EventsOptions{})
	for {
		select {
		case msg := <-msgCh:

			switch msg.Action {
			case "destroy":
				// Check if we are interested in this container
				if c := dw.buildStates.Get(msg.Actor.ID); c != nil {
					// Breakout if the whole build is done.  This does not update the status
					// and is there more so the build doesn't block forever in case of failures
					if dw.markContainerDone(msg.Actor.ID, "", nil) {
						return
					}
				}

			case "die", "kill", "stop":
				// Check if we are interested in this container
				if c := dw.buildStates.Get(msg.Actor.ID); c != nil {
					var (
						status string
						state  types.ContainerState
					)
					if cj, err := cli.ContainerInspect(context.Background(), msg.Actor.ID); err == nil {
						if cj.State.ExitCode != 0 {
							status = "failed"
						} else {
							status = "success"
						}
						state = *cj.State
					} else {
						status = msg.Action
					}
					// breakout if the whole build is done
					if dw.markContainerDone(msg.Actor.ID, status, &state) {
						return
					}
				}
			}

		case err := <-errCh:
			log.Println("ERR", err)

		}
	}
}

func (dw *DockerWorker) watchServices() {
	cli := dw.docker.Client()
	msgCh, errCh := cli.Events(context.Background(), types.EventsOptions{})
	for {
		select {
		case <-dw.stopServiceWatcher:
			log.Println("[DEBUG] service watcher stopped")
			return
		case msg := <-msgCh:
			switch msg.Action {
			case "die", "kill", "stop", "destroy":
				// Check if we are interested in this container
				if c := dw.serviceStates.Get(msg.Actor.ID); c != nil {
					dw.log.Write([]byte(fmt.Sprintf("[service/%s...] [WARN] service down before build end. Received signal: %s\n", c.Name, msg.Action)))
				}
			}
		case err := <-errCh:
			log.Println("ERR", err)
		}
	}
}
