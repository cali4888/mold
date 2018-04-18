package main

import (
	"fmt"
	"testing"
	"time"

	"bytes"

	"io/ioutil"
	"strings"

	"github.com/docker/docker/api/types"
)

func Test_Worker_Configure(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold1.yml", "")
	if err := worker.Configure(testMc); err != nil {
		t.Fatal(err)
	}
	if len(worker.serviceStates) != len(testMc.Services) {
		t.Fatalf("service mismatch have=%d want=%d", len(worker.serviceStates), len(testMc.Services)-1)
		t.FailNow()
	}
	if len(worker.buildStates) != len(testMc.Build) {
		t.Fatal("service mismatch")
	}
	for _, s := range worker.serviceStates {
		if s.Name == "" {
			t.Fatal("name empty for container", s.Container.Image)
		}
	}
}

func Test_Worker_Configure_Cache(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold9.yml", "")
	if err := worker.Configure(testMc); err != nil {
		t.Fatal(err)
	}
	if len(worker.buildStates) != len(testMc.Build) {
		t.Fatal("service mismatch")
	}
	if worker.buildStates[0].cache == nil {
		t.Fatalf("cache tag value is not set")
	}

	if worker.buildStates[0].cache.Name != "cache-mold" {
		t.Fatalf("cache name value is not correct: %s", worker.buildStates[0].cache.Name)
	}
	if len(worker.buildStates[0].cache.Tag) != 64 {
		t.Fatalf("cache tag value is not set to the correct format: %s", worker.buildStates[0].cache.Tag)
	}

	if worker.buildStates[1].cache != nil {
		t.Fatalf("cache should be nil when not specified in the mold config")
	}
}

func Test_Worker_Build(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold2.yml", "")
	worker.Configure(testMc)

	if err := worker.Setup(); err != nil {
		t.Fatal(err)
	}

	if err := worker.Build(); err != nil {
		worker.Teardown()
		t.Fatal(err)
	}

	if err := worker.Teardown(); err != nil {
		t.Log(err)
		t.Fail()
	}

	for _, v := range worker.buildStates {
		t.Log(v.Name, v.Status())
	}
}

func Test_Worker_GeneratesArtifacts(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold1.yml", "")
	worker.Configure(testMc)

	if err := worker.GenerateArtifacts(); err != nil {
		t.Fatal(err)
	}
	if err := worker.RemoveArtifacts(); err != nil {
		t.Fatal(err)
	}
	if err := worker.GenerateArtifacts("d3sw/mold-test"); err != nil {
		t.Fatal(err)
	}
	worker.RemoveArtifacts()
	if err := worker.GenerateArtifacts("foo"); err == nil {
		t.Fatal("should fail with artifact not found")
	}
}

func Test_Worker_GeneratesArtifacts_Abort(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold1.yml", "")
	if err := worker.Configure(testMc); err != nil {
		t.Fatal(err)
	}

	go func() {
		if err := worker.GenerateArtifacts(); err != nil {
			t.Fatal(err)
		}
	}()

	<-time.After(1500 * time.Millisecond)
	if err := worker.Abort(); err != nil {
		t.Fatal(err)
	}
}

func Test_ServiceWatcher(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold10.yml", "")
	worker.Configure(testMc)

	var buf bytes.Buffer
	worker.log = &Log{Writer: &buf}

	if err := worker.Setup(); err != nil {
		t.Fatal(err)
	}

	go func() {
		time.Sleep(3 * time.Second)

		for _, v := range worker.serviceStates {
			worker.docker.StopContainer(v.id, 3*time.Second)
		}
	}()

	if err := worker.Build(); err != nil {
		worker.Teardown()
		t.Fatal(err)
	}

	if err := worker.Teardown(); err != nil {
		t.Log(err)
		t.Fail()
	}

	bytes, err := ioutil.ReadAll(&buf)

	if err != nil {
		t.Fatal(err)
	}
	log := string(bytes)

	if !strings.Contains(log, "[service/mold.warn.test...] [WARN] service down before build end. Received signal:") {
		t.Fatal("Service watcher didn't run")
	}

	for _, v := range worker.buildStates {
		t.Log(v.Name, v.Status())
	}
}

func Test_Worker_Publish_fail(t *testing.T) {
	_, worker, _ := initializeBuild(testMoldCfg, "")
	worker.authCfg = nil
	if err := worker.Publish(); err == nil {
		t.Fatal("should fail")
	}
}

func Test_Worker_Publish_fail2(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold4.yml", "")
	if err := worker.Configure(testMc); err != nil {
		t.Fatal(err)
	}

	if err := worker.Publish(); err == nil {
		t.Fatalf("should fail with image not found: %+v", testMc.Artifacts.Images)
	}
}

func Test_BuildListenOnPort(t *testing.T) {
	testMc, worker, _ := initializeBuild("./testdata/mold8.yml", "")
	if err := worker.Configure(testMc); err != nil {
		t.Fatal(err)
	}
	if err := worker.GenerateArtifacts(); err != nil {
		t.Fatal(err)
	}

}

func Test_getRegistryAuth(t *testing.T) {
	d := DockerWorker{}
	authCfg := d.getRegistryAuth("")
	if authCfg != nil {
		t.Fatal("registry auth config should be nil")
	}
	d.authCfg = &DockerAuthConfig{
		Auths: make(map[string]types.AuthConfig),
	}
	authCfg = d.getRegistryAuth("")
	if authCfg != nil {
		t.Fatal("registry auth config should be nil")
	}

	d.authCfg.Auths["docker"] = types.AuthConfig{Username: "UnknownEmpty"}
	authCfg = d.getRegistryAuth("test")
	if authCfg != nil {
		t.Fatal("registry auth config should be nil")
	}
}

func Test_Publish(t *testing.T) {
	d := DockerWorker{
		buildConfig: &MoldConfig{},
		authCfg: &DockerAuthConfig{
			Auths: make(map[string]types.AuthConfig, 0),
		},
	}

	d.authCfg = &DockerAuthConfig{
		Auths: make(map[string]types.AuthConfig, 0),
	}

	err := d.Publish("")
	if err.Error() != "registry auth not specified" {
		t.Fatal("should be error \"registry auth not specified\"")
	}

	err = d.Publish("uno,dos,tres")
	if err.Error() != "registry auth not specified" {
		t.Fatal("should be error \"registry auth not specified\"")
	}

	images := "раз,два,три"
	d.authCfg.Auths["docker"] = types.AuthConfig{Username: "UnknownEmpty"}
	err = d.Publish(images)
	if err.Error() != fmt.Sprintf("no such artifact: %s", images) {
		t.Fatal(err.Error())
	}
}
