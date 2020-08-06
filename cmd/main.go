package main

import (
	"flag"
        "io/ioutil"
        "log"

        "gopkg.in/yaml.v2"

        "github.com/lhzd863/autoflow-worker/worker"
        "github.com/lhzd863/autoflow-worker/module"
)

var (
	cfg   = flag.String("conf", "conf.yaml", "basic config")
        run   = flag.String("run", "worker", "worker")
)

func main() {
	flag.Parse()
        conf := new(module.MetaWorkerConf)
        yamlFile, err := ioutil.ReadFile(*cfg)
        if err != nil {
                log.Printf("error: %s", err)
                return
        }
        err = yaml.UnmarshalStrict(yamlFile, conf)
        if err != nil {
                log.Printf("error: %s", err)
                return
        }

	mpara := make(map[string]interface{})
	mpara["cfg"] = *cfg
        mpara["accesstoken"] = conf.AccessToken
        mpara["apiserverip"] = conf.ApiserverIp
        mpara["apiserverport"] = conf.ApiserverPort
	if *run == "worker" {
		ws := worker.NewWorkerServer(mpara)
		ws.Main()
		return
	}
}

