package main

import (
	"flag"
	"os"
)

var role = flag.String("r", "master", "master | worker")
var key = flag.String("k", "worker", "key for etcd")
var name = flag.String("instanceName", "", "instance name, unique please")

func init()  {
	flag.Parse()
	if *role != "master" && *role != "worker" {
		os.Exit(1)
	}
}

func main() {
	endpoints := []string{"http://127.0.0.1:2379"}
	switch *role {
	case "master":
		master := NewMaster(endpoints)
		master.WatchWorkers(*key)
	case "worker":
		worker := NewWorker(*name, endpoints)
		worker.HeartBeats(*key)
	default:
		os.Exit(-1)
	}
}
