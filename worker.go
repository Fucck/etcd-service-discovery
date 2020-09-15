package main

import (
	"context"
	"encoding/json"
	client "go.etcd.io/etcd/clientv3"
	"log"
	"time"
)

type Worker struct {
	InstanceName string
	cli *client.Client
}

func NewWorker(instanceName string, endPoints []string) *Worker {
	cfg := client.Config{
		Endpoints: endPoints,
		DialTimeout: 5 * time.Second,
	}

	cli, err := client.New(cfg)
	if err != nil {
		log.Printf("Connect etcd endpoints error: %v", err)
		return nil
	}

	return &Worker{
		InstanceName: instanceName,
		cli: cli,
	}
}

func (w *Worker) HeartBeats(key string) {
	content, err := json.Marshal(Member{
		InstanceName: w.InstanceName,
	})
	if err != nil {
		log.Println(err)
		return
	}

	subKey := key + "/" + w.InstanceName
	lease, err := w.cli.Grant(context.TODO(), 10)
	if err != nil {
		log.Fatal(err)
		return
	}
	gRsp, err := w.cli.Get(context.TODO(), subKey)
	if err != nil {
		log.Fatal("get error", err)
		return
	}

	if gRsp.Count == 0 {
		log.Printf("%s is not set yet", subKey)
		_, err = w.cli.Put(context.TODO(), subKey, string(content), client.WithLease(lease.ID))
		if err != nil {
			log.Println("Error update workerInfo:", err)
		} else {
			log.Printf("%s is set right now", subKey)
		}
	} else {
		log.Println(subKey, "is set")
		// keep the lease
	}

	for {
		// keep alive the lease
		_, kaErr := w.cli.KeepAliveOnce(context.TODO(), lease.ID)
		if kaErr != nil {
			log.Println(kaErr)
		}
		time.Sleep(time.Second * 3)
	}
}