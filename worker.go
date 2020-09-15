package main

import (
	"context"
	"encoding/json"
	client "github.com/coreos/etcd/clientv3"
	"go.etcd.io/etcd/clientv3/concurrency"
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

// campaign leader
func (w *Worker) Campaign(election, prop string) {
	session, err := concurrency.NewSession(w.cli, concurrency.WithTTL(5))
	defer session.Close()
	if err != nil {
		log.Println(err)
		return
	}

	e := concurrency.NewElection(session, election)
	ctx := context.Background()
	for {
		leader, err := e.Leader(ctx)
		if err != nil {
			log.Println("get leader error:", err)
		} else {
			log.Println("now leader is: ", string(leader.Kvs[0].Value))
			if string(leader.Kvs[0].Value) == prop { // I am leader, go straight to the end of function
				log.Println("I'am the leader")
				goto waitForNextCircle
			}
		}

		if err = e.Campaign(ctx, prop); err != nil {
			log.Println(err)
			return
		}

		log.Printf("Now %s is leader", w.InstanceName)
		log.Println(e.Key())

		waitForNextCircle:
		time.Sleep(10 * time.Second)
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