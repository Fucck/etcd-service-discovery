package main

import (
	"context"
	"encoding/json"
	"fmt"
	client "go.etcd.io/etcd/clientv3"
	"log"
	"strings"
	"time"
)

type Member struct {
	InstanceName string `json:"instance_name"`
}

type Master struct {
	members map[string]*Member
	cli *client.Client
}

func NewMaster(endpoints []string) *Master {
	cfg := client.Config{
		Endpoints: endpoints,
		DialTimeout: 5 * time.Second,
	}

	cli, err := client.New(cfg)
	if err != nil {
		log.Printf("Connect etcd endpoints error: %v", err)
		return nil
	}

	return &Master{
		members: map[string]*Member{},
		cli: cli,
	}
}

// Watch the worker and log out the changes
func (m *Master) WatchWorkers(key string){
	watcherChan := m.cli.Watch(context.TODO(), key, client.WithPrefix())

	for wRsp := range watcherChan {
		for _, event := range wRsp.Events { // one affairs include some events
			eventKey := string(event.Kv.Key)
			log.Printf("Key %s has changes", eventKey)

			if event.Type.String() == "PUT" {
				newMember := MemberUnmarshal(event.Kv.Value)
				if newMember == nil {
					continue
				}
				m.members[string(event.Kv.Key)] = newMember
				m.ShowInstance()
			}

			if event.Type.String() == "DELETE" {
				log.Println("delete key: ", string(event.Kv.Key))
				delete(m.members, string(event.Kv.Key))
				m.ShowInstance()
			}
		}
	}
}

func MemberUnmarshal(content []byte) *Member {
	member := new(Member)
	err := json.Unmarshal(content, member)
	if err != nil {
		log.Println(err)
		return nil
	}

	return member
}

func (m *Master) ShowInstance() {
	instanceNames := []string{}
	for _, member := range m.members {
		instanceNames = append(instanceNames, member.InstanceName)
	}
	membersStr := strings.Join(instanceNames, "\n")
	output := fmt.Sprintf("now we have: \n%v", membersStr)
	log.Println(output)
}