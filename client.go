package etcd

import (
	"context"
	"errors"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
)

/*
key：etcd prefix
value：map[string]string	key：etcd key
							value：etcd value
*/

var prefixMap map[string]map[string]string
var lock sync.RWMutex

type Client struct {
	Client *clientv3.Client
	Prefix string
}

func (this *Client) GetValue(key string) error {
	prefixMap = make(map[string]map[string]string)
	resp, err := this.Client.Get(context.Background(), key, clientv3.WithPrefix())
	if err != nil {
		return err
	}
	this.Prefix = key
	go this.Watcher()
	if resp == nil || len(resp.Kvs) == 0 {
		return errors.New("resp == nil || len(resp.Kvs) == 0")
	}
	for i := range resp.Kvs {
		if resp.Kvs[i] != nil && len(resp.Kvs[i].Value) != 0 {
			SetServiceMap(key, string(resp.Kvs[i].Key), string(resp.Kvs[i].Value))
		}
	}
	return nil
}

func (this *Client) Watcher() {
	rch := this.Client.Watch(context.Background(), this.Prefix, clientv3.WithPrefix())
	for wresp := range rch {
		for _, ev := range wresp.Events {
			switch ev.Type {
			case clientv3.EventTypePut:
				SetServiceMap(this.Prefix, string(ev.Kv.Key), string(ev.Kv.Value))
			case clientv3.EventTypeDelete:
				DelServiceMap(this.Prefix, string(ev.Kv.Key))
			}
		}
	}
}

func SetServiceMap(prefix, key, val string) {
	lock.Lock()
	defer lock.Unlock()
	if keyMap, ok := prefixMap[prefix]; ok {
		keyMap[key] = val
	} else {
		keyMap := make(map[string]string)
		keyMap[key] = val
		prefixMap[prefix] = keyMap
	}
}

func DelServiceMap(prefix, key string) {
	lock.Lock()
	defer lock.Unlock()
	if keyMap, ok := prefixMap[prefix]; ok {
		delete(keyMap, key)
	}
}

func GetServiceMap(prefix string) map[string]string {
	lock.RLock()
	defer lock.RUnlock()
	return prefixMap[prefix]
}

func NewClient(endpoints []string) (*Client, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 3 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	return &Client{
		Client: cli,
	}, nil
}
