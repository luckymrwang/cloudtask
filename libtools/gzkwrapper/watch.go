package gzkwrapper

import (
	"context"
	"time"

	"go.etcd.io/etcd/clientv3"
)

const RetryInterval time.Duration = time.Second * 5

type WatchObject struct {
	Watcher clientv3.Watcher
	Path    string
	exit    chan bool
}

func CreateWatchObject(path string, conn *Client, callback WatchHandlerFunc) *WatchObject {
	if conn == nil {
		return nil
	}

	watchObject := &WatchObject{
		Watcher: clientv3.NewWatcher(conn.Client),
		Path:    path,
		exit:    make(chan bool),
	}

	go func(wo *WatchObject, c *Client, fn WatchHandlerFunc) {
		listen := true
		for listen {
			watchChan := wo.Watcher.Watch(context.TODO(), wo.Path)
			select {
			case <-watchChan:
				go func() {
					for val := range watchChan {
						for _, event := range val.Events {
							if callback != nil {
								callback(wo.Path, event.Kv.Value, nil)
							}
						}
					}
				}()
			case <-wo.exit:
				{
					listen = false
				}
			}
		}
	}(watchObject, conn, callback)

	return watchObject
}

/*
func CreateWatchObject(path string, conn *zk.Conn, callback WatchHandlerFunc) *WatchObject {

	if conn == nil {
		return nil
	}

	watchobject := &WatchObject{
		Path: path,
		exit: make(chan bool),
	}

	go func(wo *WatchObject, c *zk.Conn, fn WatchHandlerFunc) {
		listen := true
	NEW_WATCH:
		for listen {
			ret, _, ev, err := c.ExistsW(wo.Path)
			if err != nil {
				if callback != nil {
					callback(wo.Path, nil, err)
				}
				time.Sleep(RetryInterval)
				goto NEW_WATCH
			}

			select {
			case <-ev:
				{
					if ret {
						data, _, err := c.Get(wo.Path)
						if err != nil {
							if callback != nil {
								callback(wo.Path, nil, err)
							}
							time.Sleep(RetryInterval)
							goto NEW_WATCH
						}
						if callback != nil {
							callback(wo.Path, data, nil)
						}
					} else {
						if callback != nil {
							callback(wo.Path, nil, errors.New("watch exists not found."))
						}
						time.Sleep(RetryInterval)
						goto NEW_WATCH
					}
				}
			case <-wo.exit:
				{
					listen = false
				}
			}
		}
	}(watchobject, conn, callback)
	return watchobject
}
*/
func ReleaseWatchObject(wo *WatchObject) {
	if wo != nil {
		wo.exit <- true
		//close(wo.exit)
		wo.Watcher.Close()
	}
}
