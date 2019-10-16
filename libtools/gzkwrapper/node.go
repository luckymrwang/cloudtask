package gzkwrapper

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.etcd.io/etcd/clientv3"
)

var flags = int32(0)

// var acl = zk.WorldACL(zk.PermAll)
var defaultTimeout = 10 * time.Second

type Node struct {
	Hosts []string
	// Conn  *zk.Conn
	Conn     *Client
	mutex    *sync.RWMutex
	wobjects map[string]*WatchObject
}

func NewNode(hosts string) *Node {
	return &Node{
		Hosts:    strings.Split(hosts, ","),
		Conn:     nil,
		mutex:    new(sync.RWMutex),
		wobjects: make(map[string]*WatchObject, 0),
	}
}

func (n *Node) Open() error {
	if n.Conn == nil {
		conn, err := clientv3.New(clientv3.Config{
			Endpoints:   n.Hosts,
			DialTimeout: defaultTimeout,
		})
		// conn, event, err := zk.Connect(n.Hosts, defaultTimeout)
		if err != nil {
			return err
		}
		// <-event
		n.Conn = &Client{
			Client:     conn,
			reqTimeout: defaultTimeout,
		}
	}

	return nil
}

func (n *Node) Close() {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	for path, wo := range n.wobjects {
		ReleaseWatchObject(wo)
		delete(n.wobjects, path)
	}

	if n.Conn != nil {
		n.Conn.Close()
		n.Conn = nil
	}
}

func (n *Node) Server() string {
	if n.Conn != nil {
		name := ""
		endPoints := n.Conn.Endpoints()
		if len(endPoints) > 0 {
			name = endPoints[len(endPoints)-1]
		}
		return name
	}
	return ""
}

// TODO:
func (n *Node) State() string {
	if n.Conn != nil {
		//return n.Conn.State().String()
		return ""
	}

	return ""
}

func (n *Node) WatchOpen(path string, callback WatchHandlerFunc) error {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if _, ret := n.wobjects[path]; ret {
		return nil
	}

	wo := CreateWatchObject(path, n.Conn, callback)
	if wo != nil {
		n.wobjects[path] = wo
		return nil
	}
	return errors.New("watch path failed.")
}

func (n *Node) WatchClose(path string) {
	n.mutex.Lock()
	defer n.mutex.Unlock()
	if wo, ret := n.wobjects[path]; ret {
		ReleaseWatchObject(wo)
		delete(n.wobjects, path)
	}
}

func (n *Node) Exists(path string) (bool, error) {
	if n.Conn == nil {
		return false, ErrNodeConnInvalid
	}

	resp, err := n.Conn.Get(path)
	if err != nil {
		return false, err
	}

	if len(resp.Kvs) == 0 {
		return false, nil
	}

	return true, nil
}

func (n *Node) Children(path string) ([]string, error) {
	if n.Conn == nil {
		return nil, ErrNodeConnInvalid
	}

	resp, err := n.Conn.Get(path, clientv3.WithPrefix())
	if err != nil {
		return nil, err
	}

	children := make([]string, 0)
	for _, ev := range resp.Kvs {
		key := string(ev.Key)
		if key == path {
			continue
		}

		children = append(children, strings.TrimPrefix(key, path+"/"))
	}

	return children, nil
}

func (n *Node) Get(path string) ([]byte, error) {
	if n.Conn == nil {
		return nil, ErrNodeConnInvalid
	}

	fmt.Println("node get start...", path)
	resp, err := n.Conn.Get(path)
	if err != nil {
		return nil, err
	}
	fmt.Println("node get end...", resp.Kvs)
	return resp.Kvs[0].Value, nil
}

func (n *Node) Create(path string, buffer []byte) error {
	if n.Conn == nil {
		return ErrNodeConnInvalid
	}

	//resp, err := n.Conn.Grant(10 + 2)
	//if err != nil {
	//	return err
	//}

	if _, err := n.Conn.Put(path, string(buffer)); err != nil {
		return err
	}

	return nil
}

func (n *Node) Remove(path string) error {
	if n.Conn != nil {
		_, err := n.Conn.Delete(path)
		return err
	}
	return ErrNodeConnInvalid
}

func (n *Node) Set(path string, buffer []byte) error {
	if n.Conn == nil {
		return ErrNodeConnInvalid
	}

	//_, err := n.Conn.Grant(10 + 2)
	//if err != nil {
	//	return err
	//}

	if _, err := n.Conn.Put(path, string(buffer)); err != nil {
		return err
	}

	return nil
}

type Client struct {
	*clientv3.Client
	reqTimeout time.Duration
}

// etcdTimeoutContext return better error info
type etcdTimeoutContext struct {
	context.Context

	etcdEndpoints []string
}

// NewEtcdTimeoutContext return a new etcdTimeoutContext
func NewEtcdTimeoutContext(c *Client) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), c.reqTimeout)
	etcdCtx := &etcdTimeoutContext{}
	etcdCtx.Context = ctx
	etcdCtx.etcdEndpoints = c.Endpoints()

	return etcdCtx, cancel
}

func (c *Client) Get(key string, opts ...clientv3.OpOption) (*clientv3.GetResponse, error) {
	ctx, cancel := NewEtcdTimeoutContext(c)
	defer cancel()
	return c.Client.Get(ctx, key, opts...)
}

func (c *Client) Put(key, val string, opts ...clientv3.OpOption) (*clientv3.PutResponse, error) {
	ctx, cancel := NewEtcdTimeoutContext(c)
	defer cancel()
	return c.Client.Put(ctx, key, val, opts...)
}

func (c *Client) Delete(key string, opts ...clientv3.OpOption) (*clientv3.DeleteResponse, error) {
	ctx, cancel := NewEtcdTimeoutContext(c)
	defer cancel()
	return c.Client.Delete(ctx, key, opts...)
}

func (c *Client) Grant(ttl int64) (*clientv3.LeaseGrantResponse, error) {
	ctx, cancel := NewEtcdTimeoutContext(c)
	defer cancel()
	return c.Client.Grant(ctx, ttl)
}

func (c *Client) Revoke(id clientv3.LeaseID) (*clientv3.LeaseRevokeResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), c.reqTimeout)
	defer cancel()
	return c.Client.Revoke(ctx, id)
}

func (c *Client) KeepAliveOnce(id clientv3.LeaseID) (*clientv3.LeaseKeepAliveResponse, error) {
	ctx, cancel := NewEtcdTimeoutContext(c)
	defer cancel()
	return c.Client.KeepAliveOnce(ctx, id)
}
