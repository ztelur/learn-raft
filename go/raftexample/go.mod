module learn-raft-example

go 1.16

require (
	go.etcd.io/etcd/client/pkg/v3 v3.5.1
	go.etcd.io/etcd/raft/v3 v3.5.1
	go.etcd.io/etcd/server/v3 v3.5.1
	go.uber.org/zap v1.17.0
)

replace go.etcd.io/etcd/raft/v3 v3.5.1 => ../raft
