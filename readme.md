# 使用etcd做服务发现demo

demo可以理解为一个分布式的服务，服务中的实例分为两种角色，分别为1)master 2)worker。worker是主要提供服务的单元，而master负责维护worker的
列表，当worker中的实例状态异常时（如进程关闭、死锁），master能通过etcd感知，并更新自己的worker列表。

