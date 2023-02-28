from flyteidl.plugins import pytorch_pb2 as _pytorch_task

from flytekit.models import common as _common


class PyTorchJob(_common.FlyteIdlEntity):
    def __init__(
        self,
        workers_count,
        min_replicas,
        max_replicas,
        rdzv_backend,
        # rdzv_port,
        # rdzv_host,
        # standalone,
        n_proc_per_node,
        max_restarts, 
    ):
        self._workers_count = workers_count
        self._min_replicas = min_replicas
        self._max_replicas = max_replicas
        self._rdzv_backend = rdzv_backend
        # self._rdzv_port = rdzv_port
        # self._rdzv_host = rdzv_host
        # self._standalone = standalone
        self._n_proc_per_node = n_proc_per_node
        self._max_restarts = max_restarts

    @property
    def workers_count(self):
        return self._workers_count           

    @property
    def min_replicas(self):
        return self._min_replicas

    @property
    def max_replicas(self):
        return self._max_replicas

    @property
    def rdzv_backend(self):
        return self._rdzv_backend

    # @property
    # def rdzv_port(self):
    #     return self._rdzv_port

    # @property
    # def rdzv_host(self):
    #     return self._rdzv_host

    # @property
    # def standalone(self):
    #     return self._standalone

    @property
    def n_proc_per_node(self):
        return self._n_proc_per_node

    @property
    def max_restarts(self):
        return self._max_restarts                                                     

    def to_flyte_idl(self):
        return _pytorch_task.DistributedPyTorchTrainingTask(
            workers=self.workers_count,
            minReplicas=self.min_replicas,
            maxReplicas=self.max_replicas,
            RDZVBackend=self.rdzv_backend,
            # RDZVPort=self.rdzv_port,
            # RDZVHost=self.rdzv_host,
            # standalone=self.standalone,
            nProcPerNode=self.n_proc_per_node,
            maxRestarts=self.max_restarts,
        )

    @classmethod
    def from_flyte_idl(cls, pb2_object):
        return cls(
            workers_count=pb2_object.workers,
        )
