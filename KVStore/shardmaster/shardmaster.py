import logging
from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *

logger = logging.getLogger(__name__)


class ShardMasterService:
    def join(self, server: str):
        pass

    def leave(self, server: str):
        pass

    def query(self, key: int) -> str:
        pass

    def join_replica(self, server: str) -> Role:
        pass

    def query_replica(self, key: int, op: Operation) -> str:
        pass


class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self.servers = []
        self.max = 100
        
    def join(self, server: str):
        keys = int(self.max / len(self.servers))
        for i in range(len(self.servers)):
            with grpc.insecure_channel(self.servers[i]) as channel:
                stub = KVStoreStub(channel)
                stub.redistribute(RedistributeRequest(lower=keys * i, upper=keys * (i + 1), destination_server=server))
        self.servers.append(server)
        

    def leave(self, server: str):
        if server not in self.servers:
            return
        keys = int(self.max / len(self.servers))
        for i in range(len(self.servers)):
            with grpc.insecure_channel(self.servers[i]) as channel:
                stub = KVStoreStub(channel)
                stub.redistribute(RedistributeRequest(lower=keys * i, upper=keys * (i + 1), destination_server=server))
        self.servers.remove(server)

    def query(self, key: int) -> str:
        if key < 0 or key > self.max:
            return ""
        for i in range(len(self.servers)):
            if key < (i + 1) * int(self.max / len(self.servers)):
                with grpc.insecure_channel(self.servers[i]) as channel:
                    stub = KVStoreStub(channel)
                    return stub.get(ServerRequest(key=key)).value


class ShardMasterReplicasService(ShardMasterSimpleService):
    def __init__(self, number_of_shards: int):
        super().__init__()
        """
        To fill with your code
        """

    def leave(self, server: str):
        """
        To fill with your code
        """

    def join_replica(self, server: str) -> Role:
        """
        To fill with your code
        """

    def query_replica(self, key: int, op: Operation) -> str:
        """
        To fill with your code
        """


class ShardMasterServicer(ShardMasterServicer):
    def __init__(self, shard_master_service: ShardMasterService):
        self.shard_master_service = shard_master_service
        """
        To fill with your code
        """

    def Join(self, request: JoinRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_master_service.join(request.server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Leave(self, request: LeaveRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.shard_master_service.leave(request.server)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Query(self, request: QueryRequest, context) -> QueryResponse:
        return QueryResponse(server=self.shard_master_service.query(request.key))

    def JoinReplica(self, request: JoinRequest, context) -> JoinReplicaResponse:
        """
        To fill with your code
        """

    def QueryReplica(self, request: QueryReplicaRequest, context) -> QueryResponse:
        """
        To fill with your code
        """
