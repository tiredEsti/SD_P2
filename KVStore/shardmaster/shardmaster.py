import logging
from KVStore.tests.utils import KEYS_LOWER_THRESHOLD, KEYS_UPPER_THRESHOLD
from KVStore.protos.kv_store_pb2 import RedistributeRequest, ServerRequest
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterServicer
from KVStore.protos.kv_store_shardmaster_pb2 import *
import grpc
import threading

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




def get_overlap(lower1: int, upper1: int, lower2: int, upper2: int):
    """ 
    Returns the overlap between the two intervals, if any.
    """
    if upper1 < lower2 or upper2 < lower1:
        return None
    else:
        return [max(lower1, lower2), min(upper1, upper2)]

class ShardMasterSimpleService(ShardMasterService):
    def __init__(self):
        self.servers = dict()
        self.lock = threading.Lock()

    
    def join(self, server: str):
        if len(self.servers) == 0:
            self.servers[server] = KEYS_UPPER_THRESHOLD
        else:
            num_keys = KEYS_UPPER_THRESHOLD//(len(self.servers)+1)

            new_servers = self.servers.copy()
            top = num_keys
            for s in self.servers:
                new_servers[s] = top
                top += num_keys
            
            new_servers[server] = KEYS_UPPER_THRESHOLD

            #Redistribute keys
            self.lock.acquire()
            min_key1 = KEYS_LOWER_THRESHOLD
            min_key2 = KEYS_LOWER_THRESHOLD
            aux = new_servers.items()
            for s, upper in self.servers.items():
                for s2, upper2 in aux:
                    if s != s2:
                        overlap = get_overlap(min_key1, upper, min_key2, upper2)
                        if overlap is not None:
                            channel = grpc.insecure_channel(s)
                            stub = KVStoreStub(channel)
                            req = RedistributeRequest(destination_server=s2, lower_val=overlap[0], upper_val=overlap[1])
                            stub.Redistribute(req)
                    min_key2 = upper2
                min_key1 = upper
            self.servers = new_servers
            self.lock.release()

        

    def leave(self, server: str):
        if len(self.servers) > 1:
            num_keys = KEYS_UPPER_THRESHOLD//(len(self.servers)-1)
            top = num_keys
            new_servers = self.servers.copy()
            new_servers.pop(server)
            for s in new_servers:
                new_servers[s] = top
                top += num_keys
            #Redistribute keys
            self.lock.acquire()
            min_key1 = KEYS_LOWER_THRESHOLD
            min_key2 = KEYS_LOWER_THRESHOLD
            aux = new_servers.items()
            for s, upper in self.servers.items():
                for s2, upper2 in aux:
                    if s != s2:
                        overlap = get_overlap(min_key1, upper, min_key2, upper2)
                        if overlap is not None:
                            channel = grpc.insecure_channel(s)
                            stub = KVStoreStub(channel)
                            req = RedistributeRequest(destination_server=s2, lower_val=overlap[0], upper_val=overlap[1])
                            stub.Redistribute(req)
                    min_key2 = upper2
                min_key1 = upper
            self.servers = new_servers
            self.lock.release()

    #Asks the shard master about the server that holds the provided key. Returns the corresponding server address. 
    def query(self, key: int) -> str:
        for s, upper in self.servers.items():
            if key >= upper:
                continue
            else:
                return s
        return ""
        

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
