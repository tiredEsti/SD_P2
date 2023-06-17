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
        self.servers = {"address":[], "upper_key":[], "lower_key":[]}
        self.lock = threading.Lock()
    
    def join(self, server: str):
        if len(self.servers) == 0:
            self.servers["address"].append(server)
            self.servers["upper_key"].append(KEYS_UPPER_THRESHOLD)
            self.servers["lower_key"].append(KEYS_LOWER_THRESHOLD)
        else:
            return 0

        


        

    def leave(self, server: str):
        if len(self.servers) > 1:
            # Remove the server's address from the list of servers
            old_num_keys = int(KEYS_UPPER_THRESHOLD/(len(self.servers)))
            old_residue = KEYS_UPPER_THRESHOLD%len(self.servers)
            
            self.servers.remove(server)
            num_keys = int(KEYS_UPPER_THRESHOLD/(len(self.servers)))
            residue = KEYS_UPPER_THRESHOLD%len(self.servers)

            
        

    #Asks the shard master about the server that holds the provided key. Returns the corresponding server address. 
    def query(self, key: int) -> str:
        num_keys = int(KEYS_UPPER_THRESHOLD/len(self.servers))
        residue = KEYS_UPPER_THRESHOLD%len(self.servers)
        for i in range(len(self.servers)):
            if key <= (i+1)*num_keys + (residue>0):
                return self.servers[i]
        return self.servers[-1]
        

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
