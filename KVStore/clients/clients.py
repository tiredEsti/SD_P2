from typing import Union, Dict
import grpc
import logging
from KVStore.protos.kv_store_pb2 import GetRequest, PutRequest, GetResponse
from KVStore.protos.kv_store_pb2_grpc import KVStoreStub
from KVStore.protos.kv_store_shardmaster_pb2 import QueryRequest, QueryResponse, QueryReplicaRequest, Operation
from KVStore.protos.kv_store_shardmaster_pb2_grpc import ShardMasterStub

logger = logging.getLogger(__name__)


def _get_return(ret: GetResponse) -> Union[str, None]:
    if ret.HasField("value"):
        return ret.value
    else:
        return None

class SimpleClient:
    def __init__(self, kvstore_address: str):
        self.channel = grpc.insecure_channel(kvstore_address)
        self.stub = KVStoreStub(self.channel)

    def get(self, key: int) -> Union[str, None]:
        res = self.stub.Get(GetRequest(key=key))
        ret = _get_return(res)
        return ret


    def l_pop(self, key: int) -> Union[str, None]:
        res = self.stub.LPop(GetRequest(key=key))
        ret = _get_return(res)
        return ret

    def r_pop(self, key: int) -> Union[str, None]:
        res = self.stub.RPop(GetRequest(key=key))
        ret = _get_return(res)
        return ret

    def put(self, key: int, value: str):
        req = PutRequest(key=key, value=value)
        res = self.stub.Put(req)

    def append(self, key: int, value: str):
        req = PutRequest(key=key, value=value)
        res = self.stub.Append(req)

    def stop(self):
        self.channel.close()


class ShardClient(SimpleClient):
    def __init__(self, shard_master_address: str):
        self.channel = grpc.insecure_channel(shard_master_address)
        self.stub = ShardMasterStub(self.channel)
        """
        To fill with your code
        """

    def get(self, key: int) -> Union[str, None]:
        query = QueryRequest(key=key)
        res = self.stub.Query(query)
        server = res.server


    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """



    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def put(self, key: int, value: str):
        """
        To fill with your code
        """


    def append(self, key: int, value: str):
        """
        To fill with your code
        """


class ShardReplicaClient(ShardClient):

    def get(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def r_pop(self, key: int) -> Union[str, None]:
        """
        To fill with your code
        """


    def put(self, key: int, value: str):
        """
        To fill with your code
        """


    def append(self, key: int, value: str):
        """
        To fill with your code
        """

