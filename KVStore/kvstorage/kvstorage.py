import time
import random
from typing import Dict, Union, List
import logging
import grpc
from KVStore.protos.kv_store_pb2 import *
from KVStore.protos.kv_store_pb2_grpc import KVStoreServicer, KVStoreStub
import threading

from KVStore.protos.kv_store_shardmaster_pb2 import Role

EVENTUAL_CONSISTENCY_INTERVAL: int = 2

logger = logging.getLogger("KVStore")


class KVStorageService:

    def __init__(self):
        pass

    def get(self, key: int) -> str:
        pass

    def l_pop(self, key: int) -> str:
        pass

    def r_pop(self, key: int) -> str:
        pass

    def put(self, key: int, value: str):
        pass

    def append(self, key: int, value: str):
        pass

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        pass

    def transfer(self, keys_values: list):
        pass

    def add_replica(self, server: str):
        pass

    def remove_replica(self, server: str):
        pass


class KVStorageSimpleService(KVStorageService):

    def __init__(self):
        self.data = {}
        self.lock = threading.Lock()

    def get(self, key: int) -> Union[str, None]:
        if key in self.data:
            return self.data[key]
        else:
            return None

    def l_pop(self, key: int) -> Union[str, None]:
        self.lock.acquire()
        if key in self.data:
            value = self.data[key]
            self.data[key] = value[1:]
            self.lock.release()
            return value[0]
        else:
            self.lock.release()
            return None


    def r_pop(self, key: int) -> Union[str, None]:
        self.lock.acquire()
        if key in self.data:
            value = self.data[key]
            self.data[key] = value[:-1]
            self.lock.release()
            return value[-1]
        else:
            self.lock.release()
            return None

    def put(self, key: int, value: str):
        self.data[key] = value

    def append(self, key: int, value: str):
        self.lock.acquire()
        if key in self.data:
            self.data[key] += value
        else:
            self.data[key] = value
        self.lock.release()

    def redistribute(self, destination_server: str, lower_val: int, upper_val: int):
        keys_values = []
        for key in self.data:
            if key >= lower_val and key <= upper_val:
                keys_values.append(KeyValue(key=key, value=self.data[key]))
        self.transfer(keys_values)

    def transfer(self, keys_values: List[KeyValue]):
        with grpc.insecure_channel(destination_server) as channel:
            stub = KVStoreStub(channel)
            stub.Transfer(TransferRequest(keys_values=keys_values))
        


class KVStorageReplicasService(KVStorageSimpleService):
    role: Role

    def __init__(self, consistency_level: int):
        super().__init__()
        self.consistency_level = consistency_level
        """
        To fill with your code
        """

    def l_pop(self, key: int) -> str:
        """
        To fill with your code
        """

    def r_pop(self, key: int) -> str:
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

    def add_replica(self, server: str):
        """
        To fill with your code
        """

    def remove_replica(self, server: str):
        """
        To fill with your code
        """

    def set_role(self, role: Role):
        logger.info(f"Got role {role}")
        self.role = role


class KVStorageServicer(KVStoreServicer):

    def __init__(self, service: KVStorageService):
        self.storage_service = service
        """
        To fill with your code
        """

    def Get(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        value = self.storage_service.get(key)
        if value is None:
            return GetResponse()
        return GetResponse(value=value)

    def LPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        value = self.storage_service.l_pop(key)
        if value is None:
            return GetResponse()
        return GetResponse(value=value)

    def RPop(self, request: GetRequest, context) -> GetResponse:
        key = request.key
        value = self.storage_service.r_pop(key)
        if value is None:
            return GetResponse()
        return GetResponse(value=value)

    def Put(self, request: PutRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.put(key, value)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Append(self, request: AppendRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        key = request.key
        value = request.value
        self.storage_service.append(key, value)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def Redistribute(self, request: RedistributeRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.redistribute(request.destination_server, request.lower_val, request.upper_val)
        return google_dot_protobuf_dot_empty__pb2.Empty()
        

    def Transfer(self, request: TransferRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        self.storage_service.transfer(request.keys_values)
        return google_dot_protobuf_dot_empty__pb2.Empty()

    def AddReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """

    def RemoveReplica(self, request: ServerRequest, context) -> google_dot_protobuf_dot_empty__pb2.Empty:
        """
        To fill with your code
        """
