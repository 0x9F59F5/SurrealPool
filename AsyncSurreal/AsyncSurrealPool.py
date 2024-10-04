import asyncio
from AsyncSurreal.AsyncSurrealHttp import SurrealHttpClient, SurrealResponse
from typing import Any, Dict, List, Optional
import json


class AsyncSurrealPool:

    def __init__(
            self,
            host,
            port,
            user,
            password,
            namespace,
            database,
            min_size,
            max_size,
    ):
        if min_size < 0:
            raise ValueError(
                'min_size is expected to be greater or equal to zero'
            )

        if max_size <= 0:
            raise ValueError(
                'max_size is expected to be greater than zero'
            )

        if min_size > max_size:
            raise ValueError(
                'min_size cannot be greater than max_size'
            )

        self._host = host
        self._port = port
        self._user = user
        self._password = password
        self._namespace = namespace
        self._database = database
        self._minsize = min_size
        self._maxsize = max_size

        self._holders = []
        self._queue = None

    async def _pool__init__(self):
        await self._initialize()
        return self

    async def _initialize(self):
        self._queue = asyncio.LifoQueue(maxsize=self._maxsize)
        for _ in range(self._maxsize):
            pool = SurrealHttpClient(
                base_url=f"http://{self._host}:{self._port}",
                user=self._user,
                password=self._password,
                namespace=self._namespace,
                database=self._database
            )

            self._holders.append(pool)
            self._queue.put_nowait(pool)

        if self._minsize:
            first_pool = self._holders[-1]  # type: SurrealHttpClient
            await first_pool.connect()

        if self._minsize > 1:
            connect_tasks = []
            for i, pool in enumerate(reversed(self._holders[:-1])):

                if i >= self._minsize - 1:
                    break
                connect_tasks.append(pool.connect())

            await asyncio.gather(*connect_tasks)

    def get_size(self):

        return sum(holders.is_connected() for holders in self._holders)

    async def acquire(self) -> SurrealHttpClient:
        client = await self._queue.get()
        if not client.is_connected():
            await client.connect()
        return client

    async def release(self, client: SurrealHttpClient):
        await self._queue.put(client)

    async def status(self):
        client = await self.acquire()
        try:
            response = await client._request(method="GET", uri="/version")
            return response

        finally:
            await self.release(client)

    async def query(self, sql: str, vars: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        client = await self.acquire()
        try:
            response = await client._request(method="POST", uri="/sql", data=sql, params=vars)
            return response
        except Exception as e:
            print(f"Error executing query: {e}")
            return []
        finally:
            await self.release(client)

    async def close(self):
        for client in self._holders:
            await client.close()

    def __await__(self):
        return self._pool__init__().__await__()

    async def __aenter__(self):
        await self._initialize()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


def create_pool(host, port, user, password, namespace, database):
    return AsyncSurrealPool(host, port, user, password, namespace, database, 1, 10)
