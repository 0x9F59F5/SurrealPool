import asyncio
import httpx
import json
from typing import Optional, List, Type, Any, Dict, Tuple
from dataclasses import dataclass


@dataclass
class SurrealResponse:
    time: str
    status: str
    result: List[Dict[str, Any]]


class SurrealHttpClient:

    def __init__(
            self,
            url: str,
            user: str,
            password: Optional[str],
            namespace: Optional[str],
            database: Optional[str]
    ) -> None:
        self.BASE_URL = url
        self.user = user
        self.password = password
        self.namespace = namespace
        self.database = database
        self._http = None

    async def connect(self):
        self._http = httpx.AsyncClient(
            base_url=self.BASE_URL,
            auth=httpx.BasicAuth(
                username=self.user,
                password=self.password
            ),
            headers={
                "surreal-ns": self.namespace,
                "surreal-db": self.database,
                "Accept": "application/json",
                "Content-Type": "application/json"
            }
        )

    async def _request(self, method: str, uri: str, data: Optional[str] = None,params: Optional[Any] = None,) -> SurrealResponse:
        if self._http is None:
            await self.connect()
        surreal_response = await self._http.request(
            method=method,
            url=uri,
            content=data,
            params=params,
        )
        surreal_data = await surreal_response.aread()
        return json.loads(surreal_data)

    async def __aenter__(self) -> 'SurrealHttpClient':
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()

    async def close(self) -> None:
        if self._http:
            await self._http.aclose()

    def is_connected(self) -> bool:
        return self._http is not None