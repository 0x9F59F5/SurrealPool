import asyncio
import json
import logging
import httpx
from typing import Optional, List, Type, Any, Dict
from dataclasses import dataclass


@dataclass
class SurrealResponse:
    time: str
    status: str
    result: List[Dict[str, Any]]


class SurrealHttpClient:

    def __init__(self, base_url: str, user: str, password: str, namespace: str, database: str):
        self.BASE_URL = base_url
        self.user = user
        self.password = password
        self.namespace = namespace
        self.database = database
        self._conn = None

    async def connect(self):
        if self._conn is not None:
            return

        try:
            self._conn = httpx.AsyncClient(
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
        except httpx.HTTPError as e:
            logging.exception("HTTP Error connecting to SurrealDB")
            raise
        except Exception as e:
            logging.exception("Unexpected error connecting to SurrealDB")
            raise

    async def close(self):
        if self._conn:
            try:
                await self._conn.aclose()
            except Exception as e:
                logging.exception("Error closing connection")

    async def release(self):
        if self._conn:
            await self.close()
            self._conn = None
            logging.info("Connection released")

    def is_connected(self) -> bool:
        return self._conn is not None

    async def _request(self, method: str, uri: str, data: Optional[str] = None, params: Optional[Any] = None) -> SurrealResponse:
        if not self._conn:
            raise RuntimeError("Not connected to SurrealDB")

        try:
            response = await self._conn.request(
                method=method,
                url=uri,
                content=data,
                params=params
            )
            response_data = await response.aread()
            return json.loads(response_data)
        except httpx.HTTPError as e:
            logging.exception("HTTP error occurred")
            raise
        except json.JSONDecodeError as e:
            logging.exception("JSON decode error")
            raise
        except Exception as e:
            logging.exception("Unexpected error occurred")
            raise

    async def __aenter__(self) -> 'SurrealHttpClient':
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        await self.close()
