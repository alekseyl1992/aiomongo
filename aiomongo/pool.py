from asyncio import AbstractEventLoop
from contextlib import suppress

import asyncio

import logging
from pymongo.client_options import ClientOptions

from .connection import Connection


logger = logging.getLogger('aiomongo.client')


class Pool:
    def __init__(self, host: str, port: int, loop: AbstractEventLoop, options: ClientOptions):
        self.loop = loop
        self.options = options

        self.host = host
        self.port = port

        self._connected = False
        self._connections = []
        self._index = 0

    async def connect(self):
        connections = await asyncio.gather(
            *[Connection.create(
                self.loop, self.host, self.port, self.options
            ) for _ in range(self.options.pool_options.max_pool_size)],
            return_exceptions=True,
        )

        for conn in connections:
            if isinstance(conn, Exception):
                logger.exception(f'Unable to connect to {self.host}:{self.port}', exc_info=True)

                self._cleanup(connections=connections)
                return False, conn, False

        self._connected = True
        self._connections = connections

        conn = await self.get_connection()

        return True, None, conn.is_writable

    def close(self):
        self._cleanup(connections=self._connections)

    @property
    def connected(self):
        return self._connected

    async def wait_closed(self):
        return asyncio.gather(*[
            conn.wait_closed() for conn in self._connections
        ])

    async def get_connection(self) -> Connection:
        # Get the next protocol available for communication in the pool.
        connection = self._connections[self._index]
        self._index = (self._index + 1) % len(self._connections)

        await asyncio.wait_for(connection.wait_connected(),
                               timeout=self.options.pool_options.connect_timeout,
                               loop=self.loop)

        return connection

    def set_writable(self, is_writable: bool):
        for connection in self._connections:
            connection.is_writable = is_writable

    def _cleanup(self, connections: list):
        for conn in connections:
            if isinstance(conn, Connection):
                with suppress(Exception):
                    conn.close()
