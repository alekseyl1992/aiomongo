import asyncio
import logging
from asyncio import CancelledError
from typing import Optional, Union, List

from bson.codec_options import CodecOptions
from pymongo.client_options import ClientOptions
from pymongo.errors import ConfigurationError
from pymongo.read_concern import ReadConcern
from pymongo.read_preferences import _ALL_READ_PREFERENCES
from pymongo.uri_parser import parse_uri
from pymongo.write_concern import WriteConcern

from .connection import Connection
from .database import Database


logger = logging.getLogger('aiomongo.client')


class AioMongoClient:

    _index = 0

    def __init__(self, uri: str, loop: asyncio.AbstractEventLoop,
                 check_primary_period: int = 1):

        self._check_primary_period = check_primary_period

        uri_info = parse_uri(uri=uri)

        self.nodes = [{
            'host': node[0],
            'port': node[1],
        } for node in uri_info['nodelist']]

        self.options = ClientOptions(
            uri_info['username'], uri_info['password'], uri_info['database'], uri_info['options']
        )

        self.loop = loop

        self._pools = {}

        self._primary_pool = None

        self.__default_database_name = uri_info['database']

        self._check_primary_tasks = []

    async def connect(self) -> None:
        exception = None

        for node in self.nodes:
            host = node['host']
            port = node['port']

            try:
                pool = await asyncio.gather(
                    *[Connection.create(
                        self.loop, host, port, self.options
                    ) for _ in range(self.options.pool_options.max_pool_size)]
                )

                pool_name = f'{host}:{port}'
                self._pools[pool_name] = pool
            except Exception as e:
                logger.exception(f'Unable to connect to {node}', exc_info=True)
                exception = e

        if not self._pools:
            raise exception

        for host, pool in self._pools.items():
            connection: Connection = pool[self._index]
            if connection.is_writable:
                self.set_primary(host, pool)

        # primary may change in the future
        for host, pool in self._pools.items():
            task = asyncio.ensure_future(self.check_primary_coro(host, pool), loop=self.loop)
            self._check_primary_tasks.append(task)

    def __getitem__(self, item: str) -> Database:
        return Database(self, item)

    def __getattr__(self, item: str) -> Database:
        return self.__getitem__(item)

    async def check_primary(self, host: str, pool: List[Connection]):
        logger.debug(f'Check primary: {host}')

        connection: Connection = pool[self._index]

        try:
            await asyncio.wait_for(connection.wait_connected(),
                                   timeout=self.options.pool_options.connect_timeout,
                                   loop=self.loop)

            is_master = await asyncio.wait_for(connection.is_master(),
                                               timeout=self.options.pool_options.connect_timeout,
                                               loop=self.loop)

            if is_master.is_writable:
                if pool != self._primary_pool:
                    self.set_primary(host, pool)
                    return

        except CancelledError:
            raise
        except Exception:
            logger.exception('Error in check_primary', exc_info=True)

    async def check_primary_coro(self, host: str, pool: List[Connection]):
        while True:
            try:
                await asyncio.sleep(self._check_primary_period)
                await self.check_primary(host, pool)
            except CancelledError:
                logger.debug(f'Check primary coro cancelled for: {host}')
                return
            except Exception:
                logger.exception('Error in check_primary_coro', exc_info=True)
                continue

    def set_primary(self, primary_host: str, primary_pool: List[Connection]):
        logger.info(f'MongoDB primary node: {primary_host}')

        self._primary_pool = primary_pool

        for host, pool in self._pools.items():
            for connection in pool:
                connection.is_writable = host == primary_host

    async def get_connection(self) -> Connection:
        """ Gets connection from primary pool and waits for it to be ready
            to work.
        """

        # Get the next protocol available for communication in the pool.
        connection = self._primary_pool[self._index]
        self._index = (self._index + 1) % len(self._primary_pool)

        await asyncio.wait_for(connection.wait_connected(),
                               timeout=self.options.pool_options.connect_timeout,
                               loop=self.loop)

        return connection

    def get_database(self, name: str, codec_options: Optional[CodecOptions] = None,
                     read_preference: Optional[Union[_ALL_READ_PREFERENCES]] = None,
                     write_concern: Optional[WriteConcern] = None,
                     read_concern: Optional[ReadConcern] = None) -> Database:
        """Get a :class:`~aiomongo.database.Database` with the given name and
        options.

        Useful for creating a :class:`~aiomongo.database.Database` with
        different codec options, read preference, and/or write concern from
        this :class:`MongoClient`.

          >>> client.read_preference
          Primary()
          >>> db1 = client.test
          >>> db1.read_preference
          Primary()
          >>> from pymongo import ReadPreference
          >>> db2 = client.get_database(
          ...     'test', read_preference=ReadPreference.SECONDARY)
          >>> db2.read_preference
          Secondary(tag_sets=None)

        :Parameters:
          - `name`: The name of the database - a string.
          - `codec_options` (optional): An instance of
            :class:`~bson.codec_options.CodecOptions`. If ``None`` (the
            default) the :attr:`codec_options` of this :class:`MongoClient` is
            used.
          - `read_preference` (optional): The read preference to use. If
            ``None`` (the default) the :attr:`read_preference` of this
            :class:`MongoClient` is used. See :mod:`~pymongo.read_preferences`
            for options.
          - `write_concern` (optional): An instance of
            :class:`~pymongo.write_concern.WriteConcern`. If ``None`` (the
            default) the :attr:`write_concern` of this :class:`MongoClient` is
            used.
          - `read_concern` (optional): An instance of
            :class:`~pymongo.read_concern.ReadConcern`. If ``None`` (the
            default) the :attr:`read_concern` of this :class:`MongoClient` is
            used.
        """
        return Database(
            self, name, read_preference, read_concern,
            codec_options, write_concern)

    async def drop_database(self, name_or_database: Union[str, Database]) -> None:
        """Drop a database.

        Raises :class:`TypeError` if `name_or_database` is not an instance of
        :class:`basestring` (:class:`str` in python 3) or
        :class:`~aiomongo.database.Database`.

        :Parameters:
          - `name_or_database`: the name of a database to drop, or a
            :class:`~aiomongo.database.Database` instance representing the
            database to drop
        """
        name = name_or_database
        if isinstance(name, Database):
            name = name.name

        if not isinstance(name, str):
            raise TypeError('name_or_database must be an instance of str or a Database')

        await self[name].command('dropDatabase')

    def get_default_database(self) -> Database:
        """Get the database named in the MongoDB connection URI.

        >>> uri = 'mongodb://host/my_database'
        >>> client = AioMongoClient(uri)
        >>> db = client.get_default_database()
        >>> assert db.name == 'my_database'

        Useful in scripts where you want to choose which database to use
        based only on the URI in a configuration file.
        """
        if self.__default_database_name is None:
            raise ConfigurationError('No default database defined')

        return self[self.__default_database_name]

    async def server_info(self) -> dict:
        """Get information about the MongoDB server we're connected to."""
        return await self.admin.command('buildinfo')

    def close(self) -> None:
        for task in self._check_primary_tasks:
            task.cancel()

        self._check_primary_tasks = []

        for host, pool in self._pools.items():
            for conn in pool:
                conn.close()

    async def wait_closed(self) -> None:
        for host, pool in self._pools.items():
            await asyncio.wait([conn.wait_closed() for conn in pool], loop=self.loop)
