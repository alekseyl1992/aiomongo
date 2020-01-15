import asyncio
import logging
import socket
import struct
from contextlib import suppress
from typing import List, MutableMapping, Optional, Union

from bson import DEFAULT_CODEC_OPTIONS
from bson.codec_options import CodecOptions
from bson.son import SON
from pymongo import common, helpers, message
from pymongo.client_options import ClientOptions
from pymongo.collation import Collation
from pymongo.ismaster import IsMaster
from pymongo.errors import ConfigurationError, ProtocolError, ConnectionFailure, AutoReconnect
from pymongo.read_concern import DEFAULT_READ_CONCERN, ReadConcern
from pymongo.read_preferences import ReadPreference, _ALL_READ_PREFERENCES
from pymongo.server_type import SERVER_TYPE
from pymongo.write_concern import WriteConcern

from .auth import get_authenticator
from .utils import IncrementalSleeper

logger = logging.getLogger('aiomongo.connection')


INT_MAX = 2147483647


class Connection:

    def __init__(self, loop: asyncio.AbstractEventLoop, host: str, port: int,
                 options: ClientOptions):
        self.host = host
        self.port = port
        self.loop = loop
        self.reader = None
        self.writer = None
        self.read_loop_task = None
        self.reconnect_task = None
        self.is_mongos = False
        self.is_writable = False
        self.max_bson_size = common.MAX_BSON_SIZE
        self.max_message_size = common.MAX_MESSAGE_SIZE
        self.max_wire_version = 0
        self.max_write_batch_size = common.MAX_WRITE_BATCH_SIZE
        self.options = options
        self.slave_ok = False

        self.__connected = asyncio.Event(loop=loop)
        self.__disconnected = asyncio.Event(loop=loop)
        self.__request_id = 0
        self.__request_futures = {}
        self.__sleeper = IncrementalSleeper(loop)

    @classmethod
    async def create(cls, loop: asyncio.AbstractEventLoop, host: str, port: int,
                     options: ClientOptions) -> 'Connection':
        conn = cls(loop, host, port, options)

        try:
            await conn.connect()
        except Exception:
            with suppress(Exception):
                conn.close()
            raise

        return conn

    async def connect(self) -> None:
        if self.host.startswith('/'):
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_unix_connection(
                path=self.host, loop=self.loop
            ), timeout=self.options.pool_options.connect_timeout, loop=self.loop)
        else:
            self.reader, self.writer = await asyncio.wait_for(asyncio.open_connection(
                host=self.host, port=self.port, loop=self.loop
            ), timeout=self.options.pool_options.connect_timeout, loop=self.loop)

        sock = self.writer.transport.get_extra_info('socket')
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, int(self.options.pool_options.socket_keepalive))

        if self.host.startswith('/'):
            endpoint = self.host
        else:
            endpoint = '{}:{}'.format(self.host, self.port)
        logger.debug('Established connection to {}'.format(endpoint))
        self.read_loop_task = asyncio.ensure_future(self.read_loop(), loop=self.loop)
        self.read_loop_task.add_done_callback(self._on_read_loop_task_done)

        is_master = await self.is_master(
            ignore_connected=True,  # allow the request whilst __connected is not set yet
        )

        self.is_mongos = is_master.server_type == SERVER_TYPE.Mongos
        self.max_wire_version = is_master.max_wire_version
        if is_master.max_bson_size:
            self.max_bson_size = is_master.max_bson_size
        if is_master.max_message_size:
            self.max_message_size = is_master.max_message_size
        if is_master.max_write_batch_size:
            self.max_write_batch_size = is_master.max_write_batch_size
        self.is_writable = is_master.is_writable

        self.slave_ok = not self.is_mongos and self.options.read_preference != ReadPreference.PRIMARY

        if self.options.credentials:
            await self._authenticate()

        # Notify waiters that connection has been established
        self.__connected.set()

    def _on_read_loop_task_done(self, t: asyncio.Task):
        try:
            e = t.exception()
            if e is not None:
                raise e
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception('read_loop() exited with error', exc_info=True)

    def _on_reconnect_task_done(self, t: asyncio.Task):
        try:
            e = t.exception()
            if e is not None:
                raise e
        except asyncio.CancelledError:
            pass
        except Exception:
            logger.exception('reconnect() exited with error', exc_info=True)
        finally:
            # allow new reconnect tasks to start
            self.reconnect_task = None

    async def is_master(self, ignore_connected: bool = False) -> IsMaster:
        is_master = IsMaster(await self.command(
            'admin', SON([('ismaster', 1)]), ReadPreference.PRIMARY, DEFAULT_CODEC_OPTIONS,
            ignore_connected=ignore_connected,
        ))

        return is_master

    async def reconnect(self) -> None:
        logger.warning('Reconnecting...')
        while True:
            try:
                await self.connect()
            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error('Failed to reconnect: {}'.format(str(e)))
                await self.__sleeper.sleep()
            else:
                logger.warning('Reconnect succeeded')
                self.__sleeper.reset()
                return

    def gen_request_id(self) -> int:
        """ Generates request unique request id for API that doesn't use rand()
            function internally.
        """
        while self.__request_id in self.__request_futures:
            self.__request_id += 1
            if self.__request_id >= INT_MAX:
                self.__request_id = 0

        return self.__request_id

    async def perform_operation(self, operation) -> bytes:
        self._check_connected()

        request_id = None

        # Because pymongo uses rand() function internally to generate request_id
        # there is a possibility that we have more than one in-flight request with
        # the same id. To avoid this we rerun get_message function that regenerates
        # query with new request id. In most cases this loop will run only once.
        while request_id is None or request_id in self.__request_futures:
            msg = operation.get_message(self.slave_ok, self.is_mongos, True)
            request_id, data, _ = self._split_message(msg)

        response_future = asyncio.Future(loop=self.loop)
        self.__request_futures[request_id] = response_future

        self.send_message(data)

        try:
            return await response_future
        except asyncio.CancelledError:
            del self.__request_futures[request_id]
            raise

    async def write_command(self, request_id: int, msg: bytes) -> dict:
        self._check_connected()

        response_future = asyncio.Future(loop=self.loop)
        self.__request_futures[request_id] = response_future

        self.send_message(msg)

        try:
            response_data = await response_future
        except asyncio.CancelledError:
            del self.__request_futures[request_id]
            raise

        response = helpers._unpack_response(response_data)
        assert response['number_returned'] == 1

        result = response['data'][0]

        # Raises NotMasterError or OperationFailure.
        helpers._check_command_response(result)
        return result

    def send_message(self, msg: bytes) -> None:
        self.writer.write(msg)

    async def command(self, dbname: str, spec: SON,
                      read_preference: Optional[Union[_ALL_READ_PREFERENCES]] = None,
                      codec_options: Optional[CodecOptions] = None, check: bool = True,
                      allowable_errors: Optional[List[str]] = None, check_keys: bool = False,
                      read_concern: ReadConcern = DEFAULT_READ_CONCERN,
                      write_concern: Optional[WriteConcern] = None,
                      parse_write_concern_error: bool = False,
                      collation: Optional[Union[Collation, dict]] = None,
                      ignore_connected: bool = False) -> MutableMapping:

        if self.max_wire_version < 4 and not read_concern.ok_for_legacy:
            raise ConfigurationError(
                'Read concern of level {} is not valid with max wire version of {}'.format(
                    read_concern.level, self.max_wire_version
                )
            )
        if not (write_concern is None or write_concern.acknowledged or
                collation is None):
            raise ConfigurationError(
                'Collation is unsupported for unacknowledged writes.')
        if self.max_wire_version >= 5 and write_concern:
            spec['writeConcern'] = write_concern.document
        elif self.max_wire_version < 5 and collation is not None:
            raise ConfigurationError(
                'Must be connected to MongoDB 3.4+ to use a collation.')

        read_preference = read_preference or self.options.read_preference
        codec_options = codec_options or self.options.codec_options

        name = next(iter(spec))
        ns = dbname + '.$cmd'

        if read_preference != ReadPreference.PRIMARY:
            flags = 4
        else:
            flags = 0

        if self.is_mongos:
            spec = message._maybe_add_read_preference(spec, read_preference)
        if read_concern.level:
            spec['readConcern'] = read_concern.document
        if collation:
            spec['collation'] = collation

        # See explanation in perform_operation method
        request_id = None
        while request_id is None or request_id in self.__request_futures:
            request_id, msg, size = message.query(flags, ns, 0, -1, spec,
                                                  None, codec_options, check_keys)

        if size > self.max_bson_size + message._COMMAND_OVERHEAD:
            message._raise_document_too_large(
                name, size, self.max_bson_size + message._COMMAND_OVERHEAD)

        if not ignore_connected:
            self._check_connected()

        response_future = asyncio.Future()
        self.__request_futures[request_id] = response_future

        self.send_message(msg)

        try:
            response = await response_future
        except asyncio.CancelledError:
            del self.__request_futures[request_id]
            raise

        unpacked = helpers._unpack_response(response, codec_options=codec_options)
        response_doc = unpacked['data'][0]
        if check:
            helpers._check_command_response(response_doc, None, allowable_errors,
                                            parse_write_concern_error=parse_write_concern_error)

        return response_doc

    async def _authenticate(self) -> None:
        authenticator = get_authenticator(
            self.options.credentials.mechanism
        )
        await authenticator(self.options.credentials, self)

    def _check_connected(self):
        if not self.__connected.is_set():
            raise AutoReconnect('Not connected')

    @staticmethod
    def _split_message(msg: tuple) -> tuple:
        """Return request_id, data, max_doc_size.

        :Parameters:
          - `message`: (request_id, data, max_doc_size) or (request_id, data)
        """
        if len(msg) == 3:
            return msg
        else:
            # get_more and kill_cursors messages don't include BSON documents.
            request_id, data = msg
            return request_id, data, 0

    async def read_loop(self) -> None:
        while True:
            try:
                await self._read_loop_step()
            except asyncio.CancelledError:
                self._shut_down()
                return
            except Exception as e:
                self.__connected.clear()
                connection_error = ConnectionFailure('Connection was lost due to: {}'.format(str(e)))
                self.close(error=connection_error)
                for ft in self.__request_futures.values():
                    if not ft.done():
                        ft.set_exception(connection_error)
                self.__request_futures = {}

                if self.reconnect_task is None:
                    self.reconnect_task = asyncio.ensure_future(self.reconnect(), loop=self.loop)
                    self.reconnect_task.add_done_callback(self._on_reconnect_task_done)
                else:
                    logger.warning('Reconnect already in progress')

                return

    def _shut_down(self) -> None:
        connection_error = ConnectionFailure('Shutting down.')
        for ft in self.__request_futures.values():
            if not ft.done():
                ft.set_exception(connection_error)

        self.__disconnected.set()

    async def _read_loop_step(self) -> None:
        header = await self.reader.readexactly(16)
        length, = struct.unpack('<i', header[:4])
        if length < 16:
            raise ProtocolError('Message length ({}) not longer than standard '
                                'message header size (16)'.format(length))

        response_id, = struct.unpack('<i', header[8:12])

        message_data = await self.reader.readexactly(length - 16)

        if response_id not in self.__request_futures:
            logger.warning(
                f'Got response id {response_id} but request with such id was not sent'
            )
        else:
            ft = self.__request_futures.pop(response_id)
            if not ft.done():
                ft.set_result(message_data)

    async def wait_connected(self) -> None:
        """Returns when connection is ready to be used"""
        await self.__connected.wait()

    def close(self, error: Optional[Exception] = None) -> None:
        self.is_writable = False

        if error is not None:
            # we are reconnecting
            logger.error(str(error))
        else:
            # we are closing

            if self.read_loop_task is not None:
                self.read_loop_task.cancel()
                self.read_loop_task = None

            if self.reconnect_task is not None:
                self.reconnect_task.cancel()
                self.reconnect_task = None

        if self.writer is not None:
            self.writer.close()

    async def wait_closed(self) -> None:
        await self.__disconnected.wait()
