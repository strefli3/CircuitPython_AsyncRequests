# SPDX-FileCopyrightText: 2020 Dan Halbert for Adafruit Industries
# SPDX-FileContributor: Updated and repackaged/tested by Alex Herrmann, 2022
#
# SPDX-License-Identifier: MIT

"""
`adafruit_requests.async_session`
================================================================================

"""

import errno

import json as json_module

import asyncio
from adafruit_requests import  OutOfRetries

try:
    from types import TracebackType
    from typing import IO, Any, Dict, Optional, Type, List, AsyncGenerator
    from circuitpython_typing.socket import (
        SocketType,
        SocketpoolModuleType,
        SSLContextType,
    )
except ImportError:
    pass

class _AsyncRawResponse:
    def __init__(self, response: "AsyncResponse") -> None:
        self._response = response

    def read(self, size: int = -1) -> bytes:
        """Read as much as available or up to size and return it in a byte string.

        Do NOT use this unless you really need to. Reusing memory with `readinto` is much better.
        """
        if size == -1:
            return self._response.content
        return self._response.socket.recv(size)

    def readinto(self, buf: bytearray) -> int:
        """Read as much as available into buf or until it is full. Returns the number of bytes read
        into buf."""
        return self._response._readinto(buf)  # pylint: disable=protected-access


class AsyncResponse:
    """The response from a request, contains all the headers/content"""

    # pylint: disable=too-many-instance-attributes

    encoding = None
    
    def __init__(self, sock: SocketType, session: Optional["AsyncSession"] = None) -> None:
        """Private constructor - use Response.create() instead"""
        self.socket = sock
        self.encoding = "utf-8"
        self._cached = None
        self._headers = {}
        self._received_length = 0
        self._receive_buffer = bytearray(32)
        self._remaining = None
        self._chunked = False
        self._raw = None
        self._session = session
        # Don't do parsing in __init__ anymore
    
    @classmethod
    async def create(cls, sock: SocketType, session: Optional["AsyncSession"] = None) -> "AsyncResponse":
        """Async factory method to create a Response instance"""
        response = cls(sock, session)
        
        try:
            # Do the async parsing work that was originally in __init__
            http = await response._async_readto(b" ")
            if not http:
                if session:
                    session._close_socket(response.socket)
                else:
                    response.socket.close()
                raise RuntimeError("Unable to read HTTP response.")
            
            response.status_code = int(bytes(await response._async_readto(b" ")))
            response.reason = await response._async_readto(b"\r\n")
            await response._async_parse_headers()
            
            return response
            
        except Exception:
            if session:
                session._close_socket(sock)
            else:
                sock.close()
            raise
    
    async def _async_readto(self, stop: bytes) -> bytearray:
        """Async version of  _readto method
        
        This seems to be the function that takes the most "time".
        
        """
        buf = self._receive_buffer
        end = self._received_length
        while True:
            i = buf.find(stop, 0, end)
            if i >= 0:
                # Stop was found. Return everything up to but not including stop.
                result = buf[:i]
                new_start = i + len(stop)
                # Remove everything up to and including stop from the buffer.
                new_end = end - new_start
                buf[:new_end] = buf[new_start:end]
                self._received_length = new_end
                return result

            # Not found so load more bytes.
            # If our buffer is full, then make it bigger to load more.
            if end == len(buf):
                new_buf = bytearray(len(buf) + 32)
                new_buf[: len(buf)] = buf
                buf = new_buf
                self._receive_buffer = buf

            read = self._recv_into(memoryview(buf)[end:])
            if read == 0:
                self._received_length = 0
                return buf[:end]
            end += read
            await asyncio.sleep(0.0001) # Testing an idea...
    
    async def _async_parse_headers(self) -> None:
        """
        Parses the header portion of an HTTP request/response from the socket.
        Expects first line of HTTP request/response to have been read already.
        """
        while True:
            header = await self._async_readto(b"\r\n")
            if not header:
                break
            title, content = bytes(header).split(b": ", 1)
            if title and content:
                # enforce that all headers are lowercase
                title = str(title, "utf-8").lower()
                content = str(content, "utf-8")
                if title == "content-length":
                    self._remaining = int(content)
                if title == "transfer-encoding":
                    self._chunked = content.strip().lower() == "chunked"
                if title == "set-cookie" and title in self._headers:
                    self._headers[title] += ", " + content
                else:
                    self._headers[title] = content

    def __enter__(self) -> "AsyncResponse":
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[type]],
        exc_value: Optional[BaseException],
        traceback: Optional[TracebackType],
    ) -> None:
        self.close()

    def _recv_into(self, buf: bytearray, size: int = 0) -> int:
        return self.socket.recv_into(buf, size)

    def _read_from_buffer(
        self, buf: Optional[bytearray] = None, nbytes: Optional[int] = None
    ) -> int:
        if self._received_length == 0:
            return 0
        read = self._received_length
        if nbytes < read:
            read = nbytes
        membuf = memoryview(self._receive_buffer)
        if buf:
            buf[:read] = membuf[:read]
        if read < self._received_length:
            new_end = self._received_length - read
            self._receive_buffer[:new_end] = membuf[read : self._received_length]
            self._received_length = new_end
        else:
            self._received_length = 0
        return read

    def _readinto(self, buf: bytearray) -> int:
        if not self.socket:
            raise RuntimeError(
                "Newer Response closed this one. Use Responses immediately."
            )

        if not self._remaining:
            # Consume the chunk header if need be.
            if self._chunked:
                # Consume trailing \r\n for chunks 2+
                if self._remaining == 0:
                    self._throw_away(2)
                chunk_header = bytes(self._readto(b"\r\n")).split(b";", 1)[0]
                http_chunk_size = int(bytes(chunk_header), 16)
                if http_chunk_size == 0:
                    self._chunked = False
                    self._parse_headers()
                    return 0
                self._remaining = http_chunk_size
            elif self._remaining is None:
                # the Content-Length is not provided in the HTTP header
                # so try parsing as long as their is data in the socket
                pass
            else:
                return 0

        nbytes = len(buf)
        if self._remaining and nbytes > self._remaining:
            # if Content-Length was provided and remaining bytes larges than buffer
            nbytes = self._remaining  # adjust read amount

        read = self._read_from_buffer(buf, nbytes)
        if read == 0:
            read = self._recv_into(buf, nbytes)
        if self._remaining:
            # if Content-Length was provided, adjust the remaining amount to still read
            self._remaining -= read

        return read

    def _throw_away(self, nbytes: int) -> None:
        nbytes -= self._read_from_buffer(nbytes=nbytes)

        buf = self._receive_buffer
        len_buf = len(buf)
        for _ in range(nbytes // len_buf):
            to_read = len_buf
            while to_read > 0:
                to_read -= self._recv_into(buf, to_read)
        to_read = nbytes % len_buf
        while to_read > 0:
            to_read -= self._recv_into(buf, to_read)

    def close(self) -> None:
        """Drain the remaining ESP socket buffers. We assume we already got what we wanted."""
        if not self.socket:
            return
        # Make sure we've read all of our response.
        if self._cached is None:
            if self._remaining and self._remaining > 0:
                self._throw_away(self._remaining)
            elif self._chunked:
                while True:
                    chunk_header = bytes(self._readto(b"\r\n")).split(b";", 1)[0]
                    chunk_size = int(bytes(chunk_header), 16)
                    if chunk_size == 0:
                        break
                    self._throw_away(chunk_size + 2)
                self._parse_headers()
        if self._session:
            self._session._free_socket(self.socket)  # pylint: disable=protected-access
        else:
            self.socket.close()
        self.socket = None

    def _validate_not_gzip(self) -> None:
        """gzip encoding is not supported. Raise an exception if found."""
        if (
            "content-encoding" in self.headers
            and self.headers["content-encoding"] == "gzip"
        ):
            raise ValueError(
                "Content-encoding is gzip, data cannot be accessed as json or text. "
                "Use content property to access raw bytes."
            )

    @property
    def headers(self) -> Dict[str, str]:
        """
        The response headers. Does not include headers from the trailer until
        the content has been read.
        """
        return self._headers

    @property
    def content(self) -> bytes:
        """The HTTP content direct from the socket, as bytes"""
        if self._cached is not None:
            if isinstance(self._cached, bytes):
                return self._cached
            raise RuntimeError("Cannot access content after getting text or json")

        self._cached = b"".join(self.iter_content(chunk_size=32))
        return self._cached

    @property
    def text(self) -> str:
        """The HTTP content, encoded into a string according to the HTTP
        header encoding"""
        if self._cached is not None:
            if isinstance(self._cached, str):
                return self._cached
            raise RuntimeError("Cannot access text after getting content or json")

        self._validate_not_gzip()

        self._cached = str(self.content, self.encoding)
        return self._cached

    def json(self) -> Any:
        """The HTTP content, parsed into a json dictionary"""
        # The cached JSON will be a list or dictionary.
        if self._cached:
            if isinstance(self._cached, (list, dict)):
                return self._cached
            raise RuntimeError("Cannot access json after getting text or content")
        if not self._raw:
            self._raw = _AsyncRawResponse(self)

        self._validate_not_gzip()

        obj = json_module.load(self._raw)
        if not self._cached:
            self._cached = obj
        self.close()
        return obj

    def iter_content(self, chunk_size: int = 1, decode_unicode: bool = False) -> bytes:
        """An iterator that will stream data by only reading 'chunk_size'
        bytes and yielding them, when we can't buffer the whole datastream"""
        if decode_unicode:
            raise NotImplementedError("Unicode not supported")

        b = bytearray(chunk_size)
        while True:
            size = self._readinto(b)
            if size == 0:
                break
            if size < chunk_size:
                chunk = bytes(memoryview(b)[:size])
            else:
                chunk = bytes(b)
            yield chunk
        self.close()



class AsyncSession:
    """HTTP session that shares sockets and ssl context."""

    """HTTP session that shares sockets and ssl context."""

    def __init__(
        self,
        socket_pool: SocketpoolModuleType,
        ssl_context: Optional[SSLContextType] = None,
    ) -> None:
        self._socket_pool = socket_pool
        self._ssl_context = ssl_context
        # Hang onto open sockets so that we can reuse them.
        self._open_sockets = {}
        self._socket_free = {}
        self._last_response = None

    def _free_socket(self, socket: SocketType) -> None:
        if socket not in self._open_sockets.values():
            raise RuntimeError("Socket not from session")
        self._socket_free[socket] = True

    def _close_socket(self, sock: SocketType) -> None:
        sock.close()
        del self._socket_free[sock]
        key = None
        for k in self._open_sockets:  # pylint: disable=consider-using-dict-items
            if self._open_sockets[k] == sock:
                key = k
                break
        if key:
            del self._open_sockets[key]

    def _free_sockets(self) -> None:
        free_sockets = []
        for sock, val in self._socket_free.items():
            if val:
                free_sockets.append(sock)
        for sock in free_sockets:
            self._close_socket(sock)

    def _get_socket(
        self, host: str, port: int, proto: str, *, timeout: float = 1
    ) -> CircuitPythonSocketType:
        # pylint: disable=too-many-branches
        key = (host, port, proto)
        if key in self._open_sockets:
            sock = self._open_sockets[key]
            if self._socket_free[sock]:
                self._socket_free[sock] = False
                return sock
        if proto == "https:" and not self._ssl_context:
            raise RuntimeError(
                "ssl_context must be set before using adafruit_requests for https"
            )
        addr_info = self._socket_pool.getaddrinfo(
            host, port, 0, self._socket_pool.SOCK_STREAM
        )[0]
        retry_count = 0
        sock = None
        last_exc = None
        while retry_count < 5 and sock is None:
            if retry_count > 0:
                if any(self._socket_free.items()):
                    self._free_sockets()
                else:
                    raise RuntimeError("Sending request failed") from last_exc
            retry_count += 1

            try:
                sock = self._socket_pool.socket(addr_info[0], addr_info[1])
            except OSError as exc:
                last_exc = exc
                continue
            except RuntimeError as exc:
                last_exc = exc
                continue

            connect_host = addr_info[-1][0]
            if proto == "https:":
                sock = self._ssl_context.wrap_socket(sock, server_hostname=host)
                connect_host = host
            sock.settimeout(timeout)  # socket read timeout

            try:
                sock.connect((connect_host, port))
            except MemoryError as exc:
                last_exc = exc
                sock.close()
                sock = None
            except OSError as exc:
                last_exc = exc
                sock.close()
                sock = None

        if sock is None:
            raise RuntimeError("Repeated socket failures") from last_exc

        self._open_sockets[key] = sock
        self._socket_free[sock] = False
        return sock

    @staticmethod
    async def _asend(socket: SocketType, data: bytes):
        total_sent = 0
        while total_sent < len(data):
            # ESP32SPI sockets raise a RuntimeError when unable to send.
            try:
                sent = socket.send(data[total_sent:])
            except OSError as exc:
                if exc.errno == errno.EAGAIN:
                    # Can't send right now (e.g., no buffer space), try again.
                    await asyncio.sleep(0)
                # Some worse error.
                raise
            except RuntimeError as exc:
                raise OSError(errno.EIO) from exc
            if sent is None:
                sent = len(data)
            if sent == 0:
                # Not EAGAIN; that was already handled.
                raise OSError(errno.EIO)
            total_sent += sent


    async def _asend_request(
            self,
            socket: SocketType,
            host: str,
            method: str,
            path: str,
            headers: List[Dict[str, str]],
            data: Any,
            json: Any,
    ):
        # pylint: disable=too-many-arguments
        await self._asend(socket, bytes(method, "utf-8"))
        await self._asend(socket, b" /")
        await self._asend(socket, bytes(path, "utf-8"))
        await self._asend(socket, b" HTTP/1.1\r\n")
        if "Host" not in headers:
            await self._asend(socket, b"Host: ")
            await self._asend(socket, bytes(host, "utf-8"))
            await self._asend(socket, b"\r\n")
        if "User-Agent" not in headers:
            await self._asend(socket, b"User-Agent: Adafruit CircuitPython\r\n")
        # Iterate over keys to avoid tuple alloc
        for k in headers:
            await self._asend(socket, k.encode())
            await self._asend(socket, b": ")
            await self._asend(socket, headers[k].encode())
            await self._asend(socket, b"\r\n")
        if json is not None:
            assert data is None
            data = json_module.dumps(json)
            await self._asend(socket, b"Content-Type: application/json\r\n")
        if data:
            if isinstance(data, dict):
                await self._asend(
                    socket, b"Content-Type: application/x-www-form-urlencoded\r\n"
                )
                _post_data = ""
                for k in data:
                    _post_data = f"{_post_data}&{k}={data[k]}"
                    # _post_data = "{}&{}={}".format(_post_data, k, data[k])
                data = _post_data[1:]
            await self._asend(socket, b"Content-Length: %d\r\n" % len(data))
        await self._asend(socket, b"\r\n")
        if data:
            if isinstance(data, bytearray):
                await self._asend(socket, bytes(data))
            else:
                await self._asend(socket, bytes(data, "utf-8"))

    # pylint: disable=too-many-branches, too-many-statements, unused-argument, too-many-arguments, too-many-locals
    async def arequest(
            self,
            method: str,
            url: str,
            data: Optional[Any] = None,
            json: Optional[Any] = None,
            headers: Optional[List[Dict[str, str]]] = None,
            stream: bool = False,
            timeout: float = 60,
    ) -> AsyncResponse:
        """Perform an HTTP request to the given url which we will parse to determine
        whether to use SSL ('https://') or not. We can also send some provided 'data'
        or a json dictionary which we will stringify. 'headers' is optional HTTP headers
        sent along. 'stream' will determine if we buffer everything, or whether to only
        read only when requested
        """
        if not headers:
            headers = {}

        try:
            proto, dummy, host, path = url.split("/", 3)
            # replace spaces in path
            path = path.replace(" ", "%20")
        except ValueError:
            proto, dummy, host = url.split("/", 2)
            path = ""
        if proto == "http:":
            port = 80
        elif proto == "https:":
            port = 443
        else:
            raise ValueError("Unsupported protocol: " + proto)

        if ":" in host:
            host, port = host.split(":", 1)
            port = int(port)

        if self._last_response:
            self._last_response.close()
            self._last_response = None

        # We may fail to send the request if the socket we got is closed already. So, try a second
        # time in that case.
        retry_count = 0
        while retry_count < 2:
            retry_count += 1
            socket = self._get_socket(host, port, proto, timeout=timeout)
            ok = True
            try:
                await self._asend_request(socket, host, method, path, headers, data, json)
            except OSError:
                ok = False
            if ok:
                # Read the H of "HTTP/1.1" to make sure the socket is alive. send can appear to work
                # even when the socket is closed.
                if hasattr(socket, "recv"):
                    result = socket.recv(1)
                else:
                    result = bytearray(1)
                    try:
                        socket.recv_into(result)
                    except OSError:
                        pass
                if result == b"H":
                    # Things seem to be ok so break with socket set.
                    break
            self._close_socket(socket)
            socket = None

        if not socket:
            raise OutOfRetries("Repeated socket failures")

        #resp = AsyncResponse(socket, self)# our response
        resp = await AsyncResponse.create(socket, self)
        if "location" in resp.headers and 300 <= resp.status_code <= 399:
            # a naive handler for redirects
            redirect = resp.headers["location"]

            if redirect.startswith("http"):
                # absolute URL
                url = redirect
            elif redirect[0] == "/":
                # relative URL, absolute path
                url = "/".join([proto, dummy, host, redirect[1:]])
            else:
                # relative URL, relative path
                path = path.rsplit("/", 1)[0]

                while redirect.startswith("../"):
                    path = path.rsplit("/", 1)[0]
                    redirect = redirect.split("../", 1)[1]

                url = "/".join([proto, dummy, host, path, redirect])

            self._last_response = resp
            resp = await self.arequest(method, url, data, json, headers, stream, timeout)

        self._last_response = resp
        return resp

    async def ahead(self, url: str, **kw) -> AsyncResponse:
        """Send HTTP HEAD request"""
        return await self.arequest("HEAD", url, **kw)

    async def aget(self, url: str, **kw) -> AsyncResponse:
        """Send HTTP GET request"""
        return await self.arequest("GET", url, **kw)

    async def apost(self, url: str, **kw) -> AsyncResponse:
        """Send HTTP POST request"""
        return await self.arequest("POST", url, **kw)

    async def aput(self, url: str, **kw) -> AsyncResponse:
        """Send HTTP PUT request"""
        return await self.arequest("PUT", url, **kw)

    async def apatch(self, url: str, **kw) -> AsyncResponse:
        """Send HTTP PATCH request"""
        return await self.arequest("PATCH", url, **kw)

    async def adelete(self, url: str, **kw) -> AsyncResponse:
        """Send HTTP DELETE request"""
        return await self.arequest("DELETE", url, **kw)
