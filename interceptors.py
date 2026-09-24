import collections
import time
from datetime import datetime

import grpc


class LoggingInterceptor(grpc.ServerInterceptor):
    def intercept_service(self, continuation, handler_call_details):
        handler = continuation(handler_call_details)
        if handler is None:
            return None
        method = handler_call_details.method
        user = dict(handler_call_details.invocation_metadata).get("x-user", "-")

        def wrap(factory, original):
            def wrapped(*args):
                context = args[-1]
                started = time.perf_counter()
                try:
                    result = original(*args)
                    if factory is not None:
                        yield from result
                    else:
                        return result
                finally:
                    code = context.code() or grpc.StatusCode.OK
                    duration = int((time.perf_counter() - started) * 1000)
                    print(f"[{datetime.now():%H:%M:%S}] {method} duration={duration}ms "
                          f"code={code.name} user={user}", flush=True)
            return wrapped

        if handler.unary_unary:
            def unary_unary(request, context):
                started = time.perf_counter()
                try:
                    return handler.unary_unary(request, context)
                finally:
                    self._log(method, user, context, started)
            return grpc.unary_unary_rpc_method_handler(
                unary_unary, handler.request_deserializer, handler.response_serializer)
        if handler.unary_stream:
            def unary_stream(request, context):
                started = time.perf_counter()
                try:
                    yield from handler.unary_stream(request, context)
                finally:
                    self._log(method, user, context, started)
            return grpc.unary_stream_rpc_method_handler(
                unary_stream, handler.request_deserializer, handler.response_serializer)
        if handler.stream_unary:
            def stream_unary(request_iterator, context):
                started = time.perf_counter()
                try:
                    return handler.stream_unary(request_iterator, context)
                finally:
                    self._log(method, user, context, started)
            return grpc.stream_unary_rpc_method_handler(
                stream_unary, handler.request_deserializer, handler.response_serializer)
        if handler.stream_stream:
            def stream_stream(request_iterator, context):
                started = time.perf_counter()
                try:
                    yield from handler.stream_stream(request_iterator, context)
                finally:
                    self._log(method, user, context, started)
            return grpc.stream_stream_rpc_method_handler(
                stream_stream, handler.request_deserializer, handler.response_serializer)
        return handler

    @staticmethod
    def _log(method, user, context, started):
        code = context.code() or grpc.StatusCode.OK
        print(f"[{datetime.now():%H:%M:%S}] {method} "
              f"duration={int((time.perf_counter() - started) * 1000)}ms "
              f"code={code.name} user={user}", flush=True)


class _ClientCallDetails(collections.namedtuple(
        "_ClientCallDetails",
        ("method", "timeout", "metadata", "credentials", "wait_for_ready", "compression")),
        grpc.ClientCallDetails):
    pass


class HeaderInterceptor(grpc.UnaryUnaryClientInterceptor,
                        grpc.UnaryStreamClientInterceptor,
                        grpc.StreamUnaryClientInterceptor,
                        grpc.StreamStreamClientInterceptor):
    def __init__(self, user: str):
        self._user = user

    def _details(self, details):
        metadata = list(details.metadata or ())
        metadata.append(("x-user", self._user))
        return _ClientCallDetails(details.method, details.timeout, metadata,
                                  details.credentials, details.wait_for_ready,
                                  details.compression)

    def intercept_unary_unary(self, continuation, details, request):
        return continuation(self._details(details), request)

    def intercept_unary_stream(self, continuation, details, request):
        return continuation(self._details(details), request)

    def intercept_stream_unary(self, continuation, details, request_iterator):
        return continuation(self._details(details), request_iterator)

    def intercept_stream_stream(self, continuation, details, request_iterator):
        return continuation(self._details(details), request_iterator)