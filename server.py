import argparse
import queue
import threading
import time
from concurrent import futures

import grpc
from google.protobuf.timestamp_pb2 import Timestamp

import taskflow_pb2
import taskflow_pb2_grpc
from interceptors import LoggingInterceptor

_STOP = object()


def _now() -> Timestamp:
    timestamp = Timestamp()
    timestamp.GetCurrentTime()
    return timestamp


def _status_name(value: int) -> str:
    return taskflow_pb2.TaskStatus.Name(value)


class TaskFlowService(taskflow_pb2_grpc.TaskFlowServicer):
    def __init__(self, slow: bool = False):
        self._tasks = {}
        self._next_task_id = 1
        self._lock = threading.RLock()
        self._subscribers = []
        self._subs_lock = threading.Lock()
        self._slow = slow

    def CreateTask(self, request, context):
        if not request.title:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "title is required")
        if not request.created_by:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "created_by is required")
        if self._slow:
            time.sleep(5)
        with self._lock:
            task_id = str(self._next_task_id)
            self._next_task_id += 1
            task = {"id": task_id, "title": request.title,
                "description": request.description, "status": taskflow_pb2.TODO,
                "assigned_to": request.assigned_to, "created_by": request.created_by,
                "created_at": _now(), "comments": []}
            self._tasks[task["id"]] = task
            result = self._to_pb(task)
        self._publish("CREATED", task["id"], task["created_by"],
                      f'{task["created_by"]} a créé « {task["title"]} »')
        return taskflow_pb2.CreateTaskResponse(task=result)

    def GetTask(self, request, context):
        with self._lock:
            return self._to_pb(self._get_or_abort(request.id, context))

    def UpdateTask(self, request, context):
        if not request.HasField("title") and not request.HasField("description"):
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "at least one field is required")
        if request.HasField("title") and not request.title:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "title cannot be empty")
        with self._lock:
            task = self._get_or_abort(request.id, context)
            if request.HasField("title"):
                task["title"] = request.title
            if request.HasField("description"):
                task["description"] = request.description
            result = self._to_pb(task)
        self._publish("UPDATED", task["id"], request.requested_by,
                      f'{request.requested_by} a modifié « {task["title"]} »')
        return result

    def ListTasks(self, request, context):
        with self._lock:
            tasks = []
            for task in self._tasks.values():
                if request.HasField("status_filter") and task["status"] != request.status_filter:
                    continue
                if request.HasField("assigned_filter") and task["assigned_to"] != request.assigned_filter:
                    continue
                tasks.append(self._to_pb(task))
        yield from tasks

    def UpdateStatus(self, request, context):
        with self._lock:
            task = self._get_or_abort(request.id, context)
            if task["status"] == request.new_status:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              f'task already in status {_status_name(request.new_status)}')
            if task["status"] == taskflow_pb2.DONE:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, "cannot reopen a DONE task")
            task["status"] = request.new_status
            result = self._to_pb(task)
            message = (f'{request.requested_by} a passé \'{task["title"]}\' '
                       f'à {_status_name(request.new_status)}')
        self._publish("STATUS_CHANGED", task["id"], request.requested_by, message)
        return result

    def AssignTask(self, request, context):
        if not request.new_assignee:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "new_assignee is required")
        with self._lock:
            task = self._get_or_abort(request.id, context)
            if task["assigned_to"] == request.new_assignee:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT,
                              f'task already assigned to {request.new_assignee}')
            task["assigned_to"] = request.new_assignee
            result = self._to_pb(task)
        self._publish("ASSIGNED", task["id"], request.requested_by,
                      f'{request.requested_by} a assigné « {task["title"]} » à {request.new_assignee}')
        return result

    def AddComment(self, request, context):
        if not request.text:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, "comment text is required")
        with self._lock:
            task = self._get_or_abort(request.id, context)
            task["comments"].append({"author": request.author, "text": request.text,
                                      "created_at": _now()})
            result = self._to_pb(task)
        self._publish("COMMENTED", task["id"], request.author,
                      f'{request.author} a commenté « {task["title"]} »')
        return result

    def DeleteTask(self, request, context):
        with self._lock:
            task = self._get_or_abort(request.id, context)
            if task["created_by"] != request.requested_by:
                context.abort(grpc.StatusCode.PERMISSION_DENIED,
                              f'only {task["created_by"]} can delete this task')
            del self._tasks[request.id]
        self._publish("DELETED", request.id, request.requested_by,
                      f'{request.requested_by} a supprimé « {task["title"]} »')
        return taskflow_pb2.Empty()

    def Subscribe(self, request, context):
        entry = (request.username, set(request.event_types), queue.Queue())
        with self._subs_lock:
            self._subscribers.append(entry)
        context.add_callback(lambda: entry[2].put(_STOP))
        return self._event_stream(entry)

    def _event_stream(self, entry):
        _, event_types, event_queue = entry
        try:
            while True:
                event = event_queue.get()
                if event is _STOP:
                    return
                if event_types and event.event_type not in event_types:
                    continue
                yield event
        finally:
            with self._subs_lock:
                if entry in self._subscribers:
                    self._subscribers.remove(entry)

    def SearchKeywords(self, request_iterator, context):
        results = []
        with self._lock:
            for entry in request_iterator:
                if not entry.keyword:
                    context.abort(grpc.StatusCode.INVALID_ARGUMENT, "empty keyword")
                keyword = entry.keyword.lower()
                count = sum(keyword in task["title"].lower() or
                            keyword in task["description"].lower()
                            for task in self._tasks.values())
                results.append(taskflow_pb2.SearchSummary.KeywordHit(
                    keyword=entry.keyword, match_count=count))
        return taskflow_pb2.SearchSummary(total_requests=len(results), results=results)

    def _get_or_abort(self, task_id: str, context) -> dict:
        task = self._tasks.get(task_id)
        if task is None:
            context.abort(grpc.StatusCode.NOT_FOUND, f"task {task_id} not found")
        return task

    def _to_pb(self, task: dict) -> taskflow_pb2.Task:
        comments = [taskflow_pb2.Comment(author=comment["author"], text=comment["text"],
                                         created_at=comment["created_at"])
                    for comment in task.get("comments", [])]
        return taskflow_pb2.Task(
            id=task["id"], title=task["title"], description=task["description"],
            status=task["status"], assigned_to=task["assigned_to"],
            created_by=task["created_by"], created_at=task["created_at"],
            comments=comments)

    def _publish(self, event_type: str, task_id: str, author: str, message: str):
        event = taskflow_pb2.TaskEvent(event_type=event_type, task_id=task_id,
                                       author=author, message=message)
        with self._subs_lock:
            for _, event_types, event_queue in self._subscribers:
                if not event_types or event_type in event_types:
                    event_queue.put(event)


def serve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--slow", action="store_true")
    args = parser.parse_args()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=32),
                         interceptors=[LoggingInterceptor()])
    taskflow_pb2_grpc.add_TaskFlowServicer_to_server(TaskFlowService(args.slow), server)
    server.add_insecure_port(f"[::]:{args.port}")
    server.start()
    print(f"TaskFlow server listening on :{args.port}", flush=True)
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        server.stop(grace=1)


if __name__ == "__main__":
    serve()