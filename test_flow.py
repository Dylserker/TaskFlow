import queue
import subprocess
import sys
import tempfile
import threading
import time

import grpc

import taskflow_pb2 as pb
import taskflow_pb2_grpc
from interceptors import HeaderInterceptor

PORT = 50061


def expect_error(fn, code):
    try:
        fn()
    except grpc.RpcError as error:
        assert error.code() == code, f"attendu {code}, recu {error.code()}"
        return
    raise AssertionError(f"attendu une erreur {code}, aucun echec")


def run_tests(stub, log_path):
    created = stub.CreateTask(pb.CreateTaskRequest(title="Rapport", assigned_to="alice", created_by="alice"), timeout=3)
    task_id = created.task.id
    task = stub.GetTask(pb.GetTaskRequest(id=task_id), timeout=3)
    assert task.title == "Rapport" and task.status == pb.TODO
    task = stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.DONE, requested_by="alice"), timeout=3)
    assert task.status == pb.DONE
    assert any(item.id == task_id for item in stub.ListTasks(pb.ListTasksRequest(), timeout=3))
    done = list(stub.ListTasks(pb.ListTasksRequest(status_filter=pb.DONE), timeout=3))
    assert len(done) == 1 and done[0].id == task_id
    expect_error(lambda: stub.GetTask(pb.GetTaskRequest(id="inconnu"), timeout=3), grpc.StatusCode.NOT_FOUND)
    expect_error(lambda: stub.CreateTask(pb.CreateTaskRequest(created_by="alice"), timeout=3), grpc.StatusCode.INVALID_ARGUMENT)
    expect_error(lambda: stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.DONE, requested_by="alice"), timeout=3), grpc.StatusCode.INVALID_ARGUMENT)
    expect_error(lambda: stub.UpdateStatus(pb.UpdateStatusRequest(id=task_id, new_status=pb.IN_PROGRESS, requested_by="alice"), timeout=3), grpc.StatusCode.INVALID_ARGUMENT)
    expect_error(lambda: stub.DeleteTask(pb.DeleteTaskRequest(id=task_id, requested_by="bob"), timeout=3), grpc.StatusCode.PERMISSION_DENIED)
    stub.DeleteTask(pb.DeleteTaskRequest(id=task_id, requested_by="alice"), timeout=3)
    expect_error(lambda: stub.GetTask(pb.GetTaskRequest(id=task_id), timeout=3), grpc.StatusCode.NOT_FOUND)

    for title in ("alpha", "beta", "alpha beta"):
        stub.CreateTask(pb.CreateTaskRequest(title=title, created_by="alice"), timeout=3)
    summary = stub.SearchKeywords((pb.SearchEntry(keyword=value) for value in ("alpha", "BETA")), timeout=3)
    assert summary.total_requests == 2
    assert [hit.match_count for hit in summary.results] == [2, 2]
    expect_error(lambda: stub.SearchKeywords(iter([pb.SearchEntry(keyword=""), pb.SearchEntry(keyword="x")]), timeout=3), grpc.StatusCode.INVALID_ARGUMENT)

    subscribe_channel = grpc.intercept_channel(grpc.insecure_channel(f"localhost:{PORT}"), HeaderInterceptor("testeur"))
    subscribe_stub = taskflow_pb2_grpc.TaskFlowStub(subscribe_channel)
    events = queue.Queue()
    stream = subscribe_stub.Subscribe(pb.SubscribeRequest(username="testeur", event_types=["DELETED"]))
    def collect_events():
        try:
            for event in stream:
                events.put(event)
        except grpc.RpcError as error:
            if error.code() != grpc.StatusCode.CANCELLED:
                raise

    threading.Thread(target=collect_events, daemon=True).start()
    time.sleep(0.3)
    temporary = stub.CreateTask(pb.CreateTaskRequest(title="temporary", created_by="alice"), timeout=3)
    stub.DeleteTask(pb.DeleteTaskRequest(id=temporary.task.id, requested_by="alice"), timeout=3)
    assert events.get(timeout=2).event_type == "DELETED"
    subscribe_channel.close()

    with open(log_path, encoding="utf-8") as log:
        content = log.read()
    assert "user=testeur" in content
    assert "code=NOT_FOUND" in content


def main():
    log = tempfile.NamedTemporaryFile("w+", suffix=".log", delete=False)
    process = subprocess.Popen([sys.executable, "server.py", "--port", str(PORT)], stdout=log, stderr=subprocess.STDOUT)
    channel = grpc.intercept_channel(grpc.insecure_channel(f"localhost:{PORT}"), HeaderInterceptor("testeur"))
    try:
        grpc.channel_ready_future(channel).result(timeout=10)
        run_tests(taskflow_pb2_grpc.TaskFlowStub(channel), log.name)
        print("Tous les tests passent.")
    finally:
        channel.close()
        process.terminate()
        process.wait()
        log.close()


if __name__ == "__main__":
    main()