import argparse
import threading

import grpc

import taskflow_pb2
import taskflow_pb2_grpc
from interceptors import HeaderInterceptor


STATUS_NAMES = {0: "TODO", 1: "IN_PROGRESS", 2: "DONE"}
received_events = []


def print_event(event):
    print(f"\n[{event.event_type}] {event.author}: {event.message}\n> ", end="", flush=True)


def listen_events(stub, username, event_types):
    try:
        stream = stub.Subscribe(taskflow_pb2.SubscribeRequest(username=username, event_types=event_types))
        for event in stream:
            received_events.append(event)
            print_event(event)
    except grpc.RpcError as error:
        if error.code() != grpc.StatusCode.CANCELLED:
            print(f"flux coupe [{error.code().name}] : {error.details()}")


def print_task(task):
    print(f"  [{STATUS_NAMES[task.status]:12}] {task.id[:8]}... « {task.title} » -> "
          f"{task.assigned_to or 'non assignee'} ({len(task.comments)} commentaire(s))")


def main():
    parser = argparse.ArgumentParser(description="Client TaskFlow")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--user", required=True)
    parser.add_argument("--events", default="")
    parser.add_argument("--no-listen", action="store_true")
    parser.add_argument("--timeout", type=float, default=3)
    args = parser.parse_args()
    base_channel = grpc.insecure_channel(f"{args.host}:{args.port}")
    channel = grpc.intercept_channel(base_channel, HeaderInterceptor(args.user))
    stub = taskflow_pb2_grpc.TaskFlowStub(channel)
    event_types = [event.strip().upper() for event in args.events.split(",") if event.strip()]
    if not args.no_listen:
        threading.Thread(target=listen_events, args=(stub, args.user, event_types), daemon=True).start()

    while True:
        print(f"""
=== TaskFlow === (utilisateur: {args.user})
 1. Creer une tache        6. Commenter une tache
 2. Lister les taches      7. Supprimer une tache
 3. Voir une tache          8. Recherche multi-mots-cles
 4. Changer le statut       9. Evenements recus
 5. Reassigner              0. Quitter""")
        try:
            choice = input("choix > ").strip()
            if choice == "1":
                response = stub.CreateTask(taskflow_pb2.CreateTaskRequest(
                    title=input("titre > ").strip(), description=input("description > ").strip(),
                    assigned_to=input("assigne a (vide = personne) > ").strip(), created_by=args.user),
                    timeout=args.timeout)
                print(f"Tache creee: {response.task.id}")
            elif choice == "2":
                status = input("statut (TODO, IN_PROGRESS, DONE, vide = tous) > ").strip().upper()
                assigned = input("assigne (vide = tous) > ").strip()
                request = taskflow_pb2.ListTasksRequest()
                if status:
                    request.status_filter = {name: value for value, name in STATUS_NAMES.items()}[status]
                if assigned:
                    request.assigned_filter = assigned
                for task in stub.ListTasks(request, timeout=args.timeout):
                    print_task(task)
            elif choice == "3":
                task = stub.GetTask(taskflow_pb2.GetTaskRequest(id=input("id > ").strip()), timeout=args.timeout)
                print_task(task)
                for comment in task.comments:
                    print(f"  {comment.created_at.ToDatetime().isoformat()} {comment.author}: {comment.text}")
            elif choice == "4":
                status = input("statut (TODO, IN_PROGRESS, DONE) > ").strip().upper()
                task = stub.UpdateStatus(taskflow_pb2.UpdateStatusRequest(
                    id=input("id > ").strip(), new_status={name: value for value, name in STATUS_NAMES.items()}[status],
                    requested_by=args.user), timeout=args.timeout)
                print_task(task)
            elif choice == "5":
                task = stub.AssignTask(taskflow_pb2.AssignTaskRequest(
                    id=input("id > ").strip(), new_assignee=input("nouvel assigne > ").strip(),
                    requested_by=args.user), timeout=args.timeout)
                print_task(task)
            elif choice == "6":
                task = stub.AddComment(taskflow_pb2.AddCommentRequest(
                    id=input("id > ").strip(), author=args.user, text=input("commentaire > ").strip()), timeout=args.timeout)
                print_task(task)
            elif choice == "7":
                stub.DeleteTask(taskflow_pb2.DeleteTaskRequest(
                    id=input("id > ").strip(), requested_by=args.user), timeout=args.timeout)
                print("Tache supprimee")
            elif choice == "8":
                keywords = []
                while True:
                    keyword = input("mot-cle (vide = fin) > ").strip()
                    if not keyword:
                        break
                    keywords.append(keyword)
                summary = stub.SearchKeywords((taskflow_pb2.SearchEntry(keyword=keyword) for keyword in keywords), timeout=args.timeout)
                print(f"{summary.total_requests} requete(s)")
                for hit in summary.results:
                    print(f"  {hit.keyword}: {hit.match_count}")
            elif choice == "9":
                print(f"{len(received_events)} evenement(s) recu(s)")
            elif choice == "0":
                print("Au revoir !")
                break
        except grpc.RpcError as error:
            print(f"Erreur gRPC [{error.code().name}] : {error.details()}")
        except (KeyboardInterrupt, EOFError):
            print()
            break
    channel.close()


if __name__ == "__main__":
    main()