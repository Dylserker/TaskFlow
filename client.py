import argparse
import threading
import tkinter as tk
from tkinter import messagebox, ttk

import grpc

import taskflow_pb2
import taskflow_pb2_grpc
from interceptors import HeaderInterceptor


STATUS_NAMES = {0: "TODO", 1: "IN_PROGRESS", 2: "DONE"}
STATUS_VALUES = {name: value for value, name in STATUS_NAMES.items()}


class TaskFlowApp:
    def __init__(self, root, host, port, username):
        self.root = root
        self.root.title("TaskFlow")
        self.root.geometry("1000x680")
        self.root.minsize(820, 560)
        self.host = tk.StringVar(value=host)
        self.port = tk.StringVar(value=str(port))
        self.username = tk.StringVar(value=username)
        self.status_filter = tk.StringVar(value="Tous")
        self.assigned_filter = tk.StringVar()
        self.selected_id = None
        self.channel = None
        self.stub = None
        self.events = []
        self._build_style()
        self._build_ui()
        self.root.protocol("WM_DELETE_WINDOW", self.close)
        if username:
            self.connect()

    def _build_style(self):
        style = ttk.Style()
        if "clam" in style.theme_names():
            style.theme_use("clam")
        style.configure("Title.TLabel", font=("TkDefaultFont", 18, "bold"))
        style.configure("Action.TButton", padding=(10, 6))

    def _build_ui(self):
        connection = ttk.LabelFrame(self.root, text="Connexion gRPC", padding=10)
        connection.pack(fill="x", padx=14, pady=(14, 8))
        for label, variable, width in (("Hote", self.host, 18), ("Port", self.port, 8),
                                       ("Utilisateur", self.username, 18)):
            ttk.Label(connection, text=label).pack(side="left", padx=(0, 5))
            ttk.Entry(connection, textvariable=variable, width=width).pack(side="left", padx=(0, 12))
        ttk.Button(connection, text="Connecter", command=self.connect,
                   style="Action.TButton").pack(side="left")
        self.connection_label = ttk.Label(connection, text="Non connecte")
        self.connection_label.pack(side="left", padx=12)
        ttk.Label(self.root, text="TaskFlow", style="Title.TLabel").pack(anchor="w", padx=16, pady=4)
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=14, pady=(0, 14))
        self._build_tasks_tab()
        self._build_create_tab()
        self._build_search_tab()
        self._build_events_tab()

    def _build_tasks_tab(self):
        tab = ttk.Frame(self.notebook, padding=12)
        self.notebook.add(tab, text="Taches")
        filters = ttk.Frame(tab)
        filters.pack(fill="x", pady=(0, 10))
        ttk.Label(filters, text="Statut").pack(side="left")
        ttk.Combobox(filters, textvariable=self.status_filter, state="readonly",
                     values=("Tous", "TODO", "IN_PROGRESS", "DONE"), width=15).pack(side="left", padx=6)
        ttk.Label(filters, text="Assigne").pack(side="left", padx=(12, 0))
        ttk.Entry(filters, textvariable=self.assigned_filter, width=18).pack(side="left", padx=6)
        ttk.Button(filters, text="Actualiser", command=self.load_tasks,
                   style="Action.TButton").pack(side="left", padx=8)
        columns = ("id", "title", "status", "assigned", "comments")
        self.task_tree = ttk.Treeview(tab, columns=columns, show="headings", selectmode="browse")
        headings = {"id": "ID", "title": "Titre", "status": "Statut",
                    "assigned": "Assigne a", "comments": "Commentaires"}
        widths = {"id": 110, "title": 280, "status": 130, "assigned": 140, "comments": 100}
        for column in columns:
            self.task_tree.heading(column, text=headings[column])
            self.task_tree.column(column, width=widths[column], anchor="w")
        self.task_tree.pack(fill="both", expand=True)
        self.task_tree.bind("<<TreeviewSelect>>", self._select_task)
        actions = ttk.Frame(tab)
        actions.pack(fill="x", pady=(10, 0))
        ttk.Button(actions, text="Voir les details", command=self.show_task).pack(side="left")
        ttk.Button(actions, text="Changer le statut", command=self.change_status).pack(side="left", padx=6)
        ttk.Button(actions, text="Reassigner", command=self.assign_task).pack(side="left")
        ttk.Button(actions, text="Commenter", command=self.comment_task).pack(side="left", padx=6)
        ttk.Button(actions, text="Supprimer", command=self.delete_task).pack(side="left")

    def _build_create_tab(self):
        tab = ttk.Frame(self.notebook, padding=18)
        self.notebook.add(tab, text="Nouvelle tache")
        form = ttk.Frame(tab)
        form.pack(anchor="nw", fill="x")
        self.create_title = tk.StringVar()
        self.create_assignee = tk.StringVar()
        ttk.Label(form, text="Titre").grid(row=0, column=0, sticky="w", pady=5)
        ttk.Entry(form, textvariable=self.create_title, width=60).grid(row=0, column=1, sticky="ew", pady=5)
        ttk.Label(form, text="Assigne a").grid(row=1, column=0, sticky="w", pady=5)
        ttk.Entry(form, textvariable=self.create_assignee, width=60).grid(row=1, column=1, sticky="ew", pady=5)
        ttk.Label(form, text="Description").grid(row=2, column=0, sticky="nw", pady=5)
        self.create_description = tk.Text(form, height=8, width=60)
        self.create_description.grid(row=2, column=1, sticky="ew", pady=5)
        form.columnconfigure(1, weight=1)
        ttk.Button(tab, text="Creer la tache", command=self.create_task,
                   style="Action.TButton").pack(anchor="w", pady=14)

    def _build_search_tab(self):
        tab = ttk.Frame(self.notebook, padding=18)
        self.notebook.add(tab, text="Recherche")
        ttk.Label(tab, text="Mots-cles (separes par des virgules)").pack(anchor="w")
        self.keywords = tk.StringVar()
        ttk.Entry(tab, textvariable=self.keywords, width=70).pack(anchor="w", pady=8)
        ttk.Button(tab, text="Rechercher", command=self.search_keywords).pack(anchor="w")
        self.search_result = tk.Text(tab, height=15, state="disabled")
        self.search_result.pack(fill="both", expand=True, pady=(14, 0))

    def _build_events_tab(self):
        tab = ttk.Frame(self.notebook, padding=12)
        self.notebook.add(tab, text="Evenements")
        self.events_text = tk.Text(tab, state="disabled", wrap="word")
        self.events_text.pack(fill="both", expand=True)
        ttk.Button(tab, text="Vider le journal", command=self.clear_events).pack(anchor="e", pady=(8, 0))

    def _rpc(self, operation, callback):
        if not self.stub:
            messagebox.showwarning("Connexion", "Connectez-vous d'abord au serveur.")
            return

        def worker():
            try:
                result = operation()
                self.root.after(0, lambda: callback(result, None))
            except grpc.RpcError as error:
                self.root.after(0, lambda: callback(None, error))

        threading.Thread(target=worker, daemon=True).start()

    def connect(self):
        username = self.username.get().strip()
        if not username:
            messagebox.showwarning("Connexion", "Indiquez un utilisateur.")
            return
        if self.channel:
            self.channel.close()
        base_channel = grpc.insecure_channel(f"{self.host.get()}:{self.port.get()}")
        self.channel = grpc.intercept_channel(base_channel, HeaderInterceptor(username))
        self.stub = taskflow_pb2_grpc.TaskFlowStub(self.channel)
        self.connection_label.config(text=f"Connecte: {username}")
        threading.Thread(target=self.listen_events, daemon=True).start()
        self.load_tasks()

    def listen_events(self):
        try:
            stream = self.stub.Subscribe(taskflow_pb2.SubscribeRequest(
                username=self.username.get().strip()))
            for event in stream:
                self.root.after(0, self.add_event, event)
        except grpc.RpcError:
            pass

    def load_tasks(self):
        status = self.status_filter.get()
        assigned = self.assigned_filter.get().strip()

        def operation():
            request = taskflow_pb2.ListTasksRequest()
            if status != "Tous":
                request.status_filter = STATUS_VALUES[status]
            if assigned:
                request.assigned_filter = assigned
            return list(self.stub.ListTasks(request, timeout=5))

        self._rpc(operation, self.show_tasks)

    def show_tasks(self, tasks, error):
        if error:
            self.show_error(error)
            return
        for item in self.task_tree.get_children():
            self.task_tree.delete(item)
        for task in tasks:
            self.task_tree.insert("", "end", iid=task.id, values=(
                task.id, task.title, STATUS_NAMES[task.status],
                task.assigned_to or "-", len(task.comments)))

    def _select_task(self, _event):
        selection = self.task_tree.selection()
        self.selected_id = selection[0] if selection else None

    def _selected_or_warn(self):
        if not self.selected_id:
            messagebox.showinfo("Selection", "Selectionnez une tache dans la liste.")
            return None
        return self.selected_id

    def create_task(self):
        title = self.create_title.get().strip()
        if not title:
            messagebox.showwarning("Tache", "Le titre est obligatoire.")
            return
        request = taskflow_pb2.CreateTaskRequest(
            title=title, description=self.create_description.get("1.0", "end").strip(),
            assigned_to=self.create_assignee.get().strip(), created_by=self.username.get().strip())
        self._rpc(lambda: self.stub.CreateTask(request, timeout=5), self.created)

    def created(self, _response, error):
        if error:
            self.show_error(error)
            return
        self.create_title.set("")
        self.create_assignee.set("")
        self.create_description.delete("1.0", "end")
        self.notebook.select(0)
        self.load_tasks()

    def show_task(self):
        task_id = self._selected_or_warn()
        if task_id:
            self._rpc(lambda: self.stub.GetTask(
                taskflow_pb2.GetTaskRequest(id=task_id), timeout=5), self.display_task)

    def display_task(self, task, error):
        if error:
            self.show_error(error)
            return
        comments = "\n".join(f"{comment.author}: {comment.text}" for comment in task.comments) or "Aucun"
        messagebox.showinfo("Details de la tache", f"Titre: {task.title}\nStatut: {STATUS_NAMES[task.status]}\n"
                            f"Assigne a: {task.assigned_to or '-'}\n\n{task.description}\n\nCommentaires:\n{comments}")

    def change_status(self):
        task_id = self._selected_or_warn()
        if not task_id:
            return
        dialog = tk.Toplevel(self.root)
        dialog.title("Changer le statut")
        value = tk.StringVar(value="TODO")
        ttk.Label(dialog, text="Nouveau statut").pack(padx=18, pady=(18, 6))
        ttk.Combobox(dialog, textvariable=value, state="readonly",
                     values=("TODO", "IN_PROGRESS", "DONE")).pack(padx=18)
        ttk.Button(dialog, text="Valider", command=lambda: self._submit_status(
            dialog, task_id, value.get())).pack(pady=18)

    def _submit_status(self, dialog, task_id, status):
        dialog.destroy()
        request = taskflow_pb2.UpdateStatusRequest(id=task_id, new_status=STATUS_VALUES[status],
                                                    requested_by=self.username.get().strip())
        self._rpc(lambda: self.stub.UpdateStatus(request, timeout=5), self.action_done)

    def assign_task(self):
        task_id = self._selected_or_warn()
        if task_id:
            self._ask_action(task_id, "Nouvel assigne", self._assign)

    def comment_task(self):
        task_id = self._selected_or_warn()
        if task_id:
            self._ask_action(task_id, "Commentaire", self._comment)

    def _ask_action(self, task_id, label, callback):
        dialog = tk.Toplevel(self.root)
        dialog.title(label)
        value = tk.StringVar()
        ttk.Label(dialog, text=label).pack(padx=18, pady=(18, 6))
        entry = ttk.Entry(dialog, textvariable=value, width=42)
        entry.pack(padx=18)
        entry.focus_set()
        ttk.Button(dialog, text="Valider", command=lambda: self._finish_action(
            dialog, callback, task_id, value.get().strip())).pack(pady=18)

    @staticmethod
    def _finish_action(dialog, callback, task_id, value):
        dialog.destroy()
        callback(task_id, value)

    def _assign(self, task_id, assignee):
        request = taskflow_pb2.AssignTaskRequest(id=task_id, new_assignee=assignee,
                                                  requested_by=self.username.get().strip())
        self._rpc(lambda: self.stub.AssignTask(request, timeout=5), self.action_done)

    def _comment(self, task_id, text):
        request = taskflow_pb2.AddCommentRequest(id=task_id, author=self.username.get().strip(), text=text)
        self._rpc(lambda: self.stub.AddComment(request, timeout=5), self.action_done)

    def delete_task(self):
        task_id = self._selected_or_warn()
        if not task_id or not messagebox.askyesno("Supprimer", "Supprimer cette tache ?"):
            return
        request = taskflow_pb2.DeleteTaskRequest(id=task_id, requested_by=self.username.get().strip())
        self._rpc(lambda: self.stub.DeleteTask(request, timeout=5), self.action_done)

    def action_done(self, _result, error):
        if error:
            self.show_error(error)
            return
        self.load_tasks()

    def search_keywords(self):
        values = [value.strip() for value in self.keywords.get().split(",") if value.strip()]
        if not values:
            messagebox.showwarning("Recherche", "Indiquez au moins un mot-cle.")
            return
        self._rpc(lambda: self.stub.SearchKeywords(
            (taskflow_pb2.SearchEntry(keyword=value) for value in values), timeout=5), self.display_search)

    def display_search(self, summary, error):
        if error:
            self.show_error(error)
            return
        result = f"{summary.total_requests} requete(s)\n\n" + "\n".join(
            f"{hit.keyword}: {hit.match_count} tache(s)" for hit in summary.results)
        self.search_result.config(state="normal")
        self.search_result.delete("1.0", "end")
        self.search_result.insert("1.0", result)
        self.search_result.config(state="disabled")

    def add_event(self, event):
        self.events.append(event)
        self.events_text.config(state="normal")
        self.events_text.insert("end", f"[{event.event_type}] {event.author}: {event.message}\n")
        self.events_text.see("end")
        self.events_text.config(state="disabled")

    def clear_events(self):
        self.events.clear()
        self.events_text.config(state="normal")
        self.events_text.delete("1.0", "end")
        self.events_text.config(state="disabled")

    @staticmethod
    def show_error(error):
        messagebox.showerror("Erreur gRPC", f"{error.code().name}: {error.details()}")

    def close(self):
        if self.channel:
            self.channel.close()
        self.root.destroy()


def main():
    parser = argparse.ArgumentParser(description="Interface graphique TaskFlow")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--user", default="")
    args = parser.parse_args()
    root = tk.Tk()
    TaskFlowApp(root, args.host, args.port, args.user)
    root.mainloop()


if __name__ == "__main__":
    main()