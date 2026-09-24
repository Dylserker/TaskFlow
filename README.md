# TaskFlow

Gestionnaire de taches collaboratif realise en Python 3.10+ avec gRPC.

## Installation et lancement

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirement.txt
python -m grpc_tools.protoc -Iprotos --python_out=. --grpc_python_out=. protos/taskflow.proto
python server.py
```

Le `source venv/bin/activate` est indispensable dans chaque nouveau terminal.
Le prompt doit alors commencer par `(venv)`. Sinon, utilisez directement
`venv/bin/python server.py` ou `venv/bin/python client.py --user alice`.

Si le venv a ete supprime ou est incomplet, recreez-le avec `python -m venv venv`,
puis relancez `venv/bin/python -m pip install -r requirement.txt`.

Dans un autre terminal: `python client.py --user alice`. Le test complet se lance avec
`python test_flow.py`. Les fichiers `taskflow_pb2.py` et `taskflow_pb2_grpc.py` sont
generes et ignores par Git.

## Comprendre le contrat protobuf

`TaskStatus` est un enum et `TODO = 0` est sa valeur par defaut en proto3. Un champ
scalaire non optional comme `new_status` ne permet donc pas de distinguer une valeur
explicitement envoyee de sa valeur par defaut; `HasField` leve une erreur dans ce cas.
Les champs `status_filter` et `assigned_filter` sont declares `optional`, donc
`HasField` permet de distinguer filtre absent et filtre present.

`repeated Comment comments` represente une collection ordonnee de commentaires,
alors qu'un champ scalaire contient une seule valeur. `KeywordHit` est imbrique dans
`SearchSummary` car il n'a de sens que comme resultat de cette synthese et evite de
polluer l'API globale avec un message local.

## Choix d'architecture

Les RPC unary conviennent aux mutations ou lectures unitaires (`CreateTask`,
`GetTask`). `ListTasks` et `Subscribe` sont server-streaming: le serveur produit
plusieurs taches ou notifications sans que le client doive interroger en boucle.
`SearchKeywords` est client-streaming car le client envoie plusieurs mots-cles, puis
recoit une synthese unique.

Le dictionnaire interne est protege par un `RLock`: plusieurs threads gRPC peuvent
modifier simultanement une tache, par exemple une reassignation et un commentaire.
Sans verrou, une lecture ou une copie pourrait observer un etat partiellement mis a
jour. `RLock` permet aussi a une methode auxiliaire appelee sous verrou de reprendre
le meme verrou sans deadlock. Les messages protobuf sont reconstruits pour chaque
reponse afin d'eviter qu'un thread modifie une instance partagee pendant sa
serialisation.

`Subscribe` utilise une file par client et un callback de contexte. A la deconnexion,
le callback reveille `q.get()` et le `finally` retire l'abonne. Sans ce nettoyage,
les files et references s'accumuleraient et les flux occuperaient progressivement
les threads du pool.

Pour une API REST, un flux de notifications pourrait utiliser du polling, du SSE ou
un WebSocket. Le polling ajoute latence et requetes inutiles; SSE est adapte au sens
serveur vers client; WebSocket permet aussi des messages dans les deux sens, au prix
d'une gestion de connexion plus riche.

Un `abort` pendant un flux termine le RPC avec un statut d'erreur dans les trailers.
Le client peut avoir recu les elements deja envoyes, mais sait que le flux complet a
echoue et ne doit pas le traiter comme une reponse complete.

## Validation du binome

Auteur et validateur: a completer avec les initiales des deux membres avant le rendu.
Desaccord technique a consigner: le choix entre messages protobuf partages et
dictionnaires internes a ete tranche en faveur de dictionnaires proteges par verrou,
puis de copies protobuf par reponse afin d'eviter les mutations croisees.
