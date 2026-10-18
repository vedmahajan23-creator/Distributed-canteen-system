# Distributed Canteen Order Management System

A **Distributed Systems mini project (FA-1 + FA-2)** — independent services
(3 canteen nodes + 1 middleware) that cooperate over the network to take,
cook and track canteen orders.

> **FA-2 (Unit 3 + Unit 4) guide with demo script and viva Q&A: [docs/FA2_GUIDE.md](docs/FA2_GUIDE.md)**
> Open `/sync` (Unit 3) and `/paradigms` (Unit 4) after starting.

> FA-1 explanation, architecture, concept mapping and viva Q&A:
> **[docs/PROJECT_GUIDE.md](docs/PROJECT_GUIDE.md)** (also available as `docs/PROJECT_GUIDE.pdf`)

---

## Architecture (one laptop, three processes)

```
   Student's Browser  (HTML/JS pages)
          |
          |  HTTP  (browser pages + SSE stream)
          v
   ORDER SERVICE  - middleware        localhost:5000
          |
          |  XML-RPC  (real remote procedures)
          v
   CANTEEN NODE 1  <== P2P ==>  CANTEEN NODE 2      CANTEEN NODE 3 (FA-2)
   localhost:5001                localhost:5002          localhost:5003
   canteen_node_1.db             canteen_node_2.db       canteen_node_3.db
```

## Project structure

```
ds_01/
├── canteen_node.py       # Independent distributed node (run TWICE = 2 nodes)
├── order_service.py      # Middleware + serves the student web pages
├── cluster_config.py     # FA-2: node list, middleware URL, simulated clock skews
├── fa2/                  # FA-2 code
│   ├── node_sync.py      #   Unit 3 (node): clocks, Cristian/Berkeley, beacon, bully, mutex, snapshot
│   ├── node_paradigms.py #   Unit 4 (node): remote objects, DFS data node, MapReduce map
│   ├── mw_unit3.py       #   Unit 3 (middleware): time server, dashboard API
│   ├── mw_unit4.py       #   Unit 4 (middleware): object stub, web cache, DFS/MR/FaaS APIs
│   ├── dfs.py            #   NameNode of the mini-HDFS
│   ├── mapreduce.py      #   MapReduce driver
│   └── serverless.py     #   Function-as-a-Service engine
├── templates/
│   ├── index.html        # Menu + cart + place order (RPC call)
│   ├── track.html        # Live order tracking (SSE stream)
│   ├── nodes.html        # Node status dashboard (up/down, load)
│   ├── webrtc.html       # WebRTC proof-of-concept (DataChannel chat)
│   ├── fa2_base.html     # FA-2 shared layout
│   ├── fa2_sync.html     # FA-2 Unit 3 dashboard (/sync)
│   └── fa2_paradigms.html# FA-2 Unit 4 dashboard (/paradigms)
├── static/fa2.css        # Shared visual system and dashboard styles
├── run_all.bat           # Starts all three services in separate windows
├── requirements.txt      # flask, requests (only 2 dependencies)
└── docs/
    ├── PROJECT_GUIDE.md  # Full academic documentation
    ├── PROJECT_GUIDE.pdf # Same guide as PDF
    └── make_pdf.py       # Script that generated the PDF
```

## Install

```
pip install -r requirements.txt
```

## Run (Windows)

Easiest — one command opens four terminal windows:

```
run_all.bat
```

Or manually, one command per terminal:

```
python canteen_node.py --id 1 --port 5001 --peer http://localhost:5002
python canteen_node.py --id 2 --port 5002 --peer http://localhost:5001
python canteen_node.py --id 3 --port 5003 --peer http://localhost:5001
python order_service.py
```

Then open **http://localhost:5000** in the browser.

## Demo — prove every DS concept live

| # | Concept | How to demonstrate |
|---|---------|--------------------|
| 1 | Distributed architecture | Three terminal windows = three OS processes talking over HTTP. Show `canteen_node_1.db` and `canteen_node_2.db` are separate files. |
| 2 | Middleware | Browser only ever calls `localhost:5000`. The middleware queries both nodes, scores their workload, and routes the order. |
| 3 | RPC | Open Node Status → RPC Communication Monitor. The middleware uses XML-RPC `PlaceOrder()`, `GetMenu()`, `GetNodeHealth()`, `GetNodeLoad()` and `GetOrderStatus()` calls against the nodes. |
| 4 | Message-oriented communication | Node terminal prints `message queued` (producer) and later `kitchen received message` (consumer thread). |
| 5 | Stream-oriented communication | Track page: statuses Pending → Preparing → Ready → Completed appear live with NO refresh (one open SSE connection, visible in DevTools). |
| 6 | Peer-to-peer | Place 4 orders quickly → Node 1 hits its busy limit (3) and hands the 4th order DIRECTLY to Node 2 ("handed over via P2P"). |
| 7 | Fault tolerance | Ctrl+C in the Node 1 window → Node Status page shows N1 DOWN → place an order → it is served by Node 2 automatically. |
| 8 | WebRTC | Open the WebRTC Demo page → Connect Peers → chat over a direct browser-to-browser DataChannel; the signaling log shows the offer/answer handshake. |

## FA-2 demo summary (details in docs/FA2_GUIDE.md)

| Unit | Concept | Where |
|------|---------|-------|
| 3 | Lamport + vector clocks, causality compare | `/sync` -> Logical clocks (orders also store `lamport_ts`, `vector_ts`) |
| 3 | Cristian + Berkeley clock sync, beacon protocol | `/sync` -> Clock synchronization / Election (beacons) |
| 3 | Bully election | Crash the coordinator on `/sync` |
| 3 | Mutual exclusion (Ricart-Agrawala, centralised) | `/sync` -> Mutual exclusion |
| 3 | Global state (Chandy-Lamport) | `/sync` -> Global state |
| 4 | Distributed objects | `/paradigms` A (`RemoteObject` stub, `InvokeObject` RPC) |
| 4 | Web-based system (cache, ETag, replication) | `/paradigms` B |
| 4 | Distributed file system + Hadoop MapReduce | `/paradigms` C, D |
| 4 | Serverless | `/paradigms` E (receipt generated on every order) |

## Reset the data

Just delete the node databases (they are recreated with a fresh menu on next start):

```
del canteen_node_1.db canteen_node_2.db canteen_node_3.db
rmdir /s /q dfs_data_n1 dfs_data_n2 dfs_data_n3
del namenode_meta.json
```
