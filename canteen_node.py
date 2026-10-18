"""
CANTEEN NODE  (Distributed System - independent node)
=====================================================
This file represents ONE independent node of our distributed system.
We run it TWICE (with different --id / --port / --db) to get two nodes:

    python canteen_node.py --id 1 --port 5001 --peer http://localhost:5002
    python canteen_node.py --id 2 --port 5002 --peer http://localhost:5001

DS concepts demonstrated in this file:
  1. DISTRIBUTED NODE : each node is a separate OS process, with its own
     private SQLite database. Nodes share NOTHING except the network.
  2. MESSAGE-ORIENTED COMMUNICATION : new orders are put on an in-memory
     message queue (queue.Queue). A background "kitchen worker" thread
     consumes messages asynchronously - the sender does not wait.
  3. PEER-TO-PEER COMMUNICATION : if this node is busy, it talks DIRECTLY
     to the other canteen node (no middleware involved) and hands the
     order over. See place_order() and /peer/orders.

FA-2 additions (Unit 3 + Unit 4) live in the  fa2/  package and are plugged in
below:  logical clocks, clock sync, beacon, election, mutual exclusion and
snapshots (fa2/node_sync.py)  +  remote objects, DFS data node and MapReduce
worker (fa2/node_paradigms.py).
"""

import argparse
import io
import json
import logging
import queue
import sqlite3
import threading
import time
import uuid
from datetime import datetime

import requests
from flask import Flask, Response, jsonify, request
from xmlrpc.server import SimpleXMLRPCDispatcher

from cluster_config import CLUSTER, DEFAULT_SKEW, MIDDLEWARE_URL
from fa2 import node_paradigms, node_sync

app = Flask(__name__)
app.register_blueprint(node_sync.bp)          # UNIT 3 endpoints  /sync/...
app.register_blueprint(node_paradigms.bp)     # UNIT 4 endpoints  /dfs/... /mr/...

class _QuietLogs(logging.Filter):
    """Keep the terminal readable: beacons/polls happen every few seconds."""
    NOISY = ("/sync/beacon", "/sync/state", "/sync/time", "/dfs/chunks")

    def filter(self, record):
        return not any(p in record.getMessage() for p in self.NOISY)


# ---- node configuration (filled from command line in main) ----
NODE_ID = 1
PEER_URL = ""          # base URL of the other canteen node
DB_FILE = ""           # each node has its OWN database file

# A node is "busy" when it already has this many unfinished orders.
BUSY_LIMIT = 3

# MESSAGE QUEUE: producer = HTTP handler, consumer = kitchen worker thread.
order_queue = queue.Queue()
rpc_dispatcher = SimpleXMLRPCDispatcher(allow_none=True, encoding=None)


# ----------------------------------------------------------------------
# Database helpers (plain SQLite - one private DB file per node)
# ----------------------------------------------------------------------
def get_db():
    """Open a fresh connection (safe with multiple threads)."""
    con = sqlite3.connect(DB_FILE)
    con.row_factory = sqlite3.Row
    return con


def init_db():
    """Create tables and seed the menu on first run."""
    con = get_db()
    con.execute("""CREATE TABLE IF NOT EXISTS menu (
                     id INTEGER PRIMARY KEY,
                     name TEXT, price REAL, available INTEGER,
                     prep_minutes INTEGER DEFAULT 5)""")
    con.execute("""CREATE TABLE IF NOT EXISTS orders (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     student_name TEXT, items TEXT, total REAL,
                     status TEXT, note TEXT, created_at TEXT,
                     prep_minutes INTEGER DEFAULT 5,
                     estimated_wait_time INTEGER DEFAULT 5,
                     request_id TEXT,
                     lamport_ts INTEGER, vector_ts TEXT)""")
    menu_columns = {row["name"] for row in con.execute("PRAGMA table_info(menu)")}
    if "prep_minutes" not in menu_columns:
        con.execute("ALTER TABLE menu ADD COLUMN prep_minutes INTEGER DEFAULT 5")
    order_columns = {row["name"] for row in con.execute("PRAGMA table_info(orders)")}
    for column, definition in [
        ("prep_minutes", "INTEGER DEFAULT 5"),
        ("estimated_wait_time", "INTEGER DEFAULT 5"),
        ("request_id", "TEXT"),
        ("lamport_ts", "INTEGER"),      # FA-2: Lamport timestamp of the order event
        ("vector_ts", "TEXT"),          # FA-2: vector timestamp (JSON)
    ]:
        if column not in order_columns:
            con.execute(f"ALTER TABLE orders ADD COLUMN {column} {definition}")
    if con.execute("SELECT COUNT(*) FROM menu").fetchone()[0] == 0:
        con.executemany("INSERT INTO menu (id, name, price, available, prep_minutes) VALUES (?,?,?,?,?)", [
            (1, "Veg Sandwich",   40, 1, 5),
            (2, "Masala Dosa",    60, 1, 8),
            (3, "Samosa",         15, 1, 3),
            (4, "Cold Coffee",    50, 1, 3),
            (5, "Pav Bhaji",      70, 1, 10),
            (6, "Tea",            10, 1, 2),
        ])
    else:
        con.execute("UPDATE menu SET prep_minutes = CASE id "
                    "WHEN 1 THEN 5 WHEN 2 THEN 8 WHEN 3 THEN 3 "
                    "WHEN 4 THEN 3 WHEN 5 THEN 10 WHEN 6 THEN 2 "
                    "ELSE COALESCE(prep_minutes, 5) END "
                    "WHERE prep_minutes IS NULL OR prep_minutes = 5")
    con.commit()
    con.close()


def active_order_count():
    """Orders that the kitchen has not finished yet."""
    con = get_db()
    n = con.execute("SELECT COUNT(*) FROM orders WHERE status NOT IN ('Completed','Cancelled')"
                    ).fetchone()[0]
    con.close()
    return n


def queued_order_count():
    return order_queue.qsize()


def workload_minutes():
    con = get_db()
    total = con.execute(
        "SELECT COALESCE(SUM(prep_minutes), 0) FROM orders "
        "WHERE status NOT IN ('Completed','Cancelled')"
    ).fetchone()[0]
    con.close()
    return int(total) + queued_order_count() * 2


def preparation_for_items(item_ids):
    con = get_db()
    if not item_ids:
        con.close()
        return 5
    placeholders = ",".join("?" for _ in item_ids)
    rows = con.execute(
        f"SELECT id, prep_minutes FROM menu WHERE id IN ({placeholders})",
        [int(item_id) for item_id in item_ids],
    ).fetchall()
    con.close()
    return sum(int(row["prep_minutes"]) for row in rows) or 5


def save_order(student_name, items, total, note, item_ids=None, request_id=None):
    """Insert order as 'Pending' and return its full distributed id."""
    item_ids = item_ids or []
    prep_minutes = preparation_for_items(item_ids)
    estimated_wait = workload_minutes() + prep_minutes
    request_id = request_id or f"RPC-{uuid.uuid4().hex[:8].upper()}"
    # FA-2 (Unit 3): the order is an EVENT -> tick Lamport + vector clock
    ev = node_sync.local_event(f"order accepted ({request_id})")
    con = get_db()
    cur = con.execute(
        "INSERT INTO orders (student_name, items, total, status, note, created_at, "
        "prep_minutes, estimated_wait_time, request_id, lamport_ts, vector_ts)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (student_name, items, total, "Pending", note,
         datetime.now().strftime("%Y-%m-%d %H:%M:%S"), prep_minutes,
         estimated_wait, request_id, ev["lamport"], json.dumps(ev["vector"])))
    con.commit()
    local_id = cur.lastrowid
    con.close()
    # Order id is globally unique across nodes because of the node prefix.
    return f"N{NODE_ID}-{local_id}"


def rpc_get_menu():
    con = get_db()
    rows = con.execute("SELECT * FROM menu WHERE available=1").fetchall()
    con.close()
    return [dict(row) for row in rows]


def rpc_get_node_health():
    started = time.perf_counter()
    return {
        "node_id": f"N{NODE_ID}",
        "status": "HEALTHY",
        "response_time": round((time.perf_counter() - started) * 1000, 2),
        "active_orders": active_order_count(),
        "queue_length": queued_order_count(),
    }


def rpc_get_node_load(request_data=None):
    request_data = request_data or {}
    active = active_order_count()
    queued = queued_order_count()
    workload = workload_minutes()
    order_prep = preparation_for_items(request_data.get("item_ids", []))
    load_percentage = min(100, int((active * 20) + (queued * 10) + (workload * 2)))
    return {
        "node_id": f"N{NODE_ID}",
        "status": "HEALTHY",
        "active_orders": active,
        "queue_length": queued,
        "estimated_wait_time": workload,
        "estimated_wait_for_new_order": workload + order_prep,
        "new_order_prep_minutes": order_prep,
        "load_percentage": load_percentage,
        "health_status": "HEALTHY",
    }


def rpc_place_order(data):
    request_id = data.get("request_id") or f"RPC-{uuid.uuid4().hex[:8].upper()}"
    # FA-2: the middleware piggybacks its Lamport clock on the RPC request
    node_sync.on_receive({"lamport": data.get("lamport", 0), "from": "MW"},
                         f"PlaceOrder RPC from middleware ({request_id})")
    order_id = save_order(
        data["student_name"], data["items"], data["total"],
        data.get("note", ""), data.get("item_ids", []), request_id,
    )
    local_id = int(order_id.split("-")[1])
    order_queue.put(local_id)
    result = {
        "order_id": order_id,
        "status": "Pending",
        "handled_by": f"Node {NODE_ID}",
        "estimated_wait_time": rpc_get_order_status(local_id)["estimated_wait_time"],
        "request_id": request_id,
        "lamport_ts": rpc_get_order_status(local_id)["lamport_ts"],
    }
    print(f"[RPC SERVER] PlaceOrder() request_id={request_id} -> {order_id}", flush=True)
    return result


def rpc_get_order_status(order_id):
    local_id = int(order_id)
    con = get_db()
    row = con.execute("SELECT * FROM orders WHERE id=?", (local_id,)).fetchone()
    con.close()
    if row is None:
        raise ValueError("order not found")
    result = dict(row)
    result["order_id"] = f"N{NODE_ID}-{local_id}"
    return result


rpc_dispatcher.register_function(rpc_place_order, "PlaceOrder")
rpc_dispatcher.register_function(rpc_get_menu, "GetMenu")
rpc_dispatcher.register_function(rpc_get_order_status, "GetOrderStatus")
rpc_dispatcher.register_function(rpc_get_node_health, "GetNodeHealth")
rpc_dispatcher.register_function(rpc_get_node_load, "GetNodeLoad")
# FA-2 (Unit 4): remote-object invocation travels over the SAME RPC channel
rpc_dispatcher.register_function(node_paradigms.invoke, "InvokeObject")


# ----------------------------------------------------------------------
# MESSAGE-ORIENTED COMMUNICATION
# The HTTP handler only *sends a message* and returns immediately.
# This worker thread *receives the message* later and cooks the order.
# ----------------------------------------------------------------------
def kitchen_worker():
    while True:
        local_id = order_queue.get()          # blocks until a message arrives
        print(f"[Node {NODE_ID}] kitchen received message: cook order {local_id}", flush=True)
        for status, seconds in [("Preparing", 6), ("Ready", 8), ("Completed", 8)]:
            time.sleep(seconds)               # simulate cooking time
            con = get_db()
            current = con.execute("SELECT status FROM orders WHERE id=?", (local_id,)).fetchone()
            if current and current["status"] == "Cancelled":     # FA-2: remote object cancel()
                con.close()
                print(f"[Node {NODE_ID}] order {local_id} was cancelled - kitchen stops", flush=True)
                break
            con.execute("UPDATE orders SET status=? WHERE id=?", (status, local_id))
            con.commit()
            con.close()
            node_sync.local_event(f"order {local_id} -> {status}")
            print(f"[Node {NODE_ID}] order {local_id} -> {status}", flush=True)


# ----------------------------------------------------------------------
# HTTP API of this node (called remotely by the Order Service middleware)
# ----------------------------------------------------------------------
@app.get("/health")
def health():
    """Used by the middleware to check if this node is alive / busy."""
    load = rpc_get_node_load()
    return jsonify(node=NODE_ID, status="up",
                   active_orders=load["active_orders"], queue_length=load["queue_length"],
                   estimated_wait_time=load["estimated_wait_time"],
                   load_percentage=load["load_percentage"], busy_limit=BUSY_LIMIT)


@app.post("/rpc")
def rpc_endpoint():
    """XML-RPC transport endpoint used by the middleware's RPC client."""
    print(f"[RPC SERVER] Node {NODE_ID} received XML-RPC request", flush=True)
    response = rpc_dispatcher._marshaled_dispatch(request.get_data())
    return Response(response, content_type="text/xml")


@app.get("/menu")
def get_menu():
    return jsonify(rpc_get_menu())


@app.post("/orders")
def place_order():
    """Remote operation place_order() - normally called by the middleware.

    PEER-TO-PEER: if this node is busy it does NOT reject the order.
    It directly asks its peer node (canteen-to-canteen, no middleware)
    to take the order instead.
    """
    data = request.get_json()

    if active_order_count() >= BUSY_LIMIT:
        print(f"[Node {NODE_ID}] BUSY -> asking peer node directly (P2P): {PEER_URL}", flush=True)
        try:
            data["meta"] = node_sync.stamp_send("P2P order handover")     # FA-2: clocks on P2P msg
            r = requests.post(f"{PEER_URL}/peer/orders", json=data, timeout=3)
            resp = r.json()
            resp["note"] = f"Node {NODE_ID} was busy, handed over via P2P"
            return jsonify(resp)
        except requests.RequestException:
            print(f"[Node {NODE_ID}] peer unreachable, accepting order anyway", flush=True)

    order_id = save_order(data["student_name"], data["items"], data["total"],
                          note="", item_ids=data.get("item_ids"),
                          request_id=data.get("request_id"))
    local_id = int(order_id.split("-")[1])
    order_queue.put(local_id)                 # send message to kitchen worker
    print(f"[Node {NODE_ID}] accepted order {order_id}, message queued", flush=True)
    local = rpc_get_order_status(local_id)
    return jsonify(order_id=order_id, status="Pending", handled_by=f"Node {NODE_ID}",
                   estimated_wait_time=local["estimated_wait_time"],
                   request_id=local["request_id"])


@app.post("/peer/orders")
def peer_order():
    """PEER-TO-PEER endpoint: called directly by the OTHER canteen node."""
    data = request.get_json()
    node_sync.on_receive(data.get("meta"), "P2P order handover received")
    order_id = save_order(data["student_name"], data["items"], data["total"],
                          note="received via P2P from peer node",
                          item_ids=data.get("item_ids"),
                          request_id=data.get("request_id"))
    local_id = int(order_id.split("-")[1])
    order_queue.put(local_id)
    print(f"[Node {NODE_ID}] accepted P2P order {order_id} from peer", flush=True)
    return jsonify(order_id=order_id, status="Pending", handled_by=f"Node {NODE_ID}")


@app.get("/orders/<int:local_id>")
def get_order(local_id):
    """Remote operation get_order_status() - called by the middleware."""
    con = get_db()
    row = con.execute("SELECT * FROM orders WHERE id=?", (local_id,)).fetchone()
    con.close()
    if row is None:
        return jsonify(error="order not found"), 404
    d = dict(row)
    d["order_id"] = f"N{NODE_ID}-{local_id}"
    return jsonify(d)


@app.get("/orders")
def list_orders():
    """All orders on this node (handy for demonstration)."""
    con = get_db()
    rows = con.execute("SELECT * FROM orders ORDER BY id DESC").fetchall()
    con.close()
    return jsonify([dict(r) for r in rows])


# ----------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--peer", required=True, help="URL of the other node")
    parser.add_argument("--skew", type=float, default=None,
                        help="simulated clock error in seconds (FA-2 clock sync demo)")
    args = parser.parse_args()

    NODE_ID = args.id
    PEER_URL = args.peer
    DB_FILE = f"canteen_node_{NODE_ID}.db"

    init_db()

    # ---- FA-2: start Unit 3 (synchronisation) and Unit 4 (paradigms) services ----
    logging.getLogger("werkzeug").addFilter(_QuietLogs())     # hide periodic beacon/poll noise
    skew = DEFAULT_SKEW.get(NODE_ID, 0.0) if args.skew is None else args.skew
    node_sync.init(f"N{NODE_ID}", CLUSTER, MIDDLEWARE_URL, skew=skew)
    node_paradigms.init(get_db, f"N{NODE_ID}", f"dfs_data_n{NODE_ID}")

    # daemon=True -> the worker thread dies together with the process
    threading.Thread(target=kitchen_worker, daemon=True).start()

    print("\n" + "=" * 62, flush=True)
    print(f"  CANTEEN NODE {NODE_ID} - READY", flush=True)
    print("=" * 62, flush=True)
    print(f"  HTTP server       : http://localhost:{args.port}", flush=True)
    print(f"  XML-RPC server    : http://localhost:{args.port}/rpc", flush=True)
    print(f"  RPC role          : SERVER", flush=True)
    print(f"  Database          : {DB_FILE}", flush=True)
    print(f"  Message queue     : ACTIVE (kitchen worker started)", flush=True)
    print(f"  Peer node         : {PEER_URL}", flush=True)
    print("  RPC procedures    : PlaceOrder, GetMenu, GetOrderStatus", flush=True)
    print("                       GetNodeHealth, GetNodeLoad", flush=True)
    print("  Waiting for orders...", flush=True)
    print("=" * 62 + "\n", flush=True)
    app.run(port=args.port, threaded=True)
