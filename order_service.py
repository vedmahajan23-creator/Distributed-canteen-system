"""Middleware for the distributed canteen system.

The browser uses HTTP to reach this service.  Node communication uses the
standard-library XML-RPC protocol, so the middleware is a real RPC client and
each canteen node is an RPC server.
"""

import json
import time
import uuid
import xmlrpc.client
from collections import deque
from http.client import HTTPConnection

import requests
from flask import Flask, Response, jsonify, render_template, request

from cluster_config import CLUSTER
from fa2 import mw_common, mw_unit3, mw_unit4
from fa2.serverless import faas

app = Flask(__name__)
NODES = dict(CLUSTER)             # FA-2: node list now comes from cluster_config.py (N1, N2, N3)
rpc_activity = deque(maxlen=30)
notified = set()                  # (order_id, status) pairs already sent to serverless functions


class TimeoutTransport(xmlrpc.client.Transport):
    """Keep a failed node from blocking the middleware indefinitely."""

    def __init__(self, timeout=3):
        super().__init__()
        self.timeout = timeout

    def make_connection(self, host):
        connection = super().make_connection(host)
        if isinstance(connection, HTTPConnection):
            connection.timeout = self.timeout
        return connection


def call_rpc(node_name, method, payload=None):
    """Call one remote procedure and record enough detail for the dashboard."""
    request_id = payload.get("request_id") if isinstance(payload, dict) else None
    request_id = request_id or f"RPC-{uuid.uuid4().hex[:8].upper()}"
    started = time.perf_counter()
    entry = {"request_id": request_id, "method": f"{method}()", "node": node_name}
    try:
        proxy = xmlrpc.client.ServerProxy(
            f"{NODES[node_name]}/rpc", allow_none=True,
            transport=TimeoutTransport(timeout=3),
        )
        result = getattr(proxy, method)(payload) if payload is not None else getattr(proxy, method)()
        entry.update(status="SUCCESS",
                     response_time=round((time.perf_counter() - started) * 1000))
        rpc_activity.appendleft(entry)
        return result
    except (OSError, xmlrpc.client.Fault, xmlrpc.client.ProtocolError) as exc:
        entry.update(status="FAILED",
                     response_time=round((time.perf_counter() - started) * 1000),
                     error=str(exc))
        rpc_activity.appendleft(entry)
        raise requests.RequestException(str(exc)) from exc


# ---- FA-2 plug-ins (Unit 3 + Unit 4 pages and APIs) -------------------------
mw_common.init(NODES, call_rpc)
app.register_blueprint(mw_unit3.bp)       # /sync , /api/sync/... , /api/time
app.register_blueprint(mw_unit4.bp)       # /paradigms , /api/objects , /api/dfs , /api/fn ...


def get_node_snapshot(item_ids=None):
    snapshots = []
    for name in NODES:
        try:
            health = call_rpc(name, "GetNodeHealth")
            load = call_rpc(name, "GetNodeLoad", {"item_ids": item_ids or []})
            load["response_time"] = health["response_time"]
            load["name"] = name
            load["url"] = NODES[name]
            load["up"] = True
            load["load_score"] = (
                load["active_orders"] * 3
                + load["queue_length"] * 2
                + load["estimated_wait_for_new_order"]
            )
        except requests.RequestException:
            snapshots.append({"name": name, "url": NODES[name], "up": False,
                              "status": "DOWN"})
            continue
        snapshots.append(load)
    return snapshots


def select_node(item_ids):
    snapshots = get_node_snapshot(item_ids)
    healthy = [node for node in snapshots if node["up"]]
    if not healthy:
        return None, snapshots
    selected = min(healthy, key=lambda node: (node["load_score"], node["name"]))
    return selected, snapshots


@app.get("/")
def page_menu():
    return render_template("index.html")


@app.get("/track")
def page_track():
    return render_template("track.html")


@app.get("/nodes")
def page_nodes():
    return render_template("nodes.html")


@app.get("/webrtc")
def page_webrtc():
    return render_template("webrtc.html")


@app.get("/api/menu")
def api_menu():
    for name in NODES:
        try:
            return jsonify(menu=call_rpc(name, "GetMenu"), served_by=name)
        except requests.RequestException:
            continue
    return jsonify(error="All canteen nodes are down"), 503


@app.post("/rpc/place_order")
def rpc_place_order():
    data = request.get_json(silent=True) or {}
    if not data.get("student_name") or not data.get("items"):
        return jsonify(error="student_name and items are required"), 400
    selected, snapshots = select_node(data.get("item_ids", []))
    if selected is None:
        return jsonify(error="All canteen nodes are down", nodes=snapshots), 503
    request_id = f"RPC-{uuid.uuid4().hex[:8].upper()}"
    data["request_id"] = request_id
    data["lamport"] = mw_unit3.tick()          # FA-2: Lamport clock piggybacked on the RPC
    print(
    f"[RPC CLIENT] Selected {selected['name']} | "
    f"load score={selected['load_score']} | "
    f"estimated wait={selected['estimated_wait_for_new_order']} min | "
    f"calling PlaceOrder() request_id={request_id}",
    flush=True,
    )
    try:
        result = call_rpc(selected["name"], "PlaceOrder", data)
    except requests.RequestException:
        # A node may fail between the load query and the call; retry on the
        # next best healthy node without making the browser know about it.
        alternatives = [n for n in snapshots if n.get("up") and n["name"] != selected["name"]]
        if not alternatives:
            return jsonify(error=f"{selected['name']} failed while placing the order"), 502
        fallback = min(alternatives, key=lambda node: (node["load_score"], node["name"]))
        result = call_rpc(fallback["name"], "PlaceOrder", data)
        selected = fallback
    mw_unit3.observe(result.get("lamport_ts"))          # FA-2: merge the node's timestamp
    # FA-2 (serverless): an EVENT triggers functions (receipt -> DFS) without blocking the student
    faas.emit("order.placed", {"order_id": result["order_id"], "student_name": data["student_name"],
                               "items": data["items"], "total": data.get("total", 0),
                               "handled_by": result.get("handled_by"),
                               "lamport_ts": result.get("lamport_ts")})
    result["allocation"] = {
        "selected_node": selected["name"],
        "reason": "Lower workload and shorter estimated waiting time.",
        "nodes": snapshots,
    }
    return jsonify(result)


@app.get("/rpc/get_order_status/<order_id>")
def rpc_get_order_status(order_id):
    try:
        node_name, local_id = order_id.split("-", 1)
        result = call_rpc(node_name, "GetOrderStatus", int(local_id))
        return jsonify(result)
    except (ValueError, KeyError):
        return jsonify(error="invalid order id"), 400
    except requests.RequestException:
        return jsonify(error=f"{node_name} is not reachable"), 502


@app.get("/api/nodes/status")
def api_nodes_status():
    return jsonify(get_node_snapshot())


@app.get("/api/rpc/activity")
def api_rpc_activity():
    return jsonify(list(rpc_activity))


@app.get("/stream/order/<order_id>")
def stream_order(order_id):
    def event_stream():
        last_status = None
        while True:
            try:
                node_name, local_id = order_id.split("-", 1)
                order = call_rpc(node_name, "GetOrderStatus", int(local_id))
            except (ValueError, requests.RequestException):
                yield f"data: {json.dumps({'error': 'node unreachable'})}\n\n"
                return
            if order.get("status") != last_status:
                last_status = order.get("status")
                yield f"data: {json.dumps(order)}\n\n"
                if last_status == "Ready" and (order_id, "Ready") not in notified:
                    notified.add((order_id, "Ready"))       # FA-2: event -> serverless function
                    faas.emit("order.ready", {"order_id": order_id,
                                              "student_name": order.get("student_name", "")})
            if last_status == "Completed":
                return
            time.sleep(2)
    return Response(event_stream(), mimetype="text/event-stream")


if __name__ == "__main__":
    print("\n" + "=" * 62, flush=True)
    print("  ORDER SERVICE - MIDDLEWARE READY", flush=True)
    print("=" * 62, flush=True)
    print("  HTTP server       : http://localhost:5000", flush=True)
    print("  RPC role          : CLIENT", flush=True)
    print("  Connected nodes   : " + ", ".join(f"{n} ({u.rsplit(':', 1)[1]})" for n, u in NODES.items()), flush=True)
    print("  FA-2 pages        : http://localhost:5000/sync  (Unit 3)   /paradigms  (Unit 4)", flush=True)
    print("  Routing           : intelligent load-based allocation", flush=True)
    print("  Live tracking     : SSE /stream/order/<order_id>", flush=True)
    print("  Waiting for student orders...", flush=True)
    print("=" * 62 + "\n", flush=True)
    app.run(port=5000, threaded=True)
