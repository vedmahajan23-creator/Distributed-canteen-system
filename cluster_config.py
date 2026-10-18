"""
Shared cluster configuration (read by the middleware AND by every canteen node).

FA-2 adds a THIRD canteen node (N3) because several Unit-3 algorithms
(election, mutual exclusion, global snapshot) are only interesting with >= 3
processes.  If you only want two nodes, delete the "N3" line - everything
else adapts automatically (a missing node is simply treated as DOWN).
"""

CLUSTER = {
    "N1": "http://localhost:5001",
    "N2": "http://localhost:5002",
    "N3": "http://localhost:5003",
}

MIDDLEWARE_URL = "http://localhost:5000"

# Every node starts with a deliberately WRONG physical clock (seconds).
# This simulates hardware clock drift so that clock synchronisation
# (Cristian / Berkeley) has something to fix.  Override with  --skew .
DEFAULT_SKEW = {1: 4.0, 2: -6.5, 3: 11.0}
