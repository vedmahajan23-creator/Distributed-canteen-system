@echo off
REM Starts the whole distributed system: 3 canteen nodes + 1 order service.
REM Each service runs in its OWN terminal window = its own OS process.
REM (FA-2 added Node 3: election, mutual exclusion and snapshots need >= 3 processes.)

start "Canteen Node 1 (port 5001)" cmd /k python canteen_node.py --id 1 --port 5001 --peer http://localhost:5002
start "Canteen Node 2 (port 5002)" cmd /k python canteen_node.py --id 2 --port 5002 --peer http://localhost:5001
start "Canteen Node 3 (port 5003)" cmd /k python canteen_node.py --id 3 --port 5003 --peer http://localhost:5001

REM small delay so nodes are up before the middleware starts
timeout /t 2 /nobreak >nul

start "Order Service - Middleware (port 5000)" cmd /k python order_service.py

echo All services started.
echo   Menu (FA-1)             : http://localhost:5000
echo   Unit 3 Synchronization  : http://localhost:5000/sync
echo   Unit 4 Paradigms        : http://localhost:5000/paradigms
