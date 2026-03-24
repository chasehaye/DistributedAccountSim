import grpc
import threading
from concurrent import futures
import json
import sys
import banks_pb2_grpc
from branch import Branch


def start_branch_server(branch_info, all_branches):
    branch_id = branch_info["id"]
    balance = branch_info["balance"]

    # Create shutdown to later trigger thread termination
    shutdown_event = threading.Event()

    # Create Branch service instance
    branch_service = Branch(branch_id, balance, all_branches, shutdown_event)

    # Create gRPC server with multithreading
    server = grpc.server(futures.ThreadPoolExecutor())
    banks_pb2_grpc.add_BankServiceServicer_to_server(branch_service, server)
    banks_pb2_grpc.add_BranchServiceServicer_to_server(branch_service, server)

    # Assign a unique port to the branch add it to the server
    port = 5000 + branch_id
    address = f"127.0.0.1:{port}"
    server.add_insecure_port(address)
    print(f"[Server]-Branch {branch_id} started at port: {port}")
    server.start()

    # Listen for shutdown signal
    while not shutdown_event.is_set():
        shutdown_event.wait(timeout=1)
    server.stop(0)
    print(f"[Server]-Branch {branch_id} shut down")

# Scipt start 
if len(sys.argv) < 2:
    print("Invalid format")
    sys.exit(1)

# Parse JSON file
json_file = sys.argv[1]
with open(json_file, "r") as f:
    data = json.load(f)
all_branches = [obj for obj in data if obj["type"] == "branch"]
# Start servers
threads = []
for branch_info in all_branches:
    t = threading.Thread(target=start_branch_server, args=(branch_info, all_branches))
    t.start()
    threads.append(t)

for t in threads:
    t.join()
