import grpc
import json
import sys
from customer import Customer
import banks_pb2
import banks_pb2_grpc
import random


if len(sys.argv) < 2:
    sys.exit(1)

json_file = sys.argv[1]
with open(json_file, "r") as f:
    data = json.load(f)

# Customers & Branchers
all_customers = [obj for obj in data if obj["type"] == "customer"]
all_branches = [obj for obj in data if obj["type"] == "branch"]

responses = []
for customer_info in all_customers:
    customer_id = customer_info["id"]
    events = customer_info["events"]

    # have all customer transaction go to a random branch for proof of concept
    branch_info = random.choice(all_branches)
    branch_port = 5000 + branch_info["id"]

    customer = Customer(customer_id, events, branch_port)

    res = customer.executeEvents()
    responses.append({"customer_id": customer_id, "events": res})

with open("output.json", "w") as out_file:
    json.dump(responses, out_file)







# SHUTDOWN ALL BRANCHES
for branch_info in all_branches:
    branch_id = branch_info["id"]
    port = 5000 + branch_id
    address = f"127.0.0.1:{port}"

    try:
        channel = grpc.insecure_channel(address)
        stub = banks_pb2_grpc.BranchServiceStub(channel)
        stub.Shutdown(banks_pb2.ShutdownRequest())
        print(f"Branch {branch_id} terminated")
    except Exception as e:
        print(f"Failed termination on branch {branch_id}: {e}")
