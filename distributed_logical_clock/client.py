import grpc
import json
import sys
from customer import Customer
import banks_pb2
import banks_pb2_grpc
import time
from itertools import count
import threading


if len(sys.argv) < 2:
    print("Invalid")
    sys.exit(1)

json_file = sys.argv[1]
with open(json_file, "r") as f:
    data = json.load(f)

# Customers & Branchers
all_customers = [obj for obj in data if obj["type"] == "customer"]
all_branches = [obj for obj in data if obj["type"] == "branch"]
# Create stubs for all branches
branch_stubs = {}
for b in all_branches:
    port = 5000 + b["id"]
    channel = grpc.insecure_channel(f"localhost:{port}")
    branch_stubs[b["id"]] = banks_pb2_grpc.BankServiceStub(channel)



# Thread-safe container for results
responses = [None] * len(all_customers)

# Function to run a single customer
def run_customer(idx, customer_info):
    customer_id = customer_info["id"]
    customer_requests = customer_info["customer-requests"]

    branch_port = 5000 + customer_id

    customer = Customer(customer_id, customer_requests, branch_port, branch_stubs)
    res = customer.executeEvents()

    responses[idx] = {"id": customer_id, "type": "customer", "events": res}

# Start threads for all customers
threads = []
for i, customer_info in enumerate(all_customers):
    t = threading.Thread(target=run_customer, args=(i, customer_info))
    t.start()
    threads.append(t)

# Wait for all threads to finish
for t in threads:
    t.join()

# Write results
with open("output.json", "w") as out_file:
    json.dump(responses, out_file, indent=3)




    
# SHUTDOWN ALL BRANCHES
time.sleep(1)
for branch_info in all_branches:
    branch_id = branch_info["id"]
    port = 5000 + branch_id
    address = f"localhost:{port}"

    try:
        channel = grpc.insecure_channel(address)
        stub = banks_pb2_grpc.BranchServiceStub(channel)
        stub.Shutdown(banks_pb2.ShutdownRequest())
        print(f"Branch {branch_id} terminated")
    except Exception as e:
        print(f"Failed termination on branch {branch_id}: {e}")


# Handle file ouput project 2 part 3
with open("output.json", "r") as f:
    data = json.load(f)

all_events = []

for record in data:
    record_id = record.get("id")
    for event in record.get("events", []):
        e = event.copy()
        if record.get("type") == "customer":
            label = "customer"
        else:
            label = "branch"

        keys = list(e.keys())
        first_part = keys[:1]
        second_part = keys[1:]
        new_e = {"id": record_id}
        new_e.update({k: e[k] for k in first_part})
        new_e["type"] = label
        new_e.update({k: e[k] for k in second_part})
        all_events.append(new_e)

all_events_sorted = sorted(all_events, key=lambda x: (x['customer-request-id'], x['logical_clock']))

with open("output.json", "r") as f:
    data = json.load(f)
data.extend(all_events_sorted)
with open("output.json", "w") as f:
    json.dump(data, f, indent=3)