import grpc
import threading
import banks_pb2
import banks_pb2_grpc
import json
import os

class Branch(banks_pb2_grpc.BankService, banks_pb2_grpc.BranchService):
    def __init__(self, id, balance, branches, shutdown_event):
        # unique ID of the Branch
        self.id = id
        # replica of the Branch's balance
        self.balance = balance
        # the list of process IDs of the branches
        self.branches = branches
        # the list of Client stubs to communicate with the branches
        self.stubList = []
        # the list of branch ids for event logging purposes
        self.stubIdMap = {}
        # ensure no race condition is encounter just in case
        self.lock = threading.Lock()
        # Event used to signal the server to shut down
        self.shutdown_event = shutdown_event
        # Initialize gRPC stubs for ipc (except self)
        for b in self.branches:
            if b["id"] != self.id:
                port = 5000 + b["id"]
                channel = grpc.insecure_channel(f"127.0.0.1:{port}")
                stub = banks_pb2_grpc.BranchServiceStub(channel)
                self.stubList.append(stub)
                self.stubIdMap[stub] = b["id"]
        # branch personal clock
        self.clock = 0
        # event chain for logging
        self.event_chain = []

    # Shutdown RPC for branch termination
    def Shutdown(self, request, context):
        out_file = "output.json"
        if os.path.exists(out_file):
            with open(out_file, "r") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = []
        else:
            data = []
        branch_record = {
            "id": self.id,
            "type": "branch",
            "events": []
        }
        with self.lock:
            for event in self.event_chain:
                branch_record["events"].append(event.copy())
        
        data.append(branch_record)
        with open(out_file, "w") as out_file:
            json.dump(data, out_file, indent=3)

        self.shutdown_event.set()
        return banks_pb2.ShutdownResponse(result="success")

    # Customer <-> Branch RPCs
    def Query(self, request, context):
        with self.lock:
            return banks_pb2.QueryResponse(balance=self.balance)

    def Deposit(self, request, context):
        self.clock = max(self.clock, request.timestamp) + 1
        with self.lock:
            self.balance += request.amount
            event = {
                "customer-request-id": request.customer_request_id,
                "logical_clock": self.clock,
                "interface": "deposit",
                "comment": f"event_recv from customer {request.customer_id}",
            }
            self.event_chain.append(event)
        # Propagate to other branches
        for stub in self.stubList:
            self.clock += 1
            branch_id = self.stubIdMap[stub]
            event = {
                "customer-request-id": request.customer_request_id,
                "logical_clock": self.clock,
                "interface": "propogate_deposit",
                "comment": f"event_sent to branch {branch_id}",
            }
            self.event_chain.append(event)
            try:
                stub.Propagate_Deposit(
                    banks_pb2.Propagate_DepositRequest(
                        amount=request.amount,
                        timestamp=self.clock,
                        customer_request_id=request.customer_request_id,
                        source_branch_id=self.id
                    )
                )
            except grpc.RpcError as e:
                print(f"[Branch {self.id}] Failed to propogate deposit: {e}")

        return banks_pb2.OperationResponse(result="success", timestamp=self.clock)

    def Withdraw(self, request, context):
        self.clock = max(self.clock, request.timestamp) + 1
        with self.lock:
            if self.balance >= request.amount:
                self.balance -= request.amount
                event = {
                    "customer-request-id": request.customer_request_id,
                    "logical_clock": self.clock,
                    "interface": "withdraw",
                    "comment": f"event_recv from customer {request.customer_id}",
                }
                self.event_chain.append(event)
            else:
                return banks_pb2.OperationResponse(result="fail")
        # Propagate to other branches
        for stub in self.stubList:
            try:
                self.clock += 1
                branch_id = self.stubIdMap[stub]
                event = {
                    "customer-request-id": request.customer_request_id,
                    "logical_clock": self.clock,
                    "interface": "propogate_withdraw",
                    "comment": f"event_sent to branch {branch_id}",
                }
                self.event_chain.append(event)
                stub.Propagate_Withdraw(
                    banks_pb2.Propagate_WithdrawRequest(
                        amount=request.amount,
                        timestamp=self.clock,
                        customer_request_id=request.customer_request_id,
                        source_branch_id=self.id
                    )
                )
            except grpc.RpcError as e:
                print(f"[Branch {self.id}] Failed to propagate withdraw: {e}")

        return banks_pb2.OperationResponse(result="success", timestamp=self.clock)

    # Branch <-> Branch RPCs
    def Propagate_Deposit(self, request, context):
        self.clock = max(self.clock, request.timestamp) + 1
        with self.lock:
            self.balance += request.amount
            event = {
                "customer-request-id": request.customer_request_id,
                "logical_clock": self.clock,
                "interface": "propogate_deposit",
                "comment": f"event_recv from branch {request.source_branch_id}"
            }
            self.event_chain.append(event)
        return banks_pb2.OperationResponse(result="success", timestamp=self.clock)

    def Propagate_Withdraw(self, request, context):
        self.clock = max(self.clock, request.timestamp) + 1
        with self.lock:
            if self.balance >= request.amount:
                self.balance -= request.amount
                event = {
                    "customer-request-id": request.customer_request_id,
                    "logical_clock": self.clock,
                    "interface": "propogate_withdraw",
                    "comment": f"event_recv from branch {request.source_branch_id}"
                }
                self.event_chain.append(event)
                return banks_pb2.OperationResponse(result="success", timestamp=self.clock)
            else:
                return banks_pb2.OperationResponse(result="fail", timestamp=self.clock)


    # Message handler
    def MsgDelivery(self, request, context):
        if isinstance(request, banks_pb2.QueryRequest):
            return self.Query(request, context)
        elif isinstance(request, banks_pb2.DepositRequest):
            return self.Deposit(request, context)
        elif isinstance(request, banks_pb2.WithdrawRequest):
            return self.Withdraw(request, context)
        elif isinstance(request, banks_pb2.Propagate_DepositRequest):
            return self.Propagate_Deposit(request, context)
        elif isinstance(request, banks_pb2.Propagate_WithdrawRequest):
            return self.Propagate_Withdraw(request, context)
        else:
            return banks_pb2.OperationResponse(result="fail")
