import grpc
import threading
import banks_pb2
import banks_pb2_grpc
import time

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
        # ensure no race condition is encounter just in case
        self.lock = threading.Lock()
        # Event used to signal the server to shut down
        self.shutdown_event = shutdown_event
        # Initialize stubs for ipc for each branch (except self)
        for b in self.branches:
            if b["id"] != self.id:
                port = 5000 + b["id"]
                channel = grpc.insecure_channel(f"127.0.0.1:{port}")
                stub = banks_pb2_grpc.BranchServiceStub(channel)
                self.stubList.append(stub)

        # list of write event IDs that have been applied at this given branch
        self.writeset = []


    # Shutdown RPC for branch termination
    def Shutdown(self, request, context):
        self.shutdown_event.set()
        return banks_pb2.ShutdownResponse(result="success")

    # Customer <-> Branch RPCs
    def Query(self, request, context):
        incoming_writeset = request.writeset
        while True:
            with self.lock:
                if all(write_id in self.writeset for write_id in incoming_writeset):
                    break
            time.sleep(0.01)
        with self.lock:
            return banks_pb2.QueryResponse(balance=self.balance)

    def Deposit(self, request, context):
        write_id = request.event_id
        incoming_writeset = request.writeset
        while True:
            with self.lock:
                if all(w in self.writeset for w in incoming_writeset):
                    break
            time.sleep(0.01)

        with self.lock:
            self.balance += request.amount
            # Record the write event ID
            self.writeset.append(write_id)
        # Propagate to other branches
        for stub in self.stubList:
            try:
                # Propagate the event ID to other branches
                stub.Propagate_Deposit(
                    banks_pb2.Propagate_DepositRequest(amount=request.amount, write_id=write_id)
                )
            except grpc.RpcError as e:
                print(f"[Branch {self.id}] Failed to propagate deposit: {e}")

        return banks_pb2.OperationResponse(result="success", write_id=write_id)

    def Withdraw(self, request, context):
        write_id = request.event_id
        incoming_writeset = request.writeset
        while True:
            with self.lock:
                if all(w in self.writeset for w in incoming_writeset):
                    break
            time.sleep(0.01)

        with self.lock:
            if self.balance >= request.amount:
                self.balance -= request.amount
                # Record the write event ID
                self.writeset.append(write_id)
            else:
                return banks_pb2.OperationResponse(result="fail")
        # Propagate to other branches
        for stub in self.stubList:
            try:
                # Propagate the event ID to other branches
                stub.Propagate_Withdraw(
                    banks_pb2.Propagate_WithdrawRequest(amount=request.amount, write_id=write_id)
                )
            except grpc.RpcError as e:
                print(f"[Branch {self.id}] Failed to propagate withdraw: {e}")

        return banks_pb2.OperationResponse(result="success", write_id=write_id)

    # Branch <-> Branch RPCs
    def Propagate_Deposit(self, request, context):
        with self.lock:
            event_id = request.write_id
            self.balance += request.amount
            # Record the write event ID
            if event_id not in self.writeset:
                self.writeset.append(event_id)  
        return banks_pb2.OperationResponse(result="success")

    def Propagate_Withdraw(self, request, context):
        with self.lock:
            event_id = request.write_id
            if self.balance >= request.amount:
                self.balance -= request.amount
                # Record the write event ID
                if event_id not in self.writeset:
                    self.writeset.append(event_id)  
                return banks_pb2.OperationResponse(result="success")
            else:
                return banks_pb2.OperationResponse(result="fail")


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
