import grpc
import banks_pb2
import banks_pb2_grpc
import threading

class Customer:
    def __init__(self, id, events, branch_port, branch_stubs):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # pointer for the stub
        self.stub = self.createStub(branch_port)
        # customer personal clock
        self.clock = 0
        self.lock = threading.Lock()

    # Create Customer stub
    def createStub(self, branch_port):
        channel = grpc.insecure_channel(f'127.0.0.1:{branch_port}')
        return banks_pb2_grpc.BankServiceStub(channel)

    # Send out the events to the branch of the bank
    def executeEvents(self):
        results = []
        for event in self.events:
            customer_request_id = event.get("customer-request-id")
            interface = event.get("interface")
            money = event.get("money", 0)
            # customer personal clock
            with self.lock:
                self.clock += 1
                local_clock = self.clock

            comment = f"event_sent from customer {self.id}"

            try:
                if interface == "query":
                    res = self.stub.Query(
                        banks_pb2.QueryRequest(
                            customer_id=self.id
                        )
                    )
                elif interface == "deposit":
                    res = self.stub.Deposit(
                        banks_pb2.DepositRequest(
                            customer_id=self.id,
                            customer_request_id=customer_request_id,
                            amount=money,
                            timestamp=local_clock
                        )
                    )
                elif interface == "withdraw":
                    res = self.stub.Withdraw(
                        banks_pb2.WithdrawRequest(
                            customer_id=self.id,
                            customer_request_id=customer_request_id,
                            amount=money,
                            timestamp=local_clock
                        )
                    )
                else:
                    res = None

                # Update local clock based on Lamport timestamp from response
                results.append({
                    "customer-request-id": customer_request_id,
                    "logical_clock": local_clock,
                    "interface": interface,
                    "comment": comment
                })
            except grpc.RpcError as e:
                results.append({
                    "customer-request-id": customer_request_id,
                    "logical_clock": local_clock,
                    "interface": interface,
                    "error": str(e)
                })

        return results