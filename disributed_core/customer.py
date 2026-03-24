import grpc
import banks_pb2
import banks_pb2_grpc

class Customer:
    def __init__(self, id, events, branch_port):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # pointer for the stub
        self.stub = self.createStub(branch_port)

    # Create Customer stub
    def createStub(self, branch_port):
        channel = grpc.insecure_channel(f'127.0.0.1:{branch_port}')
        return banks_pb2_grpc.BankServiceStub(channel)

    # Send out the events to the branch of the bank
    def executeEvents(self):
        responses = []
        for event in self.events:
            if event['interface'] == "query":
                res = self.stub.Query(banks_pb2.QueryRequest(customer_id=self.id))
                responses.append({"interface": "query", "balance": res.balance})
            elif event['interface'] == "deposit":
                res = self.stub.Deposit(
                    banks_pb2.DepositRequest(customer_id=self.id, amount=event['money'])
                )
                responses.append({"interface": "deposit", "result": res.result})
            elif event['interface'] == "withdraw":
                res = self.stub.Withdraw(
                    banks_pb2.WithdrawRequest(customer_id=self.id, amount=event['money'])
                )
                responses.append({"interface": "withdraw", "result": res.result})
        return responses