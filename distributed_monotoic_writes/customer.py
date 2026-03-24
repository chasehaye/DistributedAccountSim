import grpc
import banks_pb2
import banks_pb2_grpc

class Customer:
    def __init__(self, id, events, branches):
        # unique ID of the Customer
        self.id = id
        # events from the input
        self.events = events
        # a list of received messages used for debugging purpose
        self.recvMsg = list()
        # dict for stubs for each branch
        self.stubs = self.createStubs(branches)

        # write set cache for read your writes
        self.writeset = []

    # Create Customer stubs
    def createStubs(self, branches):
        stubs = {}
        for b in branches:
            branch_id = b['id']
            port = 5000 + branch_id
            channel = grpc.insecure_channel(f'127.0.0.1:{port}')
            stubs[branch_id] = banks_pb2_grpc.BankServiceStub(channel)
        return stubs

    # Send out the events to the branch of the bank
    def executeEvents(self):
        responses = []
        for event in self.events:
            branch_id = event['branch']
            stub = self.stubs[branch_id]
            # event id for the server side write event creation for set
            event_id = event['id']
            if event['interface'] == "query":
                # Include current writeset for RYW
                res = stub.Query(banks_pb2.QueryRequest(customer_id=self.id, writeset=self.writeset ))
                result = res.balance
                responses.append({"id": event_id, "balance": result})
            elif event['interface'] == "deposit":
                # Include event_id for write tracking
                res = stub.Deposit(
                    banks_pb2.DepositRequest(customer_id=self.id, amount=event['money'], event_id=event_id, writeset=self.writeset )
                )
                # Append the write_id only if the operation was successful
                self.writeset.append(res.write_id)
            elif event['interface'] == "withdraw":
                # Include event_id for write tracking
                res = stub.Withdraw(
                    banks_pb2.WithdrawRequest(customer_id=self.id, amount=event['money'], event_id=event_id, writeset=self.writeset )
                )
                # Append the write_id only if the operation was successful
                self.writeset.append(res.write_id)



        return responses