import { Namespace, SendableMessageInfo, OnMessage, OnError, delay, QueueClient } from "@azure/service-bus";

const connectionString = "";

const numberOfClients = 5000;

var clients: QueueClient[] = [];

var snapshotIntervalID: any;

async function main(): Promise<void> {
    snapshotIntervalID = setInterval(snapshot, 5000); // Every 5 seconds
    createClients();
}

async function createClients() {
  const ns = Namespace.createFromConnectionString(connectionString);
 
  try {

    var num = 1;
    while (num < numberOfClients) {
        const client = ns.createQueueClient(`t1-queue-new-${num}`);
        clients.push(client);
        const sender = client.getSender();
        
        const message: SendableMessageInfo = {
            messageId: `client - ${num}`,
            body: "test",
            label: `client# ${num}`
        };
        await sender.send(message);

        const receiver = client.getReceiver();
        const onMessageHandler: OnMessage = async (brokeredMessage) => {
            await brokeredMessage.complete();
          };
          const onErrorHandler: OnError = (err) => {
            throw err;
          };
        receiver.receive(onMessageHandler, onErrorHandler, { autoComplete: false });
        await delay(2000);
        await receiver.close();
        num++;
    }
  } finally {
    var index = 0;
    while (index < clients.length) {
        await clients[index].close();
        index++;
    }
    clearInterval(snapshotIntervalID);
    ns.close();
  }
}

function snapshot() {
  console.log("Time : ", new Date());
  console.log("Number of clients created so far : ", clients.length);
  console.log("\n");
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
