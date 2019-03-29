import { Namespace, SendableMessageInfo } from "@azure/service-bus";

const connectionString = "";

const testDurationInMilliseconds = 60000 * 5 * 12 * 24 * 7; // 1 week

const numOfClients = 10;

let msgId = 1;

var snapshotIntervalID: any;

var isJobDone = false;

async function main(): Promise<void> {
  snapshotIntervalID = setInterval(snapshot, 5000); // Every 5 seconds
  setTimeout(() => {
    isJobDone = true;
  }, testDurationInMilliseconds);

  sendReceiveMessages();
}

async function sendReceiveMessages() {
  const ns = Namespace.createFromConnectionString(connectionString);

  var clients = [];
  for (let i = 0; i < numOfClients; i++) {
    clients[i] = ns.createQueueClient(`t1-queue-new-${i + 1}`);
  }

  try {
    while (!isJobDone) {
      for (let i = 0; i < numOfClients; i++) {
        const sender = clients[i].getSender();
        const message: SendableMessageInfo = {
          messageId: msgId,
          body: "test",
          label: `${msgId}`
        };
        msgId++;
        await sender.send(message);
        const receiver = clients[i].getReceiver();
        const messagesReceived = await receiver.receiveBatch(1);
        messagesReceived[0].complete();
      }
    }
  } finally {
    ns.close();
    clearInterval(snapshotIntervalID);
  }
}

function snapshot() {
  console.log("Time : ", new Date());
  console.log("Number of clients opened, closed successfully so far : ", msgId);
  console.log("\n");
}

main().catch(err => {
  console.log("Error occurred: ", err);
});
