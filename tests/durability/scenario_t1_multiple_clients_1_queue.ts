import {
  Namespace,
  SendableMessageInfo
} from "@azure/service-bus";

const connectionString = "";
const queueName = "";

const testDurationInMilliseconds = 60000 * 5 * 12 * 24 * 7; // 1 week

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

  try {
    while (!isJobDone) {
      const client = ns.createQueueClient(queueName);
      const sender = client.getSender();

      const message: SendableMessageInfo = {
        messageId: msgId,
        body: "test",
        label: `${msgId}`
      };
      msgId++;
      await sender.send(message);
      await sender.close();

      const receiver = client.getReceiver();
      const messagesReceived = await receiver.receiveBatch(1);
      messagesReceived[0].complete();
      await receiver.close();

      await client.close();
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
