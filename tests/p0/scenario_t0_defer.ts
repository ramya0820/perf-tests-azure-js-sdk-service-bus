/*
  T0 Scenario - Send and receive messages within single process.
*/

import { Namespace, SendableMessageInfo, OnMessage, OnError, delay } from "@azure/service-bus";

const connectionString = "Endpoint=sb://perftestbasic.servicebus.windows.net";
const queueName = "t0-queue-defer";

const testDurationInMilliseconds = 60000 * 5 * 12 * 3; // 3 hours

var messageMap: Set<number> = new Set<number>();
var msgId = 1;

var snapshotIntervalID: any;

var isJobDone = false;

async function main(): Promise<void> {
    snapshotIntervalID = setInterval(snapshot, 5000); // Every 5 seconds
    sendMessages();
    receiveMessages();
}

async function sendMessages() {
  const ns = Namespace.createFromConnectionString(connectionString);
  const client = ns.createQueueClient(queueName);
  try {

    const sender = client.getSender();

    while (!isJobDone) {
      const message: SendableMessageInfo = {
        messageId: msgId,
        body: "test",
        label: `${msgId}`
      };
      messageMap.add(msgId);
      msgId++;
      await sender.send(message);
      await delay(2000); // Throttling send to not increase queue size
    }
  } finally {
    client.close();
    ns.close();
  }
}

async function receiveMessages(): Promise<void> {
  const ns = Namespace.createFromConnectionString(connectionString);
  const client = ns.createQueueClient(queueName);
  
  try {
    const receiver = client.getReceiver();
    const onMessageHandler: OnMessage = async (brokeredMessage) => {
      var receivedMsgId = brokeredMessage.messageId;
      
      if(typeof(receivedMsgId) !== "number") {
        throw new Error("MessageId is corrupt or is of unexpected type")
      }
  
      if(!messageMap.has(receivedMsgId)) {
        throw new Error("Received message that is not recorded in internal map.")
      }
  
      messageMap.delete(receivedMsgId);
  
      await brokeredMessage.defer();
    };
    const onErrorHandler: OnError = (err) => {
      throw err;
    };
  
    receiver.receive(onMessageHandler, onErrorHandler, { autoComplete: false });
    await delay(testDurationInMilliseconds);
  
    isJobDone = true;
    
    await receiver.close();
    clearInterval(snapshotIntervalID);

  } finally {
    client.close();
    ns.close();
  }
}

function snapshot() {
  console.log("Time : ", new Date());
  console.log("Map Size : ", messageMap.size);
  console.log("Number of messages sent and deferred so far : ", msgId);
  console.log("\n");
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
