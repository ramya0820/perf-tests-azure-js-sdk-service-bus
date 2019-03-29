import { Namespace, SendableMessageInfo, OnMessage, OnError, delay } from "@azure/service-bus";

const connectionString = "";
const queueName = "";

const testDurationInMilliseconds = 60000 * 5 * 12 * 3; // 3 hours

var messageReceiveCountMap : { [key:string]:number; } = {};
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
      messageReceiveCountMap[msgId] = 0;
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
  
      if(!messageReceiveCountMap[receivedMsgId] === undefined) {
        throw new Error("Received message that is not recorded in internal map.")
      }
  
      const currCount = messageReceiveCountMap[receivedMsgId];
      messageReceiveCountMap[receivedMsgId] = currCount + 1;
  
      await brokeredMessage.abandon();
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
  console.log("Status of last 5 messages : ");
  if (msgId >= 5) {
    var i = 0;
    while(i < 5) {
      console.log(`MsgId: ${msgId - i} and it's ReceiveCount: ${messageReceiveCountMap[msgId - i]}`);
      i++;
    }
  }
  console.log("\n");
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
