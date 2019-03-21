import { Namespace, SendableMessageInfo, OnMessage, OnError, delay } from "@azure/service-bus";

const connectionString = "Endpoint=sb://perfteststandard.servicebus.windows.net";
const queueName = "t0-queue-random";

const testDurationInMilliseconds = 60000 * 5 * 12 * 48; // 48 hours

var messagesToProcess : Set<number> = new Set<number>();
var messageAbandonedMap : { [key:number]:number; } = {};

var abandonCount = 0, completeCount = 0, deadletterCount = 0, deferCount = 0;

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
        label: `${msgId}`,
        sessionId: "session-1"
      };
      messageAbandonedMap[msgId] = 0;
      messagesToProcess.add(msgId);
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
    const receiver = await client.getSessionReceiver({sessionId: "session-1"});
    const onMessageHandler: OnMessage = async (brokeredMessage) => {
      var receivedMsgId = brokeredMessage.messageId;
      
      if(typeof(receivedMsgId) !== "number") {
        throw new Error("MessageId is corrupt or is of unexpected type")
      }
  
      const seed = Math.floor((Math.random() * 10) % 4);

      switch (seed) {
          case 0: { 
                    const currCount = messageAbandonedMap[receivedMsgId];
                    if(currCount === 10) {
                        abandonCount++;
                        if(messagesToProcess.has(receivedMsgId)) {
                            messagesToProcess.delete(receivedMsgId);
                        } 
                    }
                    messageAbandonedMap[receivedMsgId] = currCount + 1; 
                    brokeredMessage.abandon(); 
                    break; 
                }
          case 1: { 
                    completeCount++;
                    if(messagesToProcess.has(receivedMsgId)) {
                        messagesToProcess.delete(receivedMsgId);
                    } 
                    brokeredMessage.complete();
                    break; 
                }
          case 2: { 
                    deadletterCount++;
                    if(messagesToProcess.has(receivedMsgId)) {
                        messagesToProcess.delete(receivedMsgId);
                    }
                    brokeredMessage.deadLetter(); 
                    break; 
                }
          case 3: { 
                    deferCount++;
                    if(messagesToProcess.has(receivedMsgId)) {
                        messagesToProcess.delete(receivedMsgId);
                    }
                    brokeredMessage.defer(); 
                    break; 
        }
          default: { throw new Error("Unexpected seed"); }
      }

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
  console.log("Number of messages not processed yet : ", messagesToProcess.size);
  console.log("Number of messages sent so far : ", msgId);
  console.log("Number of messages abandoned : ", abandonCount);
  console.log("Number of messages completed : ", completeCount);
  console.log("Number of messages deadlettered : ", deadletterCount);
  console.log("Number of messages deferred : ", deferCount);
  console.log("\n");
}

main().catch((err) => {
  console.log("Error occurred: ", err);
});
