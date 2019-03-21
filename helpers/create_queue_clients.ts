import ServiceBus from "azure-sb";

var connectionString = "Endpoint=sb://perftestbasic.servicebus.windows.net";

var index = 1;

while (index <= 5000) {

    const queueName = `t1-queue-new-${index}`;
    var sbService = ServiceBus.createServiceBusService(connectionString);
    sbService.createQueueIfNotExists(queueName, (err) => {
        if (err) {
            console.log('Failed to create queue: ', err);
        } else {
            console.log('Queue created - ', queueName);
        }
    });

    index++;
}