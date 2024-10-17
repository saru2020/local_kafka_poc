import { Kafka } from "kafkajs";
import { SchemaRegistry } from "@immocapital/data-platform-sdk";

const SCHEMA_REGISTRY = new SchemaRegistry();

const kafka = new Kafka({
  clientId: "property-management-resident-consumer",
  brokers: ["localhost:9092"],
});

async function main() {
  const consumer = kafka.consumer({
    groupId: "property-management-resident-group",
  });

  try {
    // Connecting to the broker
    await consumer.connect();
    console.log("Consumer connected to Kafka");

    // Subscribing to the topic
    await consumer.subscribe({
      topic: "property-management.fct.resident.0",
      fromBeginning: true,
    });
    console.log(
      "Consumer subscribed to topic: property-management.fct.resident.0"
    );

    // Start consuming messages
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        console.log("Received message:");
        console.log("Topic:", topic);
        console.log("Partition:", partition);
        console.log("Offset:", message.offset);
        console.log("Value:", message.value.toString());

        try {
          const resident = await SCHEMA_REGISTRY.decryptAndDeserializeEncodedMessage(
            message.value
          );
          //   const resident = JSON.parse(message.value.toString());
          console.log("Parsed Resident:", resident);
        } catch (error) {
          console.error("Error parsing message:", error);
        }
      },
    });
  } catch (error) {
    console.error("Error:", error);
  }
}

main().catch(console.error);
