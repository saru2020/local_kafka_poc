import { EventProducer } from '@immocapital/data-platform-sdk'
import { readFileSync } from 'fs';
import { join } from 'path';
import { fileURLToPath } from 'url';

async function main() {
  try {
    // Initialize EventProducer
    const producer = new EventProducer('property-management-resident-producer', {
        debug: true,
        isLocal: true
      });
  
    // await producer.connect();

    // Example message
    // Get the directory name of the current module
    const __dirname = fileURLToPath(new URL('.', import.meta.url));
    // Read the content of producer_event.json
    const rawData = readFileSync(join(__dirname, 'producer_event.json'), 'utf8');
    const message = JSON.parse(rawData);
    console.log('message to produce: ', message);    

    // Example topic and schema name (you may need to adjust these based on your actual setup)
    const topic = 'property-management.fct.resident.0';
    const schemaName = 'saru-property-management-resident';

    // Send a message
    await producer.send(topic, message , schemaName);
    console.log('Message sent successfully');

    // Disconnect producer
    // await producer.disconnect();

    // Fetch schema definition (for demonstration)
    // const schemaDefinition = await schemaRegistry.getSchemaDefinition(schemaName);
    // console.log('Schema Definition:', schemaDefinition);

  } catch (error) {
    console.error('Error:', error);
  }
}

main();