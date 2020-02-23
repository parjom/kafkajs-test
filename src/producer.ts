
import { Kafka, CompressionTypes, logLevel, Message } from 'kafkajs';

const host = '172.16.36.171';

const kafka = new Kafka({
  logLevel: logLevel.INFO,
  brokers: [`${host}:31090`, `${host}:31091`, `${host}:31092`],
  clientId: 'example-producer',
});

const topic = 'test-topic';
const producer = kafka.producer();

function getRandomNumber() {
    return Math.round(Math.random() * 1000);
}

function createMessage(num:any): Message
{ 
    let msg:Message = {
        key: `key-${num}`,
        value: `value-${num}-${new Date().toISOString()}`,
      };
    console.log('msg :', msg );
    return msg;
}

const sendMessage = () => {
  return producer
    .send({
      topic,
      compression: CompressionTypes.GZIP,
      messages: [ createMessage(getRandomNumber()) ]
    })
    .then(console.log)
    .catch(e => console.error(`[example/producer] ${e.message}`, e))
}

const run = async () => {
  await producer.connect()
  setInterval(sendMessage, 3000)
}

run().catch(e => console.error(`[example/producer] ${e.message}`, e));


process.on('unhandledRejection', async (e) => {
    try {
        console.log(`process.on unhandledRejection`)
        console.error(e)
        await producer.disconnect()
        process.exit(0)
      } catch (_) {
        process.exit(1)
      }
} );

process.on('uncaughtException', async (e) => {
    try {
        console.log(`process.on uncaughtException`)
        console.error(e)
        await producer.disconnect()
        process.exit(0)
      } catch (_) {
        process.exit(1)
      }
} );

process.on('SIGTERM', async (e) => {
    try {
        await producer.disconnect()
      } finally {
        process.kill(process.pid, 'SIGTERM')
      }
} );
process.on('SIGINT', async (e) => {
    try {
        await producer.disconnect()
      } finally {
        process.kill(process.pid, 'SIGINT')
      }
} );
process.on('SIGUSR2', async (e) => {
    try {
        await producer.disconnect()
      } finally {
        process.kill(process.pid, 'SIGUSR2')
      }
} );