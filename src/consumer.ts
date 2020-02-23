import fs = require('fs');

import { Kafka, logLevel } from 'kafkajs';

const host = '172.16.36.171'

// const kafka = new Kafka({
//   logLevel: logLevel.INFO,
//   brokers: [`${host}:31090`, `${host}:31091`, `${host}:31092`],
//   clientId: 'example-consumer'
// })
const kafka = new Kafka({
    logLevel: logLevel.INFO,
    brokers: [`${host}:31090`, `${host}:31091`, `${host}:31092`],
    clientId: 'example-consumer'
});


const topic = 'test-topic'
const consumer = kafka.consumer({ groupId: 'test-group-1' });

const run = async () => {
  await consumer.connect()
  await consumer.subscribe({ topic, fromBeginning: true })
  await consumer.run({
    // eachBatch: async ({ batch }) => {
    //   console.log(batch)
    // },
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[${partition} | ${message.offset}] / ${message.timestamp}`
      console.log(`- ${prefix} ${message.key}#${message.value}`)
    },
  })
}

run().catch(e => console.error(`[example/consumer] ${e.message}`, e))

process.on('unhandledRejection', async (e) => {
    try {
        console.log(`process.on unhandledRejection`)
        console.error(e)
        await consumer.disconnect()
        process.exit(0)
      } catch (_) {
        process.exit(1)
      }
} );

process.on('uncaughtException', async (e) => {
    try {
        console.log(`process.on uncaughtException`)
        console.error(e)
        await consumer.disconnect()
        process.exit(0)
      } catch (_) {
        process.exit(1)
      }
} );

process.on('SIGTERM', async (e) => {
    try {
        await consumer.disconnect()
      } finally {
        process.kill(process.pid, 'SIGTERM')
      }
} );
process.on('SIGINT', async (e) => {
    try {
        await consumer.disconnect()
      } finally {
        process.kill(process.pid, 'SIGINT')
      }
} );
process.on('SIGUSR2', async (e) => {
    try {
        await consumer.disconnect()
      } finally {
        process.kill(process.pid, 'SIGUSR2')
      }
} );