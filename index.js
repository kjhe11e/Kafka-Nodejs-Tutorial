"use strict";

const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'tutorial_1',
    brokers: ['localhost:9092']
});

const producer = kafka.producer();
const consumer = kafka.consumer({ groupId: 'tutorial-group' })

const run = async () => {

    // Producing
    await producer.connect();
    await producer.send({
        topic: 'tutorial-topic',
        messages: [
            {
                value: 'Hola mundo!'
            }
        ]
    });

    // Consuming
    await consumer.connect();
    await consumer.subscribe({ 
        topic: 'tutorial-topic',
        fromBeginning: true
    });

    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log({
                partition,
                offset: message.offset,
                value: message.value.toString()
            })
        }
    });
}

run().catch(console.error);