require('dotenv').config()
const express = require('express')
const app = express()
const appPromise = require('./config/configprovider').appPromise

const {Kafka} = require('kafkajs')
const MongoClient = require('mongodb').MongoClient

appPromise.then(function(app) {
    if (app !== null) {
        const kafka = new Kafka({
            clientId: 'transaction-client',
            brokers: [process.env.KAFKA_SERVER],
        })

        kafka_consumer().then()

        async function kafka_consumer() {
            const consumer = kafka.consumer({groupId: 'movement-subscription', allowAutoTopicCreation: true})
            await consumer.connect()
            await consumer.subscribe({topic: 'transaction-topic', fromBeginning: true})
            await consumer.run({
                autoCommit: false,
                eachMessage: async ({topic, partition, message}) => {
                    console.log({ value: message.value.toString() })
                    var jsonObj = JSON.parse(message.value.toString())
                    var amountNew = 0
                    if (jsonObj.type === 'withdrawal') {
                        amountNew = jsonObj.amount * (-1)
                    } else {
                        amountNew = jsonObj.amount
                    }
                    MongoClient.connect(process.env.DB_MONGO_URI, function (err, db) {
                        if (err)
                            throw err
                        db.db(process.env.DB_MONGO_DATABASE_MOVEMENT).collection('movement').insertOne(jsonObj, async function (err, result) {
                                if (err) {
                                    resolve(result)
                                    db.close()
                                    return console.error('Error executing query', err.stack)
                                }
                                console.log(`Account modified with accountId: ${jsonObj.accountId}`)
                                await consumer.commitOffsets([{ topic, partition, offset: (Number(message.offset) + 1).toString() }])
                                console.log(`Commit message with accountId: ${jsonObj.accountId}`)
                            }
                        )
                    })
                },
            })
        }
    }
});