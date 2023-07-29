import { unpack } from 'msgpackr';
import { connect } from 'amqplib';
import { EventEmitter } from 'node:events';
import { Publisher } from './Publisher.js';
export class Consumer extends EventEmitter {
    constructor(rabbitURL = null, ns) {
        super();
        this.ns = ns;
        this.connection = null;
        this.channels = [];
        this.exs = [];
        this.rabbitURL = rabbitURL || process.env.RABBIT_URL;
        this.publisher = new Publisher(this.rabbitURL, ns);
    }

    async connect() {
        try {
            let events = this.eventNames();
            let nss = events.map(x => `${this.ns}:${x}`);
            this.connection = await connect(this.rabbitURL);
            for (let name of nss) {
                let options = {
                    durable: true,
                    deadLetterExchange: `${name}_DIRECT_EXCHANGE`,
                    deadLetterRoutingKey: `${name}_DIRECT_EXCHANGE`
                }
                
                let channel = await this.connection.createChannel();
                this.channels.push(channel);
                // -------------------------------------------------------------
                let DIEX = `${name}_DIRECT_EXCHANGE`;
                let DIQUE = `${name}_QUEUE`;
                let DEEX = `${name}_DELAY_EXCHANGE`;
                let DEQUE = `${name}_DELAY_QUEUE`;
                // -------------------------------------------------------------
                await channel.assertExchange(DIEX, 'direct', { durable: true });
                await channel.assertQueue(DIQUE, { durable: true, autoDelete: false });
                await channel.bindQueue(DIQUE, DIEX, DIEX);
                // -------------------------------------------------------------
                await channel.assertExchange(DEEX, 'direct', { durable: true });
                await channel.assertQueue(DEQUE, options);
                await channel.bindQueue(DEQUE, DEEX, DEEX);
            }

            let onmsg = (rawData, channel) => {
                let content = unpack(rawData.content);
                let { event, message } = content;
                let done = () => { channel.ack(rawData) }
                let reject = (prevAll = false, requeue = true) => {
                    channel.nack(rawData, prevAll, requeue);
                }

                this.emit(event, message, done, reject);
            }

            for (let i = 0; i < this.channels.length; i++) {
                let cb = (msg) => onmsg(msg, this.channels[i]);
                await this.channels[i].consume(`${nss[i]}_QUEUE`, cb, { noAck: false });
            }

            let exit = async () => {
                await Promise.all(this.channels.map(x => x?.close()));
                await this.connection?.close();
            }

            process.once('SIGINT', exit);
        } catch (error) {
            await Promise.all(this.channels.map(x => x?.close()));
            await this.connection?.close();
            throw error;
        }
    }
}