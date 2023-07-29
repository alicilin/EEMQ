import { differenceInMilliseconds } from 'date-fns';
import { pack } from 'msgpackr';
import { connect } from 'amqplib';
export class Publisher {
    constructor(rabbitURL, ns) {
        this.ns = ns;
        this.connection = null;
        this.rabbitURL = rabbitURL || process.env.RABBIT_URL;
    }

    async publish({ event, message, id = null, delay = null }) {
        try {
            this.connection = await connect(this.rabbitURL);
            let channel = await this.connection.createChannel();
            let packx = { id, ns: this.ns, event, message };
            let exchange = `${this.ns}:${event}_DIRECT_EXCHANGE`;
            let route = `${this.ns}:${event}_DIRECT_EXCHANGE`;
            let options = { persistent: true };
            if (delay !== null && delay !== undefined && delay !== 0) {
                if (delay instanceof Date) {
                    delay = differenceInMilliseconds(delay, new Date());
                }

                exchange = `${this.ns}:${event}_DELAY_EXCHANGE`;
                route = `${this.ns}:${event}_DELAY_EXCHANGE`;
                options = { expiration: delay, persistent: true };
            }
        
            // -------------------------------------------------------------
            channel.publish(exchange, route, pack(packx), options);
            return await channel.close();
        } catch (error) {
            throw error;
        } finally {
            await this.connection?.close();
        }
    }
}