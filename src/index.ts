import amqp from 'amqplib';

const MQ_HOST = "amqp://localhost:5672"
const QUEUE_NAME = "PRIORITY_QUEUE"

async function sleep(msec: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, msec))
}

async function send(channel: amqp.Channel, queue: string, message: string, priority = 0): Promise<void> {
    try {
        await channel.sendToQueue(queue, Buffer.from(message), { priority });
        console.log(`Success publish: ${message}`);
    } catch (error) {
        console.error('Error sending message:', error);
    }
}

let counter = 0

async function receive(channel: amqp.Channel, queue: string): Promise<void> {
    try {
        await channel.consume(queue, (message) => {
            if(counter > 3) {
                // stackする
                return
            }
            if (message) {
                console.log(`Received: ${message.content.toString()}`);
                counter = counter + 1
                channel.ack(message);
            }
        });
    } catch (error) {
        console.error('Error receiving message:', error);
    }
}

const main = async () => {
    try {
        const conn = await amqp.connect(MQ_HOST);
        const channel = await conn.createChannel();

        // キューを宣言（優先度キューとして設定）
        await channel.assertQueue(QUEUE_NAME, { 
            durable: true,
            arguments: { 'x-max-priority': 10 }
        });

        const length = 10;
        const sequence = Array.from({ length }, (_, i) => i + 1);

        // メッセージの送信
        for (const i of sequence) {
            await send(channel, QUEUE_NAME, `hello${i}, priority 0`);
            await sleep(10); // 短い遅延を追加
        }
        await send(channel, QUEUE_NAME, `hello, priority 10`, 10);

        // メッセージの受信
        await receive(channel, QUEUE_NAME);

        // プログラムを終了させないためのループ
        while (true) {
            await sleep(1000);
        }
    } catch (error) {
        console.error('Error in main:', error);
    }
}

main();