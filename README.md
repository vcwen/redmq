# RedMQ 

> simple message queue based on Redis streams

**Only Redis 5.x and above support stream**

### Getting Started

```bash
npm install redmq
# yarn add redmq
# pnpm add redmq
```

### Usage

```typescript
import { RedMQ } from 'redmq'


const redmq = new RedMQ('id')

// producer
const producer = redmq.producer()
producer.send('my-topic', { body: 'body content' })

// consumer
const consumer = redmq.consumer(
  'group',
  ['topic1', 'topic2'],
  async (msg) => {
    // handle the message
  }
)

consumer.start()
```

### Topic

The term topic used here is actually the key of stream in Redis, multiple topics means multiple streams.

### Producer

Producer is simple and straightforward, the `maxLen` options is used for a capped stream, that means the messages will not exceed the `maxLen`(it's not accurate, since  using **almost exact** trimming (the `~` argument)).

### Consumer

Consumers use pull strategy for fetching new messages, for the messages that are not processed successfully will be retried after a specific time.

Options:

- **timeout:** message will be redelivered if timeout is reached
  
- **batchSize:** how many messages will be fetched in one time for a consumer
  
- **maxDeliverTimes:** the message will be abandon if the max deliver times reached

### Connection

Each producer and consumer will share the same Redis connection by default, if you want the producer or consumer to use an individual connection, just set `shareConnection` to false, then a new connection will created for it.