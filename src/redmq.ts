import Redis, { RedisOptions } from 'ioredis'
import { Consumer } from './consumer.js'
import { Message } from './message.js'
import { Producer } from './producer.js'

export class RedMQ {
  private connection: Redis.Redis
  private connectionOptions?: RedisOptions
  constructor(private id: string, connection?: RedisOptions) {
    this.connectionOptions = connection
    this.connection = new Redis(connection)
  }

  public producer(options?: { shareConnection?: boolean }): Producer {
    const connection =
      options?.shareConnection === false
        ? new Redis(this.connectionOptions)
        : this.connection
    const producer = new Producer(connection)
    return producer
  }

  public consumer(
    group: string,
    topics: string[],
    onMessage: (message: Message) => Promise<unknown>,
    options?: {
      timeout?: number
      batchSize?: number
      shareConnection?: boolean
    }
  ): Consumer {
    const connection =
      options?.shareConnection === false
        ? new Redis(this.connectionOptions)
        : this.connection
    const consumer = new Consumer(
      connection,
      this.id,
      group,
      topics,
      onMessage,
      options
    )
    return consumer
  }
}
