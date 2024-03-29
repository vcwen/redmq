import Redis from 'ioredis'
import Debug from 'debug'

const debug = Debug('redmq:producer')

export class Producer {
  private connection: Redis.Redis
  constructor(connection: Redis.Redis) {
    this.connection = connection
  }
  public async send(
    topic: string,
    payload: unknown,
    options?: { maxLen?: number }
  ): Promise<string> {
    const val = JSON.stringify(payload)
    if (options?.maxLen) {
      debug(
        'add message to topic(%s) with maxlen(%d):%o',
        topic,
        options.maxLen,
        payload
      )
      return this.connection.xadd(
        topic,
        'MAXLEN',
        '~',
        options.maxLen,
        '*',
        'payload',
        val
      )
    } else {
      debug('add message to topic(%s):%o', topic, payload)
      return this.connection.xadd(topic, '*', 'payload', val)
    }
  }
}
