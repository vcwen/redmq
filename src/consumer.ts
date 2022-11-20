import Redis from 'ioredis'
import _ from 'lodash'
import { Payload, Message, PendingMessageMetadata } from './message.js'
import Debug from 'debug'
import { sleep } from './utils.js'
import { EventEmitter } from 'events'
const debug = Debug('redmq:consumer')

const DEFAULT_PULL_INTERVAL = 200
const DEFAULT_STALE_MESSAGE_PULL_INTERVAL = 1000

export class Consumer {
  private connection: Redis.Redis
  public batchSize: number
  public timeout: number
  private topics: string[]
  private eventEmitter = new EventEmitter()
  private isTerminated = false
  constructor(
    connection: Redis.Redis,
    private id: string,
    private group: string,
    topics: string[],
    private readonly onMessage: (message: Message) => Promise<unknown>,
    options?: {
      timeout?: number
      batchSize?: number
    }
  ) {
    this.topics = topics
    this.timeout = options?.timeout ?? 10000
    this.batchSize = options?.batchSize ?? 1
    this.connection = connection
  }

  public on(
    event: 'error' | 'message:failed',
    listener: (...args: unknown[]) => void
  ): EventEmitter {
    return this.eventEmitter.on(event, listener)
  }
  public async startConsuming(): Promise<void> {
    await this.createGroup(this.group)
    await this.consumePendingMessages()
    debug('%s:start consuming new messages', this.getConsumerInfo())
    this.consumeNewMessages()
    debug('%s:watch stale messages', this.getConsumerInfo())
    this.watchStaleMessages()
  }

  public stopConsuming(): void {
    this.isTerminated = true
  }

  private async createGroup(group: string): Promise<void> {
    const tasks = this.topics.map(async (topic) => {
      try {
        await this.connection.xgroup('CREATE', topic, group, '0', 'MKSTREAM')
      } catch (err) {
        if (err instanceof Error && err.message.startsWith('BUSYGROUP')) {
          // ignore error: 'BUSYGROUP Consumer Group name already exists'
        } else {
          throw err
        }
      }
    })
    await Promise.all(tasks)
  }
  private async readMessages(
    lastMessageIds: {
      topic: string
      lastMessageId: string
    }[]
  ): Promise<{ topic: string; messages: Message[] }[]> {
    const keys = lastMessageIds.map((item) => item.topic)
    const lastIds = lastMessageIds.map((item) => item.lastMessageId)
    const streams = await this.connection.xreadgroup(
      'GROUP',
      this.group,
      this.id,
      'COUNT',
      this.batchSize,
      'STREAMS',
      ...keys,
      ...lastIds
    )
    if (_.isEmpty(streams)) {
      return []
    }
    return streams.map((stream) => {
      const topic = stream[0]
      const rawMessages = stream[1]
      const messages = rawMessages
        .map((rawMessage) => this.parseMessage(topic, rawMessage))
        .filter((item) => item !== null) as Message[]
      return { topic, messages }
    })
  }
  private async consumeNewMessages() {
    while (!this.isTerminated) {
      try {
        const startTime = Date.now()
        const streams = await this.readMessages(
          this.topics.map((topic) => ({ topic, lastMessageId: '>' }))
        )
        const tasks = streams.map(async ({ topic, messages }) =>
          this.consumeMessages(topic, messages)
        )
        const results = await Promise.allSettled(tasks)
        results.forEach((res) => {
          if (res.status === 'rejected') {
            debug(res.reason)
            this.eventEmitter.emit('error', res.reason)
          }
        })
        const timeElapsed = Date.now() - startTime
        const delay = DEFAULT_PULL_INTERVAL - timeElapsed
        await sleep(delay > 0 ? delay : 0)
      } catch (err) {
        debug(err)
        this.eventEmitter.emit('error', err)
      }
    }
  }
  private async consumePendingMessages() {
    let lastMessageIds = this.topics.map((topic) => ({
      topic,
      lastMessageId: '0'
    }))
    while (!this.isTerminated) {
      try {
        const streams = await this.readMessages(lastMessageIds)
        lastMessageIds = []
        const tasks = streams.map(async ({ topic, messages }) => {
          if (!_.isEmpty(messages)) {
            const lastMessageId = messages[messages.length - 1].id
            lastMessageIds.push({ topic, lastMessageId })
            await this.consumeMessages(topic, messages)
          }
        })
        const results = await Promise.allSettled(tasks)
        results.forEach((res) => {
          if (res.status === 'rejected') {
            this.eventEmitter.emit('error', res.reason)
            debug(res.reason)
          }
        })
        if (_.isEmpty(lastMessageIds)) {
          break
        }
      } catch (err) {
        this.eventEmitter.emit('error', err)
        debug(err)
      }
    }
  }
  private async watchStaleMessages() {
    let lastId = '-'
    while (!this.isTerminated) {
      const startTime = Date.now()
      const tasks = this.topics.map(async (topic) => {
        const staleMessageInfoList: unknown[][] =
          await this.connection.xpending(
            topic,
            this.group,
            'IDLE',
            this.timeout,
            lastId,
            '+',
            this.batchSize
          )
        if (_.isEmpty(staleMessageInfoList)) {
          lastId = '-'
          return
        }
        const metadataList = staleMessageInfoList.map(
          (item) =>
            new PendingMessageMetadata(
              item[0] as string,
              item[1] as string,
              item[2] as number,
              item[3] as number
            )
        )
        lastId = metadataList[metadataList.length - 1].id

        try {
          const rawMessages = await this.connection.xclaim(
            topic,
            this.group,
            this.id,
            this.timeout,
            ...metadataList.map((item) => item.id)
          )
          const messages = rawMessages
            .map((item) => this.parseMessage(topic, item))
            .filter((item) => item !== null) as Message[]
          await this.consumeMessages(topic, messages)
        } catch (err) {
          debug(
            '%s:failed to claim stale message: %o',
            this.getConsumerInfo(),
            err
          )
          throw err
        }
      })

      const results = await Promise.allSettled(tasks)
      results.forEach((res) => {
        if (res.status === 'rejected') {
          this.eventEmitter.emit('error', res.reason)
        }
      })
      const timeElapsed = Date.now() - startTime
      const delay = DEFAULT_STALE_MESSAGE_PULL_INTERVAL - timeElapsed
      await sleep(delay > 0 ? delay : 0)
    }
  }

  private async ackMessages(
    topic: string,
    group: string,
    ...messageIds: string[]
  ) {
    try {
      await this.connection.xack(topic, group, ...messageIds)
      debug('%s:messages acknowledged:%s', this.getConsumerInfo(), messageIds)
    } catch (err) {
      debug(
        `%s:failed to ack messages:${messageIds}, err:%o`,
        this.getConsumerInfo(),
        err
      )
      this.eventEmitter.emit('error', err)
    }
  }
  private parseMessage(
    topic: string,
    rawMessage: [string, string[]]
  ): Message | null {
    try {
      const [id, dataArray] = rawMessage
      debug('raw message:%o', rawMessage)
      const [prop, payloadStr] = dataArray
      if (prop !== 'payload') {
        throw new Error('invalid message data')
      }
      const payload: Payload = JSON.parse(payloadStr)
      return new Message(id, topic, payload)
    } catch (err) {
      debug(
        '%s:failed to parse message payload:%o',
        this.getConsumerInfo(),
        rawMessage
      )
      this.eventEmitter.emit('error', err, {
        type: 'raw-message',
        data: rawMessage
      })
      return null
    }
  }

  private async consumeMessages(topic: string, messages: Message[]) {
    if (_.isEmpty(messages)) {
      return
    }
    const tasks = messages.map(async (message) => {
      try {
        await this.onMessage(message)
        return message.id
      } catch (err) {
        debug('failed to handle message: %o, reason:%s', message, err)
        this.eventEmitter.emit('message:failed', err, {
          type: 'message',
          data: message
        })
      }
    })
    const results = await Promise.allSettled(tasks)
    const completedMessageIds: string[] = []
    results.forEach((res) => {
      if (res.status === 'fulfilled' && res.value) {
        completedMessageIds.push(res.value)
      }
    })
    if (!_.isEmpty(completedMessageIds)) {
      await this.ackMessages(topic, this.group, ...completedMessageIds)
    }
  }
  private getConsumerInfo(): string {
    return `${this.id}|${this.group}`
  }
}
