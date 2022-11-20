import { Message } from '../src/message.js'
import { Consumer } from '../src/consumer.js'
import { sleep } from '../src/utils.js'

describe('Consumer', () => {
  it('should be able to create an instance', () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const consumer = new Consumer(
      connection,
      '1',
      'test',
      ['foo', 'bar'],
      async (msg) => {
        // do thing
      }
    )
    expect(consumer).toBeInstanceOf(Consumer)
  })
  it("should create group when group doesn't exist", async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn()
    xgroup.mockReturnValue('ok')
    const xpending = jest.fn()
    xpending.mockReturnValue([['msg-id-1', 'cid-1', 20000, 1]])
    const xclaim = jest.fn()
    const payload = { body: { foo: 'bar' } }
    xclaim.mockReturnValue([['msg-id-1', ['payload', JSON.stringify(payload)]]])
    connection.xclaim = xclaim
    const xreadgroup = jest.fn()
    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup
    connection.xack = xack
    const onMessage = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]
    const consumer = new Consumer(connection, id, group, topics, onMessage)
    const onError = jest.fn()
    consumer.on('error', onError)
    await consumer.startConsuming()

    await sleep(1000)
    consumer.stopConsuming()
    expect(onError).not.toHaveBeenCalled()
  })
  it('should ignore duplicate group errors', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn(() => {
      throw new Error('BUSYGROUP Consumer Group name already exists')
    })
    const xpending = jest.fn()
    const xclaim = jest.fn()
    connection.xclaim = xclaim
    const xreadgroup = jest.fn()
    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup
    connection.xack = xack
    const onMessage = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]
    const consumer = new Consumer(connection, id, group, topics, onMessage)
    const onError = jest.fn()
    try {
      await consumer.startConsuming()
      await consumer.stopConsuming()
    } catch (err) {
      onError(err)
    }
    await expect(onError).not.toHaveBeenCalled()
  })

  it('should throw an error when failed to create a group', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn(() => {
      throw new Error('unknown')
    })
    const xpending = jest.fn()
    const xclaim = jest.fn()
    connection.xclaim = xclaim
    const xreadgroup = jest.fn()
    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup
    connection.xack = xack
    const onMessage = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]
    const consumer = new Consumer(connection, id, group, topics, onMessage)
    await expect(async () => consumer.startConsuming()).rejects.toThrowError()
  })

  it('should start consume messages', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn()
    const xpending = jest.fn()
    const xreadgroup = jest.fn()
    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup
    connection.xack = xack
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    const consumer = new Consumer(
      connection,
      id,
      group,
      topics,
      async (msg) => {
        // do thing
      }
    )
    await consumer.startConsuming()
    expect(consumer).toBeInstanceOf(Consumer)
    const timeout = consumer.timeout
    const batchSize = consumer.batchSize
    for (let i = 0; i < topics.length; i++) {
      expect(xgroup).toHaveBeenNthCalledWith(
        i + 1,
        'CREATE',
        topics[i],
        group,
        '0',
        'MKSTREAM'
      )
      expect(xpending).toHaveBeenNthCalledWith(
        i + 1,
        topics[i],
        group,
        'IDLE',
        timeout,
        '-',
        '+',
        batchSize
      )

      expect(xreadgroup).toHaveBeenCalledWith(
        'GROUP',
        group,
        id,
        'COUNT',
        batchSize,
        'STREAMS',
        ...topics,
        ...new Array(topics.length).fill('0')
      )
      expect(xreadgroup).toHaveBeenCalledWith(
        'GROUP',
        group,
        id,
        'COUNT',
        batchSize,
        'STREAMS',
        ...topics,
        ...new Array(topics.length).fill('>')
      )
    }
  })

  it('should consume pending messages when starts', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn()
    const xpending = jest.fn()
    const xreadgroup = jest.fn()

    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup
    connection.xack = xack
    const onMessage = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]
    xreadgroup.mockReturnValueOnce([
      [
        topics[0],
        [
          [
            'id-1',
            [
              'payload',
              JSON.stringify({ body: 'message content ' + topics[0] })
            ]
          ]
        ]
      ],
      [
        topics[1],
        [
          [
            'id-2',
            [
              'payload',
              JSON.stringify({ body: 'message content ' + topics[1] })
            ]
          ]
        ]
      ]
    ])
    const consumer = new Consumer(connection, id, group, topics, onMessage)
    await consumer.startConsuming()
    await sleep(100)
    consumer.stopConsuming()
    const batchSize = consumer.batchSize
    for (let i = 0; i < topics.length; i++) {
      expect(xreadgroup).toHaveBeenCalledWith(
        'GROUP',
        group,
        id,
        'COUNT',
        batchSize,
        'STREAMS',
        ...topics,
        ...new Array(topics.length).fill('0')
      )
      expect(onMessage).toHaveBeenCalledWith(
        new Message('id-1', topics[0], { body: 'message content ' + topics[0] })
      )
      expect(onMessage).toHaveBeenCalledWith(
        new Message('id-2', topics[1], { body: 'message content ' + topics[1] })
      )
    }
  })

  it('should consume new messages when starts', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn()
    const xpending = jest.fn()
    const xreadgroup = jest.fn((...args: unknown[]) => {
      if (args[args.length - 1] === '>') {
        return [
          [
            topics[0],
            [
              [
                'id-1',
                [
                  'payload',
                  JSON.stringify({ body: 'message content ' + topics[0] })
                ]
              ]
            ]
          ],
          [
            topics[1],
            [
              [
                'id-2',
                [
                  'payload',
                  JSON.stringify({ body: 'message content ' + topics[1] })
                ]
              ]
            ]
          ]
        ]
      } else {
        return [
          [topics[0], []],
          [topics[1], []]
        ]
      }
    })

    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup
    connection.xack = xack
    const onMessage = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]

    const consumer = new Consumer(connection, id, group, topics, onMessage)
    await consumer.startConsuming()
    const batchSize = consumer.batchSize
    for (let i = 0; i < topics.length; i++) {
      expect(xreadgroup).toHaveBeenNthCalledWith(
        1,
        'GROUP',
        group,
        id,
        'COUNT',
        batchSize,
        'STREAMS',
        ...topics,
        ...new Array(topics.length).fill('0')
      )
      expect(xreadgroup).toHaveBeenNthCalledWith(
        2,
        'GROUP',
        group,
        id,
        'COUNT',
        batchSize,
        'STREAMS',
        ...topics,
        ...new Array(topics.length).fill('>')
      )
    }
    await sleep(1000)
    consumer.stopConsuming()
    expect(onMessage).toBeCalledWith(
      new Message('id-1', topics[0], { body: 'message content ' + topics[0] })
    )
    expect(onMessage).toBeCalledWith(
      new Message('id-2', topics[1], { body: 'message content ' + topics[1] })
    )
  })

  it('should reclaim stale messages', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn()
    const xpending = jest.fn()
    xpending.mockReturnValue([['msg-id-1', 'cid-1', 20000, 1]])
    const xclaim = jest.fn()
    const payload = { body: { foo: 'bar' } }
    xclaim.mockReturnValue([['msg-id-1', ['payload', JSON.stringify(payload)]]])
    connection.xclaim = xclaim
    const xreadgroup = jest.fn()
    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup
    connection.xack = xack
    const onMessage = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]
    const consumer = new Consumer(connection, id, group, topics, onMessage)
    await consumer.startConsuming()
    const timeout = consumer.timeout
    const batchSize = consumer.batchSize
    for (const topic of topics) {
      expect(xpending).toHaveBeenCalledWith(
        topic,
        group,
        'IDLE',
        timeout,
        '-',
        '+',
        batchSize
      )
      expect(xclaim).toHaveBeenCalledWith(topic, group, id, timeout, 'msg-id-1')
    }

    await sleep(1000)
    consumer.stopConsuming()
    for (const topic of topics) {
      expect(onMessage).toHaveBeenCalledWith(
        new Message('msg-id-1', topic, payload)
      )
    }
  })

  it('should emit an error if failed to parse raw message', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn()
    const xpending = jest.fn()
    const xclaim = jest.fn()
    connection.xclaim = xclaim
    const xreadgroup = jest.fn()
    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup

    connection.xack = xack
    const onMessage = jest.fn()
    const onError = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]
    xreadgroup.mockReturnValueOnce([
      [topics[0], [['id-1', ['payload', 'message content ']]]],
      [topics[1], [['id-2', ['payload1', 'message content ']]]]
    ])
    const consumer = new Consumer(connection, id, group, topics, onMessage)
    consumer.on('error', onError)
    await consumer.startConsuming()

    await sleep(500)
    consumer.stopConsuming()
    expect(onError).toHaveBeenCalledTimes(2)
  })

  it('should emit an error if failed to process a message', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xgroup = jest.fn()
    const xpending = jest.fn()
    const xclaim = jest.fn()
    connection.xclaim = xclaim
    const xreadgroup = jest.fn()
    const xack = jest.fn()
    connection.xgroup = xgroup
    connection.xpending = xpending
    connection.xreadgroup = xreadgroup

    connection.xack = xack
    const onMessage = jest.fn().mockImplementation(() => {
      throw new Error()
    })
    const onError = jest.fn()
    const id = '1'
    const group = 'test-group'
    const topics = ['foo', 'bar']
    // [string, [string, string[]][]][]
    xreadgroup.mockReturnValueOnce([
      [
        topics[0],
        [['id-1', ['payload', JSON.stringify({ body: 'message content ' })]]]
      ],
      [
        topics[1],
        [['id-2', ['payload', JSON.stringify({ body: 'message content' })]]]
      ]
    ])
    const consumer = new Consumer(connection, id, group, topics, onMessage)
    consumer.on('message:failed', onError)
    await consumer.startConsuming()

    await sleep(500)
    consumer.stopConsuming()
    expect(onError).toHaveBeenCalledTimes(2)
  })
})
