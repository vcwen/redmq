import { Producer } from '../src/producer.js'
import { RedMQ } from '../src/redmq.js'
import { Consumer } from '../src/consumer.js'

describe('RedMQ', () => {
  it('should be able to create an instance', () => {
    const redmq = new RedMQ('id')
    expect(redmq).toBeInstanceOf(RedMQ)
  })
  it('should create new producer', async () => {
    const redmq = new RedMQ('id')
    const producer = redmq.producer()
    expect(producer).toBeInstanceOf(Producer)
  })
  it('should create new producer with new connection', async () => {
    const redmq = new RedMQ('id')
    const producer = redmq.producer({ shareConnection: false })

    expect(Reflect.get(producer, 'connection')).not.toBe(
      Reflect.get(redmq, 'connection')
    )
  })

  it('should create new consumer', async () => {
    const redmq = new RedMQ('id')
    const consumer = redmq.consumer('test-group', ['foo', 'bar'], jest.fn())
    expect(consumer).toBeInstanceOf(Consumer)
  })
  it('should create new consumer with new connection', async () => {
    const redmq = new RedMQ('id')
    const consumer = redmq.consumer('test-group', ['foo', 'bar'], jest.fn(), {
      shareConnection: false
    })

    expect(Reflect.get(consumer, 'connection')).not.toBe(
      Reflect.get(redmq, 'connection')
    )
  })
})
