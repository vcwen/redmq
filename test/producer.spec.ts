import { Producer } from '../src/producer.js'

describe('Producer', () => {
  it('should be able to create an instance', () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const producer = new Producer(connection)
    expect(producer).toBeInstanceOf(Producer)
  })
  it('should produce new message', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xadd = jest.fn()
    connection.xadd = xadd
    const producer = new Producer(connection)
    const payload = { bar: 'zee' }
    await producer.send('foo', payload)
    expect(xadd).toHaveBeenCalledWith(
      'foo',
      '*',
      'payload',
      JSON.stringify(payload)
    )
  })

  it('should produce new message with setting max length', async () => {
    const RedisMock = jest.fn()
    const connection = new RedisMock()
    const xadd = jest.fn()
    connection.xadd = xadd
    const producer = new Producer(connection)
    const payload = { bar: 'zee' }
    await producer.send('foo', payload, { maxLen: 100 })
    expect(xadd).toHaveBeenCalledWith(
      'foo',
      'MAXLEN',
      '~',
      100,
      '*',
      'payload',
      JSON.stringify(payload)
    )
  })
})
