export class Payload {
  constructor(public body: unknown, public header?: Record<string, unknown>) {}
}

export class PendingMessageMetadata {
  constructor(
    public id: string,
    public consumer: string,
    public timeElapsed: number,
    public deliverTimes: number
  ) {}
}

export class Message {
  public id: string
  public topic: string
  public header?: Record<string, unknown>
  public body: unknown
  constructor(id: string, topic: string, payload: Payload) {
    this.id = id
    this.topic = topic
    this.header = payload.header
    this.body = payload.body
  }
}
