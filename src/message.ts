export class PendingMessageMetadata {
  constructor(
    public id: string,
    public consumer: string,
    public timeElapsed: number,
    public deliverTimes: number
  ) {}
}

export class Metadata {
  constructor(public timestamp: number, public deliverTimes: number) {}
}

export class Message<T = unknown> {
  public id: string
  public topic: string
  public metadata!: Metadata
  public payload: T
  constructor(id: string, topic: string, payload: T) {
    this.id = id
    this.topic = topic
    this.payload = payload
    const timestamp = Number(id.split('-')[0])
    this.metadata = new Metadata(timestamp, 1)
  }
}
