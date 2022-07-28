import Redis, { RedisOptions } from 'ioredis';
import { Event } from './types'

export class RedisEventPublisher {
  private redis: Redis;
  private maxQueueLength: number;

  constructor(redis: Redis | RedisOptions, maxQueueLength: number = 10000) {
    this.redis = redis instanceof Redis ? redis : new Redis(redis);
    this.maxQueueLength = maxQueueLength
  }

  async publishEvent<T = Event['payload']>(channelKey: string, payload: T, maxQueueLength: number = this.maxQueueLength) {
    return await this.redis.call('xadd', channelKey, 'MAXLEN', '~', maxQueueLength, '*', 'payload', JSON.stringify(payload)) as string
  }
}
