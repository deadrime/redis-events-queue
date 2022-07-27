import Redis, { RedisOptions } from 'ioredis';
import { Event, NewEventsHandler, NewEventHandler } from './types'
import { parseEvents } from './helpers'
import { v4 as uuidv4 } from 'uuid';

type ListenEventsProps<T extends Event> = {
  onNewEvents?: NewEventsHandler<T>, // events handler
  onNewEvent?: NewEventHandler<T>,
}

export class RedisStreamClient<E extends Event = Event<any>> {
  channelKey: string;
  channelGroup: string;
  redis: Redis;
  consumerId: string
  maxEventCount: number
  retryTimeout: number
  checkFrequency: number
  maxQueueLength: number

  constructor(
    redis: Redis | RedisOptions,
    channelKey: string,
    channelGroup: string,
    {
      consumerId = uuidv4(),
      maxEventCount = 10,
      checkFrequency = 3000,
      retryTimeout = 3000,
      maxQueueLength = 10000,
    } = {}
  ) {
    this.channelKey = channelKey;
    this.channelGroup = channelGroup;
    this.redis = redis instanceof Redis ? redis : new Redis(redis);
    this.maxEventCount = maxEventCount;
    this.checkFrequency = checkFrequency;
    this.retryTimeout = retryTimeout
    this.consumerId = consumerId;
    this.maxQueueLength = maxQueueLength
  }

  private async listenEvents({ onNewEvents, onNewEvent }: ListenEventsProps<E>) {
    const {
      redis,
      channelKey,
      channelGroup,
      consumerId,
      maxEventCount,
      retryTimeout,
      checkFrequency,
    } = this

    try {
      // Create xgroup if not exists
      await redis.xgroup('CREATE', channelKey, channelGroup, '0', 'MKSTREAM');
    } catch (err: any) {
      if (!err.message.includes('BUSYGROUP')) {
        throw new Error(err);
      }
    }
    // eslint-disable-next-line no-constant-condition
    while (1) {
      let allEvents = {} as Record<string, E>;
      // Receive not processed/failed events
      const xautoclaimResponse = await redis.xautoclaim(channelKey, channelGroup, consumerId, retryTimeout, '0', 'COUNT', maxEventCount) as [any, any[], any[]];
      const [, oldEvents] = xautoclaimResponse;

      if (oldEvents.length) {
        allEvents = parseEvents<E>(oldEvents);
      }

      // Get new events for the last checkFrequency ms
      const newEventsResponce = await redis.xreadgroup(
        'GROUP',
        channelGroup,
        consumerId,
        'COUNT',
        Math.max(maxEventCount - oldEvents.length, 1),
        'BLOCK',
        checkFrequency,
        'STREAMS',
        channelKey,
        '>'
      ) as any[];

      if (newEventsResponce) {
        // Parse all channels
        const channels = newEventsResponce.reduce<Record<string, Event[]>>((acc, [streamName, events]) => {
          acc[streamName] = events;
          return acc;
        }, {});

        // But we want events only from specified channel
        const eventsStream = channels[channelKey];
        const events = parseEvents<E>(eventsStream);

        // Merge old (not processed) and new events
        allEvents = {
          ...allEvents,
          ...events,
        };
      }

      const eventsArray = Object.values(allEvents);

      if (!eventsArray.length) {
        continue;
      }

      try {
        for (let event of eventsArray) {
          onNewEvent?.(event)?.then((acknowlegedId) => {
            if (acknowlegedId) {
              return redis.xack(channelKey, channelGroup, acknowlegedId);
            }
          })
        }
        // Get succesfully processed events (array of _eventIds)
        onNewEvents?.(eventsArray)?.then((acknowlegedIds => {
          // Probably we want to return non-processed events to queue, but for now we don't know how to do it
          const returnToQueueIds = eventsArray
            .map(event => event._eventId)
            .filter(id => !acknowlegedIds.includes(id));
          console.log('non-processed event ids', returnToQueueIds);

          if (acknowlegedIds.length) {
            // and mark it as acknowledged
            return redis.xack(channelKey, channelGroup, ...acknowlegedIds);
          }
        }))
      } catch (error) {
        console.log(error);
      }
    }
  }

  onNewEvents(cb: NewEventsHandler<E>) {
    this.listenEvents({ onNewEvents: cb })
  }

  onNewEvent(cb: NewEventHandler<E>) {
    this.listenEvents({ onNewEvent: cb })
  }

  async publishEvent(payload: E['payload']) {
    return await this.redis.call('xadd', this.channelKey, 'MAXLEN', '~', this.maxQueueLength, '*', 'payload', JSON.stringify(payload)) as string
  }
}

export default RedisStreamClient