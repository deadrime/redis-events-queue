import Redis, { RedisOptions } from 'ioredis';
import { Event, NewEventsHandler, NewEventHandler } from './types';
import { parseEvents } from './helpers';
import { v4 as uuidv4 } from 'uuid';

type ListenEventsProps<T extends Event> = {
  onNewEvents?: NewEventsHandler<T>, // events handler
  onNewEvent?: NewEventHandler<T>,
  onEventProcessingError?: (error: Error, event: Event) => void;
  onMultipleEventsProcessingError?: (error: Error, events: Event[]) => void;
}

export class RedisEventListener<E extends Event = Event> {
  private channelGroup: string;
  private nonBlockedClient: Redis;
  private maxEventCount: number;
  private retryTimeout: number;
  private checkFrequency: number;
  private maxQueueLength: number;
  channelKey: string;
  consumerId: string;
  redis: Redis;
  eventProcessingErrorHandler: ((error: Error, event: Event) => void) | null;
  multipleEventsProcessingErrorHandler: ((error: Error, events: Event[]) => void) | null;

  constructor(
    redis: RedisOptions,
    channelKey: string,
    channelGroup: string,
    {
      consumerId = uuidv4(),
      maxEventCount = 10,
      checkFrequency = 10000, // ms
      retryTimeout = 60 * 1000 * 2, // ms, 2 min
      maxQueueLength = 10000,
    } = {}
  ) {
    this.channelKey = channelKey;
    this.channelGroup = channelGroup;
    this.redis = new Redis(redis);
    this.nonBlockedClient = new Redis(redis);
    this.maxEventCount = maxEventCount;
    this.checkFrequency = checkFrequency;
    this.retryTimeout = retryTimeout;
    this.consumerId = consumerId;
    this.maxQueueLength = maxQueueLength;
    this.eventProcessingErrorHandler = null;
    this.multipleEventsProcessingErrorHandler = null;
  }

  private async listenEvents({ onNewEvents, onNewEvent }: ListenEventsProps<E>) {
    const {
      redis,
      nonBlockedClient,
      channelKey,
      channelGroup,
      consumerId,
      maxEventCount,
      retryTimeout,
      checkFrequency,
    } = this;

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

      const eventIds = eventsArray.map(e => e._eventId);

      try {
        for (const event of eventsArray) {
          // If callback lag
          const intervalId = setInterval(async () => {
            await nonBlockedClient.xclaim(channelKey, channelGroup, consumerId, 0, event._eventId, 'JUSTID');
          }, retryTimeout - 1000);

          onNewEvent?.(event)
            .then(() => {
              clearInterval(intervalId);
              nonBlockedClient.xack(channelKey, channelGroup, event._eventId);
            })
            .catch(err => {
              console.log(err);
              clearInterval(intervalId);
              nonBlockedClient.xclaim(channelKey, channelGroup, consumerId, 0, event._eventId, 'JUSTID', 'IDLE', retryTimeout - 1000);
              console.log('Error processing', JSON.stringify(event), err);
              this.eventProcessingErrorHandler?.(err, event);
            });
        }

        if (!onNewEvents) {
          continue;
        }

        const intervalId = setInterval(async () => {
          await nonBlockedClient.xclaim(channelKey, channelGroup, consumerId, 0, ...eventIds, 'JUSTID');
        }, retryTimeout - 1000);

        // Get succesfully processed events (array of _eventIds)
        onNewEvents?.(eventsArray)
          .then((acknowlegedIds => {
            clearInterval(intervalId);

            const returnToQueueIds = eventsArray
              .map(event => event._eventId)
              .filter(id => !acknowlegedIds.includes(id));

            // Return non-processed events to queue
            nonBlockedClient.xclaim(channelKey, channelGroup, consumerId, 0, ...returnToQueueIds, 'JUSTID', 'IDLE', retryTimeout - 1000);

            if (acknowlegedIds.length) {
              // and mark it as acknowledged
              return nonBlockedClient.xack(channelKey, channelGroup, ...acknowlegedIds);
            }
          }))
          .catch(error => {
            clearInterval(intervalId);
            // Return failed event to queue
            nonBlockedClient.xclaim(channelKey, channelGroup, consumerId, 0, ...eventIds, 'JUSTID', 'IDLE', retryTimeout - 1000);
            console.log('error processing', JSON.stringify(eventsArray), error);
            this.multipleEventsProcessingErrorHandler?.(error, eventsArray);
          });
      } catch (error) {
        console.log(error);
      }
    }
  }

  onNewEvents(cb: NewEventsHandler<E>) {
    this.listenEvents({ onNewEvents: cb });
  }

  onNewEvent(cb: NewEventHandler<E>) {
    this.listenEvents({ onNewEvent: cb });
  }

  onEventProcessingError(cb: (err: Error, event: Event) => void) {
    this.eventProcessingErrorHandler = cb;
  }

  onMultipleEventsProcessingError(cb: (err: Error, events: Event[]) => void) {
    this.multipleEventsProcessingErrorHandler = cb;
  }

  async publishEvent<T = Event['payload']>(channelKey: string, payload: T, maxQueueLength: number = this.maxQueueLength) {
    return await this.nonBlockedClient.call('xadd', channelKey, 'MAXLEN', '~', maxQueueLength, '*', 'payload', JSON.stringify(payload)) as string;
  }
}
