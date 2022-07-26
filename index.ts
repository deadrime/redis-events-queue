import Redis, { RedisOptions } from 'ioredis';

// Parse redis events whose format is ['someEventId', 'field1', 'value1', 'field2', 'value2']
const parseEvents = (records) => records.reduce((acc, [eventId, fields]) => {
  const event = {};
  for (let i = 0; i < fields.length; i += 2) {
    event[fields[i]] = fields[i + 1];
  }
  acc[eventId] = {
    ...event,
    _eventId: eventId,
  };

  return acc;
}, {});

export type Event<T = string> = {
  _eventId: string; // Redis gives data as array, so we can use any name for this field
  event: T;
}

export type NewEventsHandler<T = Event> = (events: T[]) => Promise<string[]>

export type SubscribeToNewEventsProps<T = Event> = {
  channelKey: string,
  channelGroup: string,
  consumerId: string,
  maxEventCount?: number, // maximum events per one xreadgroup
  retryTimeout?: number, // time for return non-processed events back to queue
  checkFrequency?: number, // block time for xreadgroup
  onNewEvents: NewEventsHandler<T>, // events handler
  redisOptions?: RedisOptions,
}

export const subscribeToNewEvents = async <T extends Event = Event>(props: SubscribeToNewEventsProps<T>) => {
  const {
    channelKey,
    channelGroup,
    consumerId,
    onNewEvents,
    maxEventCount = 10,
    retryTimeout = 30000,
    checkFrequency = 3000,
    redisOptions = {
      host: 'localhost',
      port: 6379,
      db: 0,
    }
  } = props;

  const instance = new Redis(redisOptions)

  try {
    // Create xgroup if not exists
    await instance.xgroup('CREATE', channelKey, channelGroup, '0', 'MKSTREAM');
  } catch (err) {
    if (!err.message.includes('BUSYGROUP')) {
      throw new Error(err);
    }
  }

  // eslint-disable-next-line no-constant-condition
  while (1) {
    let allEvents = {} as Record<string, T>;
    // Receive not processed/failed events
    const xautoclaimResponse = await instance.xautoclaim(channelKey, channelGroup, consumerId, retryTimeout, '0', 'COUNT', maxEventCount) as [any, any[], any[]];
    const [, oldEvents] = xautoclaimResponse;

    if (oldEvents.length) {
      allEvents = parseEvents(oldEvents);
    }

    // Get new events for the last checkFrequency ms
    const newEventsResponce = await instance.xreadgroup(
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
      const events = parseEvents(eventsStream);

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
      // Get succesfully processed events (array of _eventIds)
      // Not sure about await
      const acknowlegedIds = await onNewEvents(eventsArray);

      // Probably we want to return non-processed events to queue, but for now we don't know how to do it
      const returnToQueueIds = eventsArray
        .map(event => event._eventId)
        .filter(id => !acknowlegedIds.includes(id));
      console.log('non-processed event ids', returnToQueueIds);

      if (acknowlegedIds.length) {
        // and mark it as acknowledged
        await instance.xack(channelKey, channelGroup, ...acknowlegedIds);
      }
    } catch (error) {
      console.log(error);
    }
  }
};
