# Redis events queue client
Simple client for listen and publish events, based on redis streams.

# Event format
Event format is `['payload', '{ "some": "JSON" }']` (`XADD mystream * payload '{ "some": "JSON" }'`)

# Basic usage

```ts

const redisConfig = {
  host: 'localhost',
  port: 6379,
  db: 0,
};

const channelKey = 'myChannelKey';
const channelGroup = 'myChannelGroup';

type MyEvent = Event<{ foo: string }>;

const myListener = new RedisEventListener<MyEvent>(
  redisConfig,
  channelKey,
  channelGroup
);

const myPublisher = new RedisEventPublisher(redisConfig);

myPublisher.publishEvent<MyEvent>(myListener.channelKey, { foo: "bar" })

myListener.onNewEvent(event => {
  console.log(event.payload) // { "foo": "bar" }
  return Promise.resolve() // You need to return resolved promise
})
```

# Handling multiple events

```ts
myPublisher.publishEvent(myStream.channelKey, { foo: "bar" })
myPublisher.publishEvent(myStream.channelKey, { foo: "baz" })

myListener.onNewEvents(async events => {
  const successfullyHandledEventIds = events.map(event => {
    console.log(event.payload) // { "foo": "bar "}, { "foo": "baz" }
    return event._eventId
  })

  // You need to return every _eventId of event that you successfully handle.
  // Unhandled events will be returned to the queue
  return successfullyHandledEventIds
})
```

# Define you own payload type

```ts

type MyPayload = {
  id: number;
  createdAt: number;
  type: 'foo' | 'bar';
}

type MyEvent = Event<MyPayload>

const myStream = new myListener<MyEvent>(...)
```
