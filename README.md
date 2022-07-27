# Redis events queue client
Simple client for handling Redis streams.
This client allow you to subscribe and handle events.

# Event format
Event format is `['payload', '{ "some": "JSON" }']` (`XADD mystream * payload '{ "some": "JSON" }'`)

# Basic usage

```ts
const myStream = new RedisStreamClient(
  {
    host: 'localhost',
    port: 6379,
    db: 0,
  },
  'someChannelKey',
  'someChannelGroup',
);

myStream.publishEvent(myStream.channelKey, { foo: "bar" })

myStream.onNewEvent(event => {
  console.log(event.payload) // { "foo": "bar" }
  return Promise.resolve() // You need to return resolved promise
})
```

# Handling multiple events

```ts
myStream.publishEvent(myStream.channelKey, { foo: "bar" })
myStream.publishEvent(myStream.channelKey, { foo: "baz" })

myStream.onNewEvents(async events => {
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

const myStream = new RedisStreamClient<MyEvent>(...)
```
