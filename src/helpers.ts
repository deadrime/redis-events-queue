import { Event } from './types'

// Parse redis events whose format is ['someEventId', 'field1', 'value1', 'field2', 'value2']
export const parseEvents = <E extends Event = Event<any>>(records: any[]) => records.reduce<Record<string, E>>((acc, [eventId, fields]) => {
  const event = {} as Record<string, string>;
  for (let i = 0; i < fields.length; i += 2) {
    event[fields[i]] = fields[i + 1];
  }
  let payload: E['payload'] = {}
  try {
    payload = JSON.parse(event.payload) as E['payload'];
  } catch (err) {
    console.error('Error while parsing event', event, err)
  }
  acc[eventId] = {
    _eventId: String(eventId),
    payload,
  } as E
  return acc;
}, {});
