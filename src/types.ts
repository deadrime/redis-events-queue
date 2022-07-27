export type Event<T = {}> = {
  _eventId: string; // Redis gives data as array, so we can use any name for this field
  payload: T; // JSON
}

export type NewEventsHandler<T = Event> = (events: T[]) => Promise<string[]>

export type NewEventHandler<T = Event> = (event: T) => Promise<string>
