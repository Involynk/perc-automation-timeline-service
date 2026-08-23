/**
 * PERC Central Kafka Topics Definitions
 * Standardized topic names matching the PERC Engine Architecture specifications.
 */

// Lead Capture Engine (Engine 1) Topics
export const KAFKA_TOPIC_LEAD_CAPTURED = process.env.KAFKA_TOPIC_LEAD_CAPTURED || 'perc.lead-events';

// Response Engine (Engine 2) Topics
export const KAFKA_TOPIC_RESPONSE_SENT = process.env.KAFKA_TOPIC_RESPONSE_SENT || 'perc.response.sent';

// Follow-up Engine (Engine 3) Topics
export const KAFKA_TOPIC_FOLLOWUP_ACTION_REQUIRED = process.env.KAFKA_TOPIC_FOLLOWUP_ACTION_REQUIRED || 'perc.followup.action-required';

// Meeting Engine (Engine 7) Topics
export const KAFKA_TOPIC_MEETING_BOOKED = process.env.KAFKA_TOPIC_MEETING_BOOKED || 'perc.meeting-events';
export const KAFKA_TOPIC_MEETING_CREATE_REQUESTED = process.env.KAFKA_TOPIC_MEETING_CREATE_REQUESTED || 'perc.meeting.create-requested';

// Timeline Engine (Engine 5) Topics
export const KAFKA_TOPIC_TIMELINE_EVENTS = 'perc.timeline.events';
export const KAFKA_TOPIC_TIMELINE_EVENT_INGEST = 'perc.timeline.event-ingest-requested';
export const KAFKA_TOPIC_TIMELINE_EVENT_RECORDED = 'perc.timeline.event-recorded';
export const KAFKA_TOPIC_TIMELINE_APPEND_NOTE = 'perc.timeline.append-note-requested';
export const KAFKA_TOPIC_TIMELINE_DLQ = 'perc.timeline.events.dlq';

// Notification Engine (Engine 8) Topics
export const KAFKA_TOPIC_NOTIFICATION_SEND = 'perc.notification.send-requested';
export const KAFKA_TOPIC_NOTIFICATION_BROADCAST = 'perc.notification.broadcast-requested';
export const KAFKA_TOPIC_NOTIFICATION_DELIVERED = 'perc.notification.notification-delivered';
export const KAFKA_TOPIC_NOTIFICATION_DLQ = 'perc.notification.commands.dlq';

// Scheduler Engine (Engine 4) Topics
export const KAFKA_TOPIC_SCHEDULER_SCHEDULE = 'perc.scheduler.timer-schedule-requested';
export const KAFKA_TOPIC_SCHEDULER_CANCEL = 'perc.scheduler.timer-cancel-requested';
export const KAFKA_TOPIC_SCHEDULER_RESCHEDULE = 'perc.scheduler.timer-reschedule-requested';
export const KAFKA_TOPIC_SCHEDULER_TRIGGERED = 'perc.scheduler.timer-triggered';
export const KAFKA_TOPIC_SCHEDULER_DLQ = 'perc.scheduler.commands.dlq';

// Consumer Group IDs
export const KAFKA_GROUP_TIMELINE_ENGINE = 'perc-timeline-consumer-group';
export const KAFKA_GROUP_ANALYTICS_ENGINE = 'perc-analytics-consumer-group';
export const KAFKA_GROUP_FOLLOWUP_ENGINE = 'perc-followup-consumer-group';
export const KAFKA_GROUP_RECOMMENDATION_ENGINE = 'perc-recommendation-consumer-group';
export const KAFKA_GROUP_NOTIFICATION_ENGINE = 'perc-notification-consumer-group';
export const KAFKA_GROUP_MEETING_ENGINE = 'perc-meeting-consumer-group';
export const KAFKA_GROUP_WORKFLOW_ENGINE = 'perc-workflow-consumer-group';

