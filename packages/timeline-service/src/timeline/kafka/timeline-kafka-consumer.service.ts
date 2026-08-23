import { Injectable, Logger, OnModuleInit, OnModuleDestroy } from '@nestjs/common';
import { Kafka, Consumer } from 'kafkajs';
import {
  KAFKA_TOPIC_TIMELINE_EVENTS,
  KAFKA_TOPIC_TIMELINE_APPEND_NOTE,
  KAFKA_TOPIC_LEAD_CAPTURED,
  KAFKA_TOPIC_RESPONSE_SENT,
  KAFKA_TOPIC_FOLLOWUP_ACTION_REQUIRED,
  KAFKA_TOPIC_MEETING_BOOKED,
  KAFKA_GROUP_TIMELINE_ENGINE,
  KafkaTimelineEventInput,
  KafkaAppendNoteInput,
  KnownEventType,
  SourceEngine,
  ActorType,
} from '@perc/shared';
import { EventValidatorService } from '../validator/event-validator.service';
import { EventTransformerService } from '../transformer/event-transformer.service';
import { TimelineRepository } from '../repository/timeline.repository';
import { TimelineKafkaPublisherService } from './timeline-kafka-publisher.service';

@Injectable()
export class TimelineKafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(TimelineKafkaConsumerService.name);
  private kafka: Kafka | null = null;
  private consumer: Consumer | null = null;
  private isRunning = false;

  constructor(
    private readonly validator: EventValidatorService,
    private readonly transformer: EventTransformerService,
    private readonly repository: TimelineRepository,
    private readonly publisher: TimelineKafkaPublisherService,
  ) {
    const brokersEnv = process.env.KAFKA_BROKERS;
    const brokers = brokersEnv ? brokersEnv.split(',') : ['localhost:9092'];
    const enabled = process.env.KAFKA_ENABLED === 'true' || !!brokersEnv;

    if (enabled) {
      try {
        this.kafka = new Kafka({
          clientId: 'perc-timeline-engine-consumer',
          brokers,
          retry: { initialRetryTime: 100, retries: 5 },
        });
        this.consumer = this.kafka.consumer({
          groupId: process.env.KAFKA_GROUP_ID || KAFKA_GROUP_TIMELINE_ENGINE,
        });
      } catch (err: any) {
        this.logger.warn(`Kafka client init warning: ${err.message}`);
      }
    }
  }

  async onModuleInit() {
    await this.start();
  }

  async onModuleDestroy() {
    await this.stop();
  }

  async start(): Promise<void> {
    if (this.consumer && !this.isRunning) {
      try {
        await this.consumer.connect();
        await this.consumer.subscribe({
          topics: [
            KAFKA_TOPIC_TIMELINE_EVENTS,
            KAFKA_TOPIC_TIMELINE_APPEND_NOTE,
            KAFKA_TOPIC_LEAD_CAPTURED,           // Audit log: lead captured event
            KAFKA_TOPIC_RESPONSE_SENT,            // Audit log: response delivered
            KAFKA_TOPIC_FOLLOWUP_ACTION_REQUIRED, // Audit log: follow-up dispatched
            KAFKA_TOPIC_MEETING_BOOKED,           // Audit log: meeting booked
          ],
          fromBeginning: false,
        });

        this.isRunning = true;
        this.logger.log(`[Kafka Consumer] Subscribed to all domain event topics for audit logging`);

        await this.consumer.run({
          eachMessage: async ({ topic, partition, message }) => {
            const rawValue = message.value?.toString();
            const key = message.key?.toString();
            this.logger.log(`[Kafka Inbound] Topic: ${topic} | Partition: ${partition} | Key: ${key}`);

            if (!rawValue) return;

            try {
              const payload = JSON.parse(rawValue);
              if (topic === KAFKA_TOPIC_TIMELINE_EVENTS) {
                await this.processTimelineEvent(payload, topic);
              } else if (topic === KAFKA_TOPIC_TIMELINE_APPEND_NOTE) {
                await this.processAppendNoteCommand(payload, topic);
              } else if (topic === KAFKA_TOPIC_LEAD_CAPTURED) {
                await this.processDomainEvent(payload, 'LEAD_CAPTURED', SourceEngine.LEAD_CAPTURE, topic);
              } else if (topic === KAFKA_TOPIC_RESPONSE_SENT) {
                await this.processDomainEvent(payload, 'RESPONSE_SENT', SourceEngine.RESPONSE, topic);
              } else if (topic === KAFKA_TOPIC_FOLLOWUP_ACTION_REQUIRED) {
                await this.processDomainEvent(payload, 'FOLLOWUP_DISPATCHED', SourceEngine.FOLLOW_UP, topic);
              } else if (topic === KAFKA_TOPIC_MEETING_BOOKED) {
                await this.processDomainEvent(payload, 'MEETING_BOOKED', SourceEngine.MEETING, topic);
              }
            } catch (err: any) {
              this.logger.error(`[Kafka Error] Failed to process message from ${topic}: ${err.message}`);
              await this.sendToDlq(topic, err.message, rawValue);
            }
          },
        });
      } catch (error: any) {
        this.logger.warn(`[Kafka Consumer] Could not connect to Kafka broker: ${error.message}. Consumer listening in standby mode.`);
        this.isRunning = false;
      }
    }
  }

  async stop(): Promise<void> {
    if (this.consumer && this.isRunning) {
      try {
        await this.consumer.disconnect();
        this.isRunning = false;
        this.logger.log('[Kafka Consumer] Disconnected from Kafka broker.');
      } catch (err: any) {
        this.logger.error(`[Kafka Consumer] Error on disconnect: ${err.message}`);
      }
    }
  }

  /**
   * Processes an incoming Kafka timeline event message
   */
  async processTimelineEvent(event: KafkaTimelineEventInput, topic = KAFKA_TOPIC_TIMELINE_EVENTS): Promise<any> {
    try {
      this.logger.log(`[Kafka Ingest] Processing event '${event.eventType}' from '${event.sourceEngine}' for Lead: ${event.leadId}`);

      // 1. Validation
      await this.validator.validate(event as any);

      // 2. Transformation
      const transformed = this.transformer.transform(event as any);

      // 3. PostgreSQL Persistence
      const record = await this.repository.create(transformed);
      this.logger.log(`[Kafka Ingest] Saved Timeline Event ID: ${record.id}`);

      // 4. Broadcast output event to perc.timeline.event-recorded
      await this.publisher.broadcastEventRecorded(record);

      return record;
    } catch (err: any) {
      if (err.status === 409 || err.message?.includes('Duplicate Event Rejected')) {
        this.logger.warn(`[Kafka Ingest] Duplicate event skipped (${event.deduplicationKey || event.eventId})`);
        return null;
      }

      this.logger.error(`[Kafka Validation/Processing Failure]: ${err.message}`);
      await this.sendToDlq(topic, err.message, event);
      throw err;
    }
  }

  /**
   * Translates raw domain events into structured timeline audit records.
   * Called for lead.captured, response.sent, followup.action.required, meeting.booked.
   */
  private async processDomainEvent(
    payload: any,
    eventType: string,
    sourceEngine: string,
    topic: string,
  ): Promise<void> {
    const leadId = payload.leadId || payload.correlationId;
    if (!leadId) {
      this.logger.warn(`[Timeline Domain Event] Skipping event - no leadId in payload from topic ${topic}`);
      return;
    }
    const timelineInput: KafkaTimelineEventInput = {
      eventId: payload.eventId || `evt_tl_${Date.now()}_${leadId.slice(0, 8)}`,
      workflowId: payload.workflowId || leadId,
      leadId,
      eventType,
      sourceEngine,
      actorType: ActorType.SYSTEM,
      title: eventType.replace(/_/g, ' '),
      description: `Event ${eventType} processed from ${topic}`,
      metadata: payload,
      occurredAt: payload.sentAt || payload.firedAt || payload.scheduledAt || new Date().toISOString(),
    };
    await this.processTimelineEvent(timelineInput, topic);
  }

  /**
   * Processes an incoming Kafka admin note command message
   */
  async processAppendNoteCommand(command: KafkaAppendNoteInput, topic = KAFKA_TOPIC_TIMELINE_APPEND_NOTE): Promise<any> {
    try {
      this.logger.log(`[Kafka Ingest] Appending admin note for Workflow: ${command.workflowId}, Lead: ${command.leadId}`);

      const noteEvent: KafkaTimelineEventInput = {
        eventId: command.eventId || `evt_note_${Date.now()}`,
        workflowId: command.workflowId,
        leadId: command.leadId,
        eventType: KnownEventType.INTERNAL_NOTE_ADDED,
        sourceEngine: SourceEngine.ADMIN,
        actorType: ActorType.ADMIN,
        actorId: command.actorId,
        title: command.title || 'Internal Note Added',
        description: command.note,
        metadata: { ...(command.metadata || {}), isInternal: true },
        occurredAt: command.occurredAt || new Date().toISOString(),
      };

      return this.processTimelineEvent(noteEvent, topic);
    } catch (err: any) {
      this.logger.error(`[Kafka Note Command Failure]: ${err.message}`);
      await this.sendToDlq(topic, err.message, command);
      throw err;
    }
  }

  /**
   * Routes unparseable or failed messages to DLQ (perc.timeline.events.dlq)
   */
  private async sendToDlq(topic: string, reason: string, payload: any): Promise<void> {
    try {
      await this.publisher.getProducer().publishToDlq({
        originalTopic: topic,
        errorReason: reason,
        rawPayload: payload,
        receivedAt: new Date().toISOString(),
        failedAt: new Date().toISOString(),
      });
      this.logger.warn(`[Kafka DLQ] Routed invalid message to 'perc.timeline.events.dlq' (Reason: ${reason})`);
    } catch (dlqErr: any) {
      this.logger.error(`[Kafka DLQ] Failed to write to DLQ: ${dlqErr.message}`);
    }
  }
}
