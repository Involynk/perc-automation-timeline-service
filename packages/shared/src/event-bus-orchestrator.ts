import { SourceEngine, KnownEventType, ActorType } from './enums';
import { KafkaProducerService } from './kafka/kafka-producer.service';
import { KafkaTimelineEventInput } from './kafka/kafka.contracts';

export interface PublishEngineEventOptions {
  workflowId: string;
  leadId: string;
  eventType: string | KnownEventType;
  sourceEngine: string | SourceEngine;
  actorType: string | ActorType;
  title: string;
  description: string;
  metadata?: Record<string, any>;
  deduplicationKey?: string;
  occurredAt?: string;
  baseUrl?: string;
}

export interface PublishEventResponse {
  success: boolean;
  message?: string;
  data?: any;
  error?: string;
}

export class EventBusOrchestrator {
  private baseUrl: string;
  private kafkaProducer: KafkaProducerService;

  constructor(baseUrl?: string, kafkaProducer?: KafkaProducerService) {
    this.baseUrl = baseUrl || process.env.TIMELINE_SERVICE_URL || 'http://localhost:3003';
    this.kafkaProducer = kafkaProducer || new KafkaProducerService();
  }

  /**
   * Publishes an engine event to the central Timeline Engine via Kafka
   * Topic: perc.timeline.events
   */
  async publishKafkaEvent(options: PublishEngineEventOptions & { eventId?: string }) {
    const eventId = options.eventId || `evt_${Date.now()}_${Math.random().toString(36).substring(7)}`;
    const kafkaInput: KafkaTimelineEventInput = {
      eventId,
      workflowId: options.workflowId,
      leadId: options.leadId,
      eventType: options.eventType,
      sourceEngine: options.sourceEngine,
      actorType: options.actorType,
      title: options.title,
      description: options.description,
      metadata: options.metadata || {},
      deduplicationKey:
        options.deduplicationKey ||
        `dedup_${String(options.sourceEngine).toLowerCase()}_${Date.now()}_${Math.random().toString(36).substring(7)}`,
      occurredAt: options.occurredAt || new Date().toISOString(),
    };

    return this.kafkaProducer.publishTimelineEvent(kafkaInput);
  }

  /**
   * Publishes an engine event from any producer engine to the central Timeline Engine Bus via REST or Kafka
   */
  async publishEvent(options: PublishEngineEventOptions): Promise<PublishEventResponse> {
    if (process.env.USE_KAFKA_FOR_EVENTS === 'true') {
      const kafkaResult = await this.publishKafkaEvent(options);
      return {
        success: kafkaResult.success,
        message: 'Event published to Kafka perc.timeline.events topic',
        data: kafkaResult,
        error: kafkaResult.error,
      };
    }

    const payload = {
      workflowId: options.workflowId,
      leadId: options.leadId,
      eventType: options.eventType,
      sourceEngine: options.sourceEngine,
      actorType: options.actorType,
      title: options.title,
      description: options.description,
      metadata: options.metadata || {},
      deduplicationKey:
        options.deduplicationKey ||
        `dedup_${String(options.sourceEngine).toLowerCase()}_${Date.now()}_${Math.random().toString(36).substring(7)}`,
      occurredAt: options.occurredAt || new Date().toISOString(),
    };

    try {
      const res = await fetch(`${this.baseUrl}/api/v1/events/publish`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
      const data = await res.json();
      return data;
    } catch (error: any) {
      return {
        success: false,
        error: error.message || 'Unknown network error',
      };
    }
  }

  /**
   * Helper to fetch timeline events for a workflow
   */
  async getWorkflowTimeline(workflowId: string, options: { page?: number; limit?: number; sourceEngine?: string } = {}) {
    try {
      const query = new URLSearchParams(options as any).toString();
      const res = await fetch(`${this.baseUrl}/api/v1/workflows/${workflowId}/timeline?${query}`);
      return await res.json();
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  /**
   * Helper to fetch engine analytics breakdown stats
   */
  async getEngineStats() {
    try {
      const res = await fetch(`${this.baseUrl}/api/v1/engines/stats`);
      return await res.json();
    } catch (error: any) {
      return { success: false, error: error.message };
    }
  }

  getKafkaProducer(): KafkaProducerService {
    return this.kafkaProducer;
  }
}
