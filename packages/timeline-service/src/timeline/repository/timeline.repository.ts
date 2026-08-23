import { Injectable, Logger } from '@nestjs/common';
import { PrismaService } from '../../prisma/prisma.service';
import { TimelineEventRecord, PaginatedTimelineResult, EngineStats } from '../interfaces/timeline-event.interface';
import { TimelineQueryDto } from '../dto/timeline-query.dto';
import { v4 as uuidv4 } from 'uuid';

@Injectable()
export class TimelineRepository {
  private readonly logger = new Logger(TimelineRepository.name);
  private readonly memoryStore: Map<string, TimelineEventRecord> = new Map();

  constructor(private readonly prisma?: PrismaService) {}

  async create(data: Partial<TimelineEventRecord>): Promise<TimelineEventRecord> {
    const record: TimelineEventRecord = {
      id: data.id || uuidv4(),
      workflowId: data.workflowId!,
      leadId: data.leadId!,
      eventType: data.eventType!,
      sourceEngine: data.sourceEngine!,
      actorType: data.actorType || 'System',
      actorId: data.actorId || null,
      title: data.title || '',
      description: data.description || '',
      metadata: data.metadata || {},
      deduplicationKey: data.deduplicationKey || null,
      occurredAt: data.occurredAt ? new Date(data.occurredAt) : new Date(),
      createdAt: new Date(),
    };

    if (this.prisma && this.prisma.isConnected) {
      try {
        const created = await this.prisma.timelineEvent.create({
          data: {
            id: record.id,
            workflowId: record.workflowId,
            leadId: record.leadId,
            eventType: record.eventType,
            sourceEngine: record.sourceEngine,
            actorType: record.actorType,
            actorId: record.actorId,
            title: record.title,
            description: record.description,
            metadata: record.metadata,
            deduplicationKey: record.deduplicationKey,
            occurredAt: record.occurredAt,
          },
        });
        return this.mapPrismaToRecord(created);
      } catch (error: any) {
        this.logger.warn(`Prisma error, falling back to memory store: ${error.message}`);
      }
    }

    this.memoryStore.set(record.id, record);
    return record;
  }

  async findByDeduplicationKey(key: string): Promise<TimelineEventRecord | null> {
    if (this.prisma && this.prisma.isConnected) {
      try {
        const found = await this.prisma.timelineEvent.findUnique({
          where: { deduplicationKey: key },
        });
        return found ? this.mapPrismaToRecord(found) : null;
      } catch (err) {}
    }

    for (const record of this.memoryStore.values()) {
      if (record.deduplicationKey === key) return record;
    }
    return null;
  }

  async findById(id: string): Promise<TimelineEventRecord | null> {
    if (this.prisma && this.prisma.isConnected) {
      try {
        const found = await this.prisma.timelineEvent.findUnique({ where: { id } });
        return found ? this.mapPrismaToRecord(found) : null;
      } catch (err) {}
    }

    return this.memoryStore.get(id) || null;
  }

  async findByWorkflowId(workflowId: string, query: TimelineQueryDto): Promise<PaginatedTimelineResult> {
    return this.queryStore((item) => item.workflowId === workflowId, query);
  }

  async findByLeadId(leadId: string, query: TimelineQueryDto): Promise<PaginatedTimelineResult> {
    return this.queryStore((item) => item.leadId === leadId, query);
  }

  async search(query: TimelineQueryDto): Promise<PaginatedTimelineResult> {
    return this.queryStore(() => true, query);
  }

  async getStats(): Promise<EngineStats> {
    const all = await this.getAllRecords();
    const eventsByEngine: Record<string, number> = {};
    const eventsByType: Record<string, number> = {};
    const workflows = new Set<string>();

    all.forEach((evt) => {
      eventsByEngine[evt.sourceEngine] = (eventsByEngine[evt.sourceEngine] || 0) + 1;
      eventsByType[evt.eventType] = (eventsByType[evt.eventType] || 0) + 1;
      if (evt.workflowId) workflows.add(evt.workflowId);
    });

    return {
      totalEvents: all.length,
      eventsByEngine,
      eventsByType,
      activeWorkflows: workflows.size,
    };
  }

  private async getAllRecords(): Promise<TimelineEventRecord[]> {
    if (this.prisma && this.prisma.isConnected) {
      try {
        const records = await this.prisma.timelineEvent.findMany({
          orderBy: { occurredAt: 'desc' },
        });
        return records.map((r: any) => this.mapPrismaToRecord(r));
      } catch (err) {}
    }

    return Array.from(this.memoryStore.values());
  }


  private async queryStore(
    filterPredicate: (item: TimelineEventRecord) => boolean,
    query: TimelineQueryDto,
  ): Promise<PaginatedTimelineResult> {
    const { type, sourceEngine, actorType, search, page = 1, limit = 20, sort = 'desc' } = query;

    let items = await this.getAllRecords();
    items = items.filter(filterPredicate);

    if (type) items = items.filter((item) => item.eventType.toLowerCase() === type.toLowerCase());
    if (sourceEngine) items = items.filter((item) => item.sourceEngine.toLowerCase() === sourceEngine.toLowerCase());
    if (actorType) items = items.filter((item) => item.actorType.toLowerCase() === actorType.toLowerCase());

    if (search) {
      const q = search.toLowerCase();
      items = items.filter(
        (item) =>
          item.title.toLowerCase().includes(q) ||
          item.description.toLowerCase().includes(q) ||
          item.eventType.toLowerCase().includes(q) ||
          JSON.stringify(item.metadata).toLowerCase().includes(q),
      );
    }

    items.sort((a, b) => {
      const diff = a.occurredAt.getTime() - b.occurredAt.getTime();
      return sort === 'asc' ? diff : -diff;
    });

    const total = items.length;
    const totalPages = Math.ceil(total / limit) || 1;
    const startIndex = (page - 1) * limit;
    const paginated = items.slice(startIndex, startIndex + limit);

    return { data: paginated, total, page, limit, totalPages };
  }

  private mapPrismaToRecord(dbRecord: any): TimelineEventRecord {
    return {
      id: dbRecord.id,
      workflowId: dbRecord.workflowId,
      leadId: dbRecord.leadId,
      eventType: dbRecord.eventType,
      sourceEngine: dbRecord.sourceEngine,
      actorType: dbRecord.actorType,
      actorId: dbRecord.actorId,
      title: dbRecord.title,
      description: dbRecord.description,
      metadata: (dbRecord.metadata as Record<string, any>) || {},
      deduplicationKey: dbRecord.deduplicationKey,
      occurredAt: dbRecord.occurredAt,
      createdAt: dbRecord.createdAt,
    };
  }
}
