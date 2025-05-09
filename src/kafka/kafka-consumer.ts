import { Injectable, OnModuleInit, OnModuleDestroy, Logger } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { UserHandler } from '../handlers/user.handler';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly logger = new Logger(KafkaConsumerService.name);

  constructor(
    private readonly configService: ConfigService,
    private readonly userHandler: UserHandler,
  ) {
    const brokers = this.configService.get<string>('KAFKA_BROKERS', 'localhost:9092').split(',');
    const clientId = this.configService.get<string>('KAFKA_CLIENT_ID', 'centralized-consumer');

    this.kafka = new Kafka({
      clientId,
      brokers,
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
    });

    const groupId = this.configService.get<string>('KAFKA_CONSUMER_GROUP_ID', 'centralized-consumer-group');
    this.consumer = this.kafka.consumer({ groupId });
  }

  async onModuleInit() {
    try {
      await this.connectConsumer();
      await this.subscribeToTopics();
      await this.runConsumer();
      this.logger.log('Kafka consumer initialized successfully');
    } catch (error) {
      this.logger.error('Failed to initialize Kafka consumer', error.stack);
    }
  }

  async onModuleDestroy() {
    await this.disconnectConsumer();
  }

  private async connectConsumer() {
    try {
      await this.consumer.connect();
      this.logger.log('Kafka consumer connected');
    } catch (error) {
      this.logger.error(`Failed to connect Kafka consumer: ${error.message}`, error.stack);
      throw error;
    }
  }

  private async disconnectConsumer() {
    try {
      await this.consumer.disconnect();
      this.logger.log('Kafka consumer disconnected');
    } catch (error) {
      this.logger.error(`Failed to disconnect Kafka consumer: ${error.message}`, error.stack);
    }
  }

  private async subscribeToTopics() {
    const topics = this.configService.get<string>('KAFKA_TOPICS', 'user-topic').split(',');
    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
      this.logger.log(`Subscribed to Kafka topic: ${topic}`);
    }
  }

  private async runConsumer() {
    await this.consumer.run({
      eachMessage: async ({ topic, partition, message }: EachMessagePayload) => {
        try {
          const value = message.value?.toString();

          if (!value) {
            this.logger.warn('Received empty Kafka message');
            return;
          }

          const event = JSON.parse(value);
          this.logger.debug(`Received event from topic [${topic}]: ${value}`);

          await this.processEvent(topic, event);
        } catch (error) {
          this.logger.error(`Error processing Kafka message: ${error.message}`, error.stack);
          // Optionally: Send to DLQ
        }
      },
    });
  }

  private async processEvent(topic: string, event: any) {
    const { eventType, data } = event;

    if (!eventType || !data) {
      this.logger.warn(`Invalid event received from topic ${topic}: ${JSON.stringify(event)}`);
      return;
    }

    switch (topic) {
      case 'user-topic':
        await this.handleUserEvent(eventType, data);
        break;

      case 'event-events':
        await this.handleEventEvent(eventType, data);
        break;

      case 'attendance-events':
        await this.handleAttendanceEvent(eventType, data);
        break;

      default:
        this.logger.warn(`Unhandled Kafka topic: ${topic}`);
    }
  }

  private async handleUserEvent(eventType: string, data: any) {
    switch (eventType) {
      case 'USER_CREATED':
      case 'USER_UPDATED':
        return this.userHandler.handleUserUpsert(data);
  
      case 'USER_DELETED':
        return this.userHandler.handleUserDelete(data);
        
      case 'COHORT_CREATED':
      case 'COHORT_UPDATED':
        return this.userHandler.handleCohortUpsert(data);

      case 'COHORT_DELETED':
        return this.userHandler.handleCohortDelete(data);
      default:
        this.logger.warn(`Unhandled user eventType: ${eventType}`);
    }
  }
  

  private async handleEventEvent(eventType: string, data: any) {
    this.logger.log(`Handling event-event type: ${eventType}`);
    // TODO: Implement logic for EVENT_CREATED, etc.
  }

  private async handleAttendanceEvent(eventType: string, data: any) {
    this.logger.log(`Handling attendance-event type: ${eventType}`);
    // TODO: Implement logic for ATTENDANCE_MARKED, etc.
  }
}
