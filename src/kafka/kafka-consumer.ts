import {
  Injectable,
  OnModuleInit,
  OnModuleDestroy,
  Logger,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Kafka, Consumer, EachMessagePayload } from 'kafkajs';
import { UserHandler } from '../handlers/user.handler';
import { CourseHandler } from 'src/handlers/course.handler';
import { ContentHandler } from 'src/handlers/content.handler';
import { AttendanceHandler } from '../handlers/attendance.handler';
import { AssessmentHandler } from '../handlers/assessment.handler';
import { EventHandler } from 'src/handlers/event.handler';

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly kafka: Kafka;
  private readonly consumer: Consumer;
  private readonly logger = new Logger(KafkaConsumerService.name);

  constructor(
    private readonly configService: ConfigService,
    private readonly userHandler: UserHandler,
    private readonly courseHandler: CourseHandler,
    private readonly contentHandler: ContentHandler,
    private readonly attendanceHandler: AttendanceHandler,
    private readonly assessmentHandler: AssessmentHandler,
    private readonly eventHandler: EventHandler,
  ) {
    const brokers = this.configService
      .get<string>('KAFKA_BROKERS', 'localhost:9092')
      .split(',');
    const clientId = this.configService.get<string>(
      'KAFKA_CLIENT_ID',
      'centralized-consumer',
    );

    this.kafka = new Kafka({
      clientId,
      brokers,
      retry: {
        initialRetryTime: 100,
        retries: 8,
      },
    });

    const groupId = this.configService.get<string>(
      'KAFKA_CONSUMER_GROUP_ID',
      'centralized-consumer-group',
    );
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
      this.logger.error(
        `Failed to connect Kafka consumer: ${error.message}`,
        error.stack,
      );
      throw error;
    }
  }

  private async disconnectConsumer() {
    try {
      await this.consumer.disconnect();
      this.logger.log('Kafka consumer disconnected');
    } catch (error) {
      this.logger.error(
        `Failed to disconnect Kafka consumer: ${error.message}`,
        error.stack,
      );
    }
  }

  private async subscribeToTopics() {
    const topics = this.configService
      .get<string>('KAFKA_TOPICS', 'user-topic')
      .split(',');
    for (const topic of topics) {
      await this.consumer.subscribe({ topic, fromBeginning: false });
      this.logger.log(`Subscribed to Kafka topic: ${topic}`);
    }
  }

  private async runConsumer() {
    await this.consumer.run({
      eachMessage: async ({
        topic,
        partition,
        message,
      }: EachMessagePayload) => {
        try {
          const value = message.value?.toString();

          if (!value) {
            this.logger.warn('Received empty Kafka message');
            return;
          }

          const event = JSON.parse(value);
          // this.logger.debug(`Received event from topic [${topic}]: ${value}`);

          await this.processEvent(topic, event);
        } catch (error) {
          this.logger.error(
            `Error processing Kafka message: ${error.message}`,
            error.stack,
          );
          // Optionally: Send to DLQ
        }
      },
    });
  }

  private async processEvent(topic: string, event: any) {
    const { eventType, data } = event;

    console.log('=== KAFKA MESSAGE RECEIVED ===');
    console.log('Topic:', topic);
    console.log('EventType:', eventType);
    console.log('Data:', JSON.stringify(data, null, 2));
    console.log('===============================');

    if (!eventType || !data) {
      this.logger.warn(
        `Invalid event received from topic ${topic}: ${JSON.stringify(event)}`,
      );
      return;
    }

    // Special routing for course enrollment events regardless of topic
    if (eventType === 'COURSE_ENROLLMENT_CREATED') {
      console.log('🔀 ROUTING: Course enrollment event detected, routing to course handler');
      await this.handleCourseEvent(eventType, data);
      return;
    }

    // Special routing for content tracking events regardless of topic
    if (eventType === 'CONTENT_TRACKING_CREATED') {
      console.log('🔀 ROUTING: Content tracking event detected, routing to content handler');
      await this.handleContentEvent(eventType, data);
      return;
    }

    switch (topic) {
      case 'user-topic':
        await this.handleUserEvent(eventType, data);
        break;

      case 'event-topic':
        await this.handleEventEvent(eventType, data);
        break;

      case 'attendance-topic':
        await this.handleAttendanceEvent(eventType, data);
        break;
      case 'course-topic':
        await this.handleCourseEvent(eventType, data);
        break;

      case 'assessment-topic':
        await this.handleAssessmentEvent(eventType, data);
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
    switch (eventType) {
      case 'EVENT_CREATED':
      case 'EVENT_UPDATED':
        return this.eventHandler.handleEventUpsert(data);

      case 'EVENT_DELETED':
        return this.eventHandler.handleEventDelete(data);

      default:
        this.logger.warn(`Unhandled event eventType: ${eventType}`);
    }
  }

  private async handleAttendanceEvent(eventType: string, data: any) {
    this.logger.log(`Handling attendance-event type: ${eventType}`);
    switch (eventType) {
      case 'ATTENDANCE_CREATED':
      case 'ATTENDANCE_UPDATED':
        return this.attendanceHandler.handleAttendanceUpsert(data);
  
      case 'ATTENDANCE_DELETED':
        return this.attendanceHandler.handleAttendanceDelete(data);
        
      default:
        this.logger.warn(`Unhandled attendance eventType: ${eventType}`);
    }
  }

  private async handleAssessmentEvent(eventType: string, data: any) {
    this.logger.log(`Handling assessment-event type: ${eventType}`);
    switch (eventType) {
      case 'ASSESSMENT_CREATED':
      case 'ASSESSMENT_UPDATED':
        return this.assessmentHandler.handleAssessmentUpsert(data);
  
      case 'ASSESSMENT_DELETED':
        return this.assessmentHandler.handleAssessmentDelete(data);
        
      default:
        this.logger.warn(`Unhandled assessment eventType: ${eventType}`);
    }
  }
  private async handleCourseEvent(eventType: string, data: any) {
    console.log(`🎯 [KafkaConsumer] handleCourseEvent called with eventType: ${eventType}`);
    console.log(`🎯 [KafkaConsumer] Course event data:`, JSON.stringify(data, null, 2));
    
    this.logger.log(`Handling course-event type: ${eventType}`);
    switch (eventType) {
      case 'COURSE_ENROLLMENT_CREATED':
        console.log('📞 [KafkaConsumer] Calling courseHandler.handleCourseEnrollmentCreated...');
        return this.courseHandler.handleCourseEnrollmentCreated(data);
      case 'COURSE_STATUS_UPDATED':
        console.log('📞 [KafkaConsumer] Calling courseHandler.handleUserCourseUpdate...');
        return this.courseHandler.handleUserCourseUpdate(data);
      default:
        console.log(`⚠️ [KafkaConsumer] Unhandled course eventType: ${eventType}`);
        this.logger.warn(`Unhandled course eventType: ${eventType}`);
    }
  }
  
  private async handleContentEvent(eventType: string, data: any) {
    console.log(`🎯 [KafkaConsumer] handleContentEvent called with eventType: ${eventType}`);
    console.log(`🎯 [KafkaConsumer] Content event data:`, JSON.stringify(data, null, 2));
    
    this.logger.log(`Handling content-event type: ${eventType}`);
    switch (eventType) {
      case 'CONTENT_TRACKING_CREATED':
        console.log('📞 [KafkaConsumer] Calling contentHandler.handleContentTrackingCreated...');
        return this.contentHandler.handleContentTrackingCreated(data);
      default:
        console.log(`⚠️ [KafkaConsumer] Unhandled content eventType: ${eventType}`);
        this.logger.warn(`Unhandled content eventType: ${eventType}`);
    }
  }
}
