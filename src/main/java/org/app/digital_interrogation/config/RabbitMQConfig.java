package org.app.digital_interrogation.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {
    public static final String INTERROGATION_QUEUE = "interrogation.processing.queue";
    public static final String INTERROGATION_EXCHANGE = "interrogation.exchange";
    public static final String INTERROGATION_ROUTING_KEY = "interrogation.process";

    public static final String INTERROGATION_RESULT_EXCHANGE = "interrogation.result.exchange";

    // Routing keys по статусам
    public static final String INTERROGATION_RESULT_PENDING_ROUTING_KEY = "interrogation.result.pending";
    public static final String INTERROGATION_RESULT_PROCESSING_ROUTING_KEY = "interrogation.result.processing";
    public static final String INTERROGATION_RESULT_SUCCESS_ROUTING_KEY = "interrogation.result.transcribed";
    public static final String INTERROGATION_RESULT_FAILURE_ROUTING_KEY = "interrogation.result.failed";

    public static final String DLQ_QUEUE = "document.dlq";
    public static final String DLQ_EXCHANGE = "document.dlq.exchange";
    public static final String DLQ_ROUTING_KEY = "document.dlq";
    @Bean
    public Queue interrogationQueue() {
        return QueueBuilder.durable(INTERROGATION_QUEUE)
                .withArgument("x-dead-letter-exchange", DLQ_EXCHANGE)
                .withArgument("x-dead-letter-routing-key", DLQ_ROUTING_KEY)
                .build();
    }
    @Bean
    public DirectExchange interrogationExchange() {
        return new DirectExchange(INTERROGATION_EXCHANGE);
    }

    @Bean
    public Binding interrogationBinding(Queue interrogationQueue, DirectExchange interrogationExchange) {
        return BindingBuilder
                .bind(interrogationQueue)
                .to(interrogationExchange)
                .with(INTERROGATION_ROUTING_KEY);
    }

    @Bean
    public DirectExchange interrogationResultExchange() {
        return new DirectExchange(INTERROGATION_RESULT_EXCHANGE);
    }
    @Bean
    public Queue dlqQueue() {
        return QueueBuilder.durable(DLQ_QUEUE).build();
    }
    @Bean
    public DirectExchange dlqExchange() {
        return new DirectExchange(DLQ_EXCHANGE);
    }

    @Bean
    public Binding dlqBinding(Queue dlqQueue, DirectExchange dlqExchange) {
        return BindingBuilder
                .bind(dlqQueue)
                .to(dlqExchange)
                .with(DLQ_ROUTING_KEY);
    }
    @Bean
    @SuppressWarnings("deprecation")
    public MessageConverter jsonMessageConverter() {
        return new Jackson2JsonMessageConverter();
    }

    @Bean
    public RabbitTemplate rabbitTemplate(
            org.springframework.amqp.rabbit.connection.ConnectionFactory connectionFactory,
            MessageConverter jsonMessageConverter) {
        RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
        rabbitTemplate.setMessageConverter(jsonMessageConverter);
        return rabbitTemplate;
    }
}