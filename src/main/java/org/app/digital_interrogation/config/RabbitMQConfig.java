package org.app.digital_interrogation.config;

import org.springframework.amqp.core.*;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.Jackson2JsonMessageConverter;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    @Value("${spring.rabbitmq.inter.queue}")
    public String INTERROGATION_QUEUE = "interrogation.processing.queue";
    @Value("${spring.rabbitmq.inter.exchange}")
    public String INTERROGATION_EXCHANGE = "interrogation.exchange";
    @Value("${spring.rabbitmq.inter.routing-key}")
    public String INTERROGATION_ROUTING_KEY = "interrogation.process";

    @Value("${spring.rabbitmq.inter.result.exchange}")
    public String INTERROGATION_RESULT_EXCHANGE = "interrogation.result.exchange";

    // Routing keys по статусам
    @Value("${spring.rabbitmq.inter.result.pending.routing-key}")
    public String INTERROGATION_RESULT_PENDING_ROUTING_KEY = "interrogation.result.pending";
    @Value("${spring.rabbitmq.inter.result.processing.routing-key}")
    public String INTERROGATION_RESULT_PROCESSING_ROUTING_KEY = "interrogation.result.processing";

    @Value("${spring.rabbitmq.inter.result.success.routing-key}")
    public String INTERROGATION_RESULT_SUCCESS_ROUTING_KEY = "interrogation.result.transcribed";

    @Value("${spring.rabbitmq.inter.result.failure.routing-key}")
    public String INTERROGATION_RESULT_FAILURE_ROUTING_KEY = "interrogation.result.failed";

    @Value("${spring.rabbitmq.dlq.queue}")
    public String DLQ_QUEUE = "document.dlq";

    @Value("${spring.rabbitmq.dlq.exchange}")
    public String DLQ_EXCHANGE = "document.dlq.exchange";

    @Value("${spring.rabbitmq.dlq.routing-key}")
    public String DLQ_ROUTING_KEY = "document.dlq";
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