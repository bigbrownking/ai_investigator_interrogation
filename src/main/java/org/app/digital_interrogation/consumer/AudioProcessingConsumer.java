package org.app.digital_interrogation.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.app.digital_interrogation.config.RabbitMQConfig;
import org.app.digital_interrogation.dto.AudioProcessingMessage;
import org.app.digital_interrogation.service.AudioProcessingService;
import org.app.digital_interrogation.service.MinioService;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

import java.io.InputStream;

@Slf4j
@Component
@RequiredArgsConstructor
public class AudioProcessingConsumer {

    private final MinioService minioService;
    private final AudioProcessingService audioProcessingService;

    @RabbitListener(queues = "${spring.rabbitmq.inter.queue}")
    public void processAudio(AudioProcessingMessage message) {
        log.info("📥 Received audio for processing: {} (ID: {}) from case {} uploaded by {}",
                message.getAudioFileUrl(),
                message.getInterrogationId(),
                message.getCaseNumber(),
                message.getEmail());

        try {
            log.info("📂 Downloading file from MinIO: {}", message.getAudioFileUrl());
            InputStream fileStream = minioService.downloadFile(message.getAudioFileUrl());

            log.info("🎙️ Starting transcription for interrogation: {} in case {}",
                    message.getInterrogationId(), message.getCaseNumber());

            audioProcessingService.processAudio(
                    fileStream,
                    message.getAudioFileUrl(),
                    message.getLanguage(),
                    message
            );

            log.info("✅ Transcription initiated successfully for interrogation {} in case {}",
                    message.getInterrogationId(), message.getCaseNumber());

        } catch (Exception e) {
            log.error("❌ Failed to initiate transcription for interrogation {} in case {}: {}",
                    message.getInterrogationId(),
                    message.getCaseNumber(),
                    e.getMessage(),
                    e);

            audioProcessingService.notifyFailure(message, e.getMessage(), 0);
        }
    }
}