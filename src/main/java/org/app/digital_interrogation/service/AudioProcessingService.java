package org.app.digital_interrogation.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.app.digital_interrogation.config.RabbitMQConfig;
import org.app.digital_interrogation.dto.AudioProcessingMessage;
import org.app.digital_interrogation.dto.TranscriptionResultMessage;
import org.app.digital_interrogation.dto.TranscriptionStatus;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.ByteArrayResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.reactive.function.client.WebClient;

import java.io.InputStream;
import java.time.LocalDateTime;

@Slf4j
@Service
@RequiredArgsConstructor
public class AudioProcessingService {

    private final RabbitTemplate rabbitTemplate;
    private final WebClient webClient;

    @Value("${audio.model.url}")
    private String aiModelUrl;

    @Value("${audio.model.port}")
    private String port;

    public void processAudio(InputStream fileStream, String fileName,
                             String caseNumber, AudioProcessingMessage originalMessage) {

        notifyProcessing(originalMessage);

        try {
            byte[] fileBytes = fileStream.readAllBytes();

            processAudioAsync(fileBytes, fileName, caseNumber, originalMessage);

            log.info("Transcription task submitted for file {} (ID: {}) in case {}",
                    fileName, originalMessage.getInterrogationId(), caseNumber);

        } catch (Exception e) {
            log.error("Failed to submit transcription task for file {} (ID: {}) in case {}: {}",
                    fileName, originalMessage.getInterrogationId(), caseNumber, e.getMessage());
            notifyFailure(originalMessage, e.getMessage(), 0);
        }
    }

    @Async("audioProcessingExecutor")
    public void processAudioAsync(byte[] fileBytes, String fileName,
                                  String caseNumber, AudioProcessingMessage originalMessage) {
        long startTime = System.currentTimeMillis();

        try {
            log.info("Transcription started for file {} (ID: {}) in case {}",
                    fileName, originalMessage.getInterrogationId(), caseNumber);

            String result = webClient.post()
                    .uri(aiModelUrl + ":" + port + "/transcribe")
                    .contentType(MediaType.MULTIPART_FORM_DATA)
                    .bodyValue(createMultipartBody(fileBytes, fileName))
                    .retrieve()
                    .bodyToMono(String.class)
                    .block();

            long duration = (System.currentTimeMillis() - startTime) / 1000;
            log.info("Transcription completed for file {} (ID: {}) in case {} after {}s",
                    fileName, originalMessage.getInterrogationId(), caseNumber, duration);

            notifyCompletion(originalMessage, result, duration);

        } catch (Exception e) {
            long duration = (System.currentTimeMillis() - startTime) / 1000;
            log.error("Transcription failed for file {} (ID: {}) in case {} after {}s: {}",
                    fileName, originalMessage.getInterrogationId(), caseNumber, duration, e.getMessage());

            notifyFailure(originalMessage, e.getMessage(), duration);
        }
    }

    private MultiValueMap<String, Object> createMultipartBody(byte[] fileBytes, String fileName) {
        MultiValueMap<String, Object> body = new LinkedMultiValueMap<>();

        ByteArrayResource fileResource = new ByteArrayResource(fileBytes) {
            @Override
            public String getFilename() {
                return fileName;
            }
        };

        HttpHeaders fileHeaders = new HttpHeaders();
        fileHeaders.setContentType(MediaType.parseMediaType("audio/wav"));
        body.add("data", new HttpEntity<>(fileResource, fileHeaders));
        body.add("language", "");

        return body;
    }

    public void notifyProcessing(AudioProcessingMessage originalMessage) {
        sendNotification(TranscriptionResultMessage.builder()
                .interrogationId(originalMessage.getInterrogationId())
                .qaId(originalMessage.getQaId())
                .caseNumber(originalMessage.getCaseNumber())
                .email(originalMessage.getEmail())
                .status(TranscriptionStatus.PROCESSING)
                .transcribedText(null)
                .errorMessage(null)
                .timestamp(LocalDateTime.now())
                .build());

        log.info("Sent PROCESSING notification for file {} (ID: {}) in case {} from user {}",
                originalMessage.getOriginalFileName(),
                originalMessage.getInterrogationId(),
                originalMessage.getCaseNumber(),
                originalMessage.getEmail());
    }

    public void notifyCompletion(AudioProcessingMessage originalMessage,
                                 String transcribedText, long durationSeconds) {
        sendNotification(TranscriptionResultMessage.builder()
                .interrogationId(originalMessage.getInterrogationId())
                .qaId(originalMessage.getQaId())
                .caseNumber(originalMessage.getCaseNumber())
                .email(originalMessage.getEmail())
                .status(TranscriptionStatus.COMPLETED)
                .transcribedText(transcribedText)
                .errorMessage(null)
                .timestamp(LocalDateTime.now())
                .processingDurationSeconds(durationSeconds)
                .build());

        log.info("Sent COMPLETED notification for file {} (ID: {}) in case {} from user {} ({}s)",
                originalMessage.getOriginalFileName(),
                originalMessage.getInterrogationId(),
                originalMessage.getCaseNumber(),
                originalMessage.getEmail(),
                durationSeconds);
    }

    public void notifyFailure(AudioProcessingMessage originalMessage,
                              String errorMessage, long durationSeconds) {
        sendNotification(TranscriptionResultMessage.builder()
                .interrogationId(originalMessage.getInterrogationId())
                .qaId(originalMessage.getQaId())
                .caseNumber(originalMessage.getCaseNumber())
                .email(originalMessage.getEmail())
                .status(TranscriptionStatus.FAILED)
                .transcribedText(null)
                .errorMessage(errorMessage)
                .timestamp(LocalDateTime.now())
                .processingDurationSeconds(durationSeconds)
                .build());

        log.error("Sent FAILED notification for file {} (ID: {}) in case {} from user {} ({}s): {}",
                originalMessage.getOriginalFileName(),
                originalMessage.getInterrogationId(),
                originalMessage.getCaseNumber(),
                originalMessage.getEmail(),
                durationSeconds,
                errorMessage);
    }

    public void notifyPending(AudioProcessingMessage originalMessage) {
        sendNotification(TranscriptionResultMessage.builder()
                .interrogationId(originalMessage.getInterrogationId())
                .qaId(originalMessage.getQaId())
                .caseNumber(originalMessage.getCaseNumber())
                .email(originalMessage.getEmail())
                .status(TranscriptionStatus.PENDING)
                .transcribedText(null)
                .errorMessage(null)
                .timestamp(LocalDateTime.now())
                .build());

        log.info("Sent PENDING notification for file {} (ID: {}) in case {} from user {}",
                originalMessage.getOriginalFileName(),
                originalMessage.getInterrogationId(),
                originalMessage.getCaseNumber(),
                originalMessage.getEmail());
    }

    private void sendNotification(TranscriptionResultMessage message) {
        int maxRetries = 3;
        int retryCount = 0;

        while (retryCount < maxRetries) {
            try {
                rabbitTemplate.convertAndSend(
                        RabbitMQConfig.INTERROGATION_RESULT_EXCHANGE,
                        getRoutingKey(message.getStatus()),
                        message
                );

                log.debug("Successfully sent {} notification to exchange {} with routing key {}",
                        message.getStatus(),
                        RabbitMQConfig.INTERROGATION_RESULT_EXCHANGE,
                        getRoutingKey(message.getStatus()));

                return;

            } catch (Exception e) {
                retryCount++;
                log.error("Failed to send {} notification for interrogation {} in case {} (attempt {}/{}): {}",
                        message.getStatus(),
                        message.getInterrogationId(),
                        message.getCaseNumber(),
                        retryCount,
                        maxRetries,
                        e.getMessage());

                if (retryCount >= maxRetries) {
                    log.error("Failed to send notification after {} attempts. Message will be lost: case={}, interrogationId={}, status={}",
                            maxRetries,
                            message.getCaseNumber(),
                            message.getInterrogationId(),
                            message.getStatus());
                } else {
                    try {
                        Thread.sleep(1000L * retryCount);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        log.error("Retry sleep interrupted for interrogation {} in case {}",
                                message.getInterrogationId(), message.getCaseNumber());
                    }
                }
            }
        }
    }

    private String getRoutingKey(TranscriptionStatus status) {
        return switch (status) {
            case PENDING -> RabbitMQConfig.INTERROGATION_RESULT_PENDING_ROUTING_KEY;
            case PROCESSING -> RabbitMQConfig.INTERROGATION_RESULT_PROCESSING_ROUTING_KEY;
            case COMPLETED -> RabbitMQConfig.INTERROGATION_RESULT_SUCCESS_ROUTING_KEY;
            case FAILED -> RabbitMQConfig.INTERROGATION_RESULT_FAILURE_ROUTING_KEY;
        };
    }
}