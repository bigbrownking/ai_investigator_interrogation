package org.app.digital_interrogation.dto;

import lombok.*;

import java.time.LocalDateTime;

@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class TranscriptionResultMessage {
    private Long interrogationId;
    private Long qaId;
    private Long caseId;
    private String caseNumber;
    private TranscriptionStatus status;
    private String transcribedText;
    private String errorMessage;
    private Long processingDurationSeconds;
    private String email;
    private LocalDateTime timestamp;
}
