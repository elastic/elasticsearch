/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * Telemetry for the ingest attachment processor.
 */
public final class AttachmentIngestMetrics {

    /**
     * Raw size of the attachment source field (before base64 decode) observed before the max-field size check runs.
     */
    public static final String RAW_FIELD_BYTES_RECEIVED = "es.ingest.attachment.raw_field_bytes.received.histogram";

    /**
     * Raw size of the attachment source field (before base64 decode) for attachments that were processed.
     */
    public static final String RAW_FIELD_BYTES_PROCESSED = "es.ingest.attachment.raw_field_bytes.processed.histogram";

    private final LongHistogram rawBytesReceived;
    private final LongHistogram rawBytesProcessed;

    public AttachmentIngestMetrics(MeterRegistry meterRegistry) {
        this.rawBytesReceived = meterRegistry.registerLongHistogram(
            RAW_FIELD_BYTES_RECEIVED,
            "Raw attachment field sizes in bytes before max-field size checks.",
            "bytes"
        );
        this.rawBytesProcessed = meterRegistry.registerLongHistogram(
            RAW_FIELD_BYTES_PROCESSED,
            "Raw attachment field sizes in bytes that completed attachment processing.",
            "bytes"
        );
    }

    public void recordRawBytesReceived(long rawBytes) {
        rawBytesReceived.record(rawBytes, Map.of());
    }

    public void recordRawBytesProcessed(long rawBytes) {
        rawBytesProcessed.record(rawBytes, Map.of());
    }

}
