/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.attachment;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.telemetry.metric.DoubleHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

/**
 * Telemetry for the ingest attachment processor.
 * <p>
 * Attachment sizes are recorded on double histograms in fractional mebibytes rather than as raw byte counts.
 * The APM Java agent applies default OpenTelemetry histogram bucket boundaries whose largest finite value is 131072.
 * If sizes were in bytes, values above 128 KiB collapsed into that overflow bucket. Recording mebibytes instead reuses
 * those same default boundaries (see
 * <a href="https://github.com/elastic/apm/blob/main/specs/agents/metrics-otel.md">APM OpenTelemetry metrics</a>)
 * over a much wider attachment size range: from 0.00390625 MiB (4 KiB) up to 131072 MiB (128 GiB).
 */
public final class AttachmentIngestMetrics {

    /**
     * Raw size of the attachment source field (before base64 decode) observed before the max-field size check runs.
     */
    public static final String RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED = "es.ingest.attachment.raw_field_size_in_mebibytes.received.histogram";

    /**
     * Raw size of the attachment source field (before base64 decode) for attachments that were processed.
     */
    public static final String RAW_FIELD_SIZE_IN_MEBIBYTES_PROCESSED =
        "es.ingest.attachment.raw_field_size_in_mebibytes.processed.histogram";

    private final DoubleHistogram rawMebibytesReceived;
    private final DoubleHistogram rawMebibytesProcessed;

    public AttachmentIngestMetrics(MeterRegistry meterRegistry) {
        this.rawMebibytesReceived = meterRegistry.registerDoubleHistogram(
            RAW_FIELD_SIZE_IN_MEBIBYTES_RECEIVED,
            "Raw attachment field sizes in mebibytes before max-field size checks.",
            "MiBy"
        );
        this.rawMebibytesProcessed = meterRegistry.registerDoubleHistogram(
            RAW_FIELD_SIZE_IN_MEBIBYTES_PROCESSED,
            "Raw attachment field sizes in mebibytes that completed attachment processing.",
            "MiBy"
        );
    }

    public void recordRawBytesReceived(long rawBytes) {
        rawMebibytesReceived.record(toMebibytes(rawBytes));
    }

    public void recordRawBytesProcessed(long rawBytes) {
        rawMebibytesProcessed.record(toMebibytes(rawBytes));
    }

    static double toMebibytes(long rawBytes) {
        return ByteSizeValue.ofBytes(rawBytes).getMbFrac();
    }

}
