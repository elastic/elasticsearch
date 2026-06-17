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
 */
public final class AttachmentIngestMetrics {

    /**
     * Raw size of the attachment source field (before base64 decode) observed before the max-field size check runs.
     */
    public static final String RAW_FIELD_SIZE_IN_MEGABYTES_RECEIVED =
        "es.ingest.attachment.raw_field_size_in_megabytes.received.histogram";

    /**
     * Raw size of the attachment source field (before base64 decode) for attachments that were processed.
     */
    public static final String RAW_FIELD_SIZE_IN_MEGABYTES_PROCESSED =
        "es.ingest.attachment.raw_field_size_in_megabytes.processed.histogram";

    private final DoubleHistogram rawMegabytesReceived;
    private final DoubleHistogram rawMegabytesProcessed;

    public AttachmentIngestMetrics(MeterRegistry meterRegistry) {
        this.rawMegabytesReceived = meterRegistry.registerDoubleHistogram(
            RAW_FIELD_SIZE_IN_MEGABYTES_RECEIVED,
            "Raw attachment field sizes in mebibytes before max-field size checks.",
            "megabytes"
        );
        this.rawMegabytesProcessed = meterRegistry.registerDoubleHistogram(
            RAW_FIELD_SIZE_IN_MEGABYTES_PROCESSED,
            "Raw attachment field sizes in mebibytes that completed attachment processing.",
            "megabytes"
        );
    }

    public void recordRawBytesReceived(long rawBytes) {
        rawMegabytesReceived.record(toMegabytes(rawBytes));
    }

    public void recordRawBytesProcessed(long rawBytes) {
        rawMegabytesProcessed.record(toMegabytes(rawBytes));
    }

    static double toMegabytes(long rawBytes) {
        return ByteSizeValue.ofBytes(rawBytes).getMbFrac();
    }

}
