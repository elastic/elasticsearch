/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.document;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class BulkOperationWaitForChunkMetrics {
    public static final String CHUNK_WAIT_TIME_HISTOGRAM = "es.rest.wait.duration.histogram";

    /* Capture in milliseconds because the APM histogram only has a range of 100,000 */
    private final LongHistogram chunkWaitTimeMillisHistogram;

    public BulkOperationWaitForChunkMetrics(MeterRegistry meterRegistry) {
        this(meterRegistry.registerLongHistogram(CHUNK_WAIT_TIME_HISTOGRAM,
            "Total time in millis spent waiting for next chunk of a bulk request", "centis"));
    }

    private BulkOperationWaitForChunkMetrics(LongHistogram chunkWaitTimeMillisHistogram) {
        this.chunkWaitTimeMillisHistogram = chunkWaitTimeMillisHistogram;
    }

    public long recordTookTime(long tookTime) {
        chunkWaitTimeMillisHistogram.record(tookTime);
        return tookTime;
    }
}
