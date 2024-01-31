/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;

import java.util.Objects;

class ChunkedDataExtractorContext {

    interface TimeAligner {
        long alignToFloor(long value);

        long alignToCeil(long value);
    }

    final String jobId;
    final int scrollSize;
    final long start;
    final long end;
    final TimeValue chunkSpan;
    final TimeAligner timeAligner;
    final boolean hasAggregations;
    final Long histogramInterval;

    ChunkedDataExtractorContext(
        String jobId,
        int scrollSize,
        long start,
        long end,
        @Nullable TimeValue chunkSpan,
        TimeAligner timeAligner,
        boolean hasAggregations,
        @Nullable Long histogramInterval
    ) {
        this.jobId = Objects.requireNonNull(jobId);
        this.scrollSize = scrollSize;
        this.start = start;
        this.end = end;
        this.chunkSpan = chunkSpan;
        this.timeAligner = Objects.requireNonNull(timeAligner);
        this.hasAggregations = hasAggregations;
        this.histogramInterval = histogramInterval;
    }
}
