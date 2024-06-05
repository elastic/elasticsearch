/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.core.TimeValue;

record ChunkedDataExtractorContext(
    String jobId,
    int scrollSize,
    long start,
    long end,
    TimeValue chunkSpan,
    TimeAligner timeAligner,
    boolean hasAggregations,
    Long histogramInterval
) {
    interface TimeAligner {
        long alignToFloor(long value);

        long alignToCeil(long value);
    }
}
