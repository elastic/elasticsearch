/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Map;
import java.util.Objects;

class ChunkedDataExtractorContext {

    interface TimeAligner {
        long alignToFloor(long value);
        long alignToCeil(long value);
    }

    final String jobId;
    final String timeField;
    final String[] indices;
    final QueryBuilder query;
    final int scrollSize;
    final long start;
    final long end;
    final TimeValue chunkSpan;
    final TimeAligner timeAligner;
    final Map<String, String> headers;
    final boolean hasAggregations;
    final Long histogramInterval;
    final IndicesOptions indicesOptions;

    ChunkedDataExtractorContext(String jobId, String timeField, List<String> indices, QueryBuilder query, int scrollSize, long start,
                                long end, @Nullable TimeValue chunkSpan, TimeAligner timeAligner, Map<String, String> headers,
                                boolean hasAggregations, @Nullable Long histogramInterval, IndicesOptions indicesOptions) {
        this.jobId = Objects.requireNonNull(jobId);
        this.timeField = Objects.requireNonNull(timeField);
        this.indices = indices.toArray(new String[indices.size()]);
        this.query = Objects.requireNonNull(query);
        this.scrollSize = scrollSize;
        this.start = start;
        this.end = end;
        this.chunkSpan = chunkSpan;
        this.timeAligner = Objects.requireNonNull(timeAligner);
        this.headers = headers;
        this.hasAggregations = hasAggregations;
        this.histogramInterval = histogramInterval;
        this.indicesOptions = Objects.requireNonNull(indicesOptions);
    }
}
