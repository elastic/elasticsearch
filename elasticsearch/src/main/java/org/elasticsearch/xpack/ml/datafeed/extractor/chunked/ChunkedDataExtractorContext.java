/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.datafeed.extractor.chunked;

import org.elasticsearch.common.inject.internal.Nullable;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.Objects;

class ChunkedDataExtractorContext {

    final String jobId;
    final String timeField;
    final String[] indexes;
    final String[] types;
    final QueryBuilder query;
    final int scrollSize;
    final long start;
    final long end;
    final Long chunkSpan;

    public ChunkedDataExtractorContext(String jobId, String timeField, List<String> indexes, List<String> types,
                                       QueryBuilder query, int scrollSize, long start, long end, @Nullable Long chunkSpan) {
        this.jobId = Objects.requireNonNull(jobId);
        this.timeField = Objects.requireNonNull(timeField);
        this.indexes = indexes.toArray(new String[indexes.size()]);
        this.types = types.toArray(new String[types.size()]);
        this.query = Objects.requireNonNull(query);
        this.scrollSize = scrollSize;
        this.start = start;
        this.end = end;
        this.chunkSpan = chunkSpan;
    }
}
