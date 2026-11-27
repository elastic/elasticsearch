/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

/**
 * Query shaping helpers for {@link EsqlDataExtractor}.
 */
final class EsqlDataExtractorQueryUtils {

    private static final String EPOCH_MILLIS = "epoch_millis";

    /**
     * Row cap appended to every extraction query. Truncation detection assumes the server returns up to this many rows;
     * lowering {@code esql.query.result_truncation_max_size} (dynamic, default 10_000) below this value defeats detection.
     */
    static final long INJECTED_ROW_LIMIT = 10_000L;

    private static final String TIME_SORT = "SORT ??timeField ASC";
    private static final String ROW_LIMIT = "LIMIT " + INJECTED_ROW_LIMIT;
    private static final String SUMMARY_STATS =
        "STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)";

    private EsqlDataExtractorQueryUtils() {}

    static QueryBuilder timeRangeFilter(String timeField, long start, long end) {
        return new RangeQueryBuilder(timeField).gte(start).lt(end).format(EPOCH_MILLIS);
    }

    /**
     * Appends a pipe to a user query. The newline ends a trailing {@code //} comment, so the appended command is never swallowed
     * by it. ML does not parse the user query; it only appends to it.
     */
    static String appendPipe(String query, String pipe) {
        return query + "\n| " + pipe;
    }

    /**
     * Appends {@code SORT} on the time field, then {@link #INJECTED_ROW_LIMIT}. A smaller user {@code LIMIT} still wins, because
     * it comes earlier in the pipeline; a larger one is capped so truncation is detectable. Capped chunks keep the earliest rows.
     * <p>
     * Known limitation: a user {@code LIMIT} below {@link #INJECTED_ROW_LIMIT} caps each chunk before the {@code SORT}, so it keeps
     * arbitrary rows and the chunk is never flagged as truncated. It also makes the summary row count too small, which makes chunks
     * larger than intended. ML cannot detect a user {@code LIMIT} without parsing the query, so users must not put an outer
     * {@code LIMIT} in an ES|QL datafeed query.
     */
    static String buildOrderedQuery(String query) {
        return appendPipe(appendPipe(query, TIME_SORT), ROW_LIMIT);
    }

    /** One-row MIN/MAX/COUNT over the output of the user query; {@code ??timeField} is the emitted time column. */
    static String summaryQuery(String query) {
        return appendPipe(query, SUMMARY_STATS);
    }
}
