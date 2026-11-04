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
     * Injected row cap when the user query has no outer LIMIT; chunking truncation checks must use this value,
     * not {@code esql.query.result_truncation_default_size}.
     */
    static final long INJECTED_ROW_LIMIT = 10_000L;

    private static final String DEFAULT_LIMIT = " | LIMIT " + INJECTED_ROW_LIMIT;
    private static final String TIME_SORT = " | SORT ??timeField ASC";
    private static final String SOURCE_RANGE_SUMMARY_STATS =
        " | STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)";
    private static final String PROBE_OUTPUT_ROWS_STATS = " | STATS probe_output_rows = COUNT(*)";

    private EsqlDataExtractorQueryUtils() {}

    static QueryBuilder timeRangeFilter(String timeField, long start, long end) {
        return new RangeQueryBuilder(timeField).gte(start).lt(end).format(EPOCH_MILLIS);
    }

    /**
     * Appends {@code SORT} on the time field and, when missing, {@link #INJECTED_ROW_LIMIT} after sort so capped
     * chunks keep the earliest rows of the interval.
     */
    static String buildOrderedQuery(String query) {
        String sorted = EsqlQueryClauseScanner.appendGeneratedPipeline(query, TIME_SORT);
        return EsqlQueryClauseScanner.hasOuterLimit(query) ? sorted : sorted + DEFAULT_LIMIT;
    }

    static String sourceRangeSummaryQuery(String esqlQuery) {
        return EsqlQueryClauseScanner.appendGeneratedPipeline(
            EsqlQueryClauseScanner.extractLeadingCommand(esqlQuery).stripTrailing(),
            SOURCE_RANGE_SUMMARY_STATS
        );
    }

    static String boundedProbeQuery(String esqlQuery) {
        return EsqlQueryClauseScanner.appendGeneratedPipeline(esqlQuery, PROBE_OUTPUT_ROWS_STATS);
    }
}
