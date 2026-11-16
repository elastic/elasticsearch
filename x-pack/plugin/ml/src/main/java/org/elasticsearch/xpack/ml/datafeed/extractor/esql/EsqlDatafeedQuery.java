/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryParser;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;

import java.util.Objects;
import java.util.OptionalInt;

/** An ES|QL datafeed query with its parsed form: the datafeed rules that depend on the query shape, and the query texts ML runs. */
public final class EsqlDatafeedQuery {

    /**
     * Row cap appended when no user LIMIT caps the output rows. Truncation detection assumes the server returns up to this many rows;
     * lowering {@code esql.query.result_truncation_max_size} (dynamic, default 10_000) below this value defeats detection.
     */
    public static final long INJECTED_ROW_LIMIT = 10_000L;

    private static final String TIME_SORT = "SORT ??timeField ASC";
    private static final String ROW_LIMIT = "\n| LIMIT " + INJECTED_ROW_LIMIT;
    private static final String SOURCE_RANGE_SUMMARY_STATS =
        "STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)";
    private static final String OUTPUT_ROWS_STATS = "STATS probe_output_rows = COUNT(*)";
    private static final String KEEP_SOURCE_TIME_FIELD_LIMIT_ZERO = "KEEP ??sourceTimeField | LIMIT 0";
    private static final String LIMIT_ZERO = "LIMIT 0";

    private final String query;
    private final ParsedEsqlQuery parsed;

    EsqlDatafeedQuery(String query, ParsedEsqlQuery parsed) {
        this.query = Objects.requireNonNull(query);
        this.parsed = Objects.requireNonNull(parsed);
    }

    /** Parses {@code query}; throws a 400 if it is not valid ES|QL or does not read from indices with FROM or TS. */
    public static EsqlDatafeedQuery parse(EsqlQueryParser parser, String query) {
        ParsedEsqlQuery parsed = parser.parse(query);
        if (parsed.readsIndices() == false) {
            // Source probes and the extractor time filter need an index source.
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_ESQL_SOURCE_COMMAND_REQUIRED));
        }
        return new EsqlDatafeedQuery(query, parsed);
    }

    /** The query text as the user wrote it. */
    public String query() {
        return query;
    }

    ParsedEsqlQuery parsed() {
        return parsed;
    }

    /** Whether the top-level pipeline aggregates, so source document count is a poor proxy for output rows. */
    public boolean hasAggregation() {
        return parsed.aggregates();
    }

    /** Whether {@link #orderedQuery} appends {@link #INJECTED_ROW_LIMIT}: no user LIMIT caps the output rows. */
    public boolean injectsRowLimit() {
        return parsed.outputLimit().isEmpty();
    }

    /** The user LIMIT that caps the output rows, if any. */
    OptionalInt outputLimit() {
        return parsed.outputLimit();
    }

    /** The user LIMIT that runs before the first STATS, so each extraction window aggregates only some of its source rows. */
    OptionalInt limitBeforeAggregation() {
        return parsed.limitBeforeAggregation();
    }

    boolean hasOuterTimeFilter(String timeField) {
        return parsed.filtersOn(timeField);
    }

    boolean hasOuterTimeSort(String timeField) {
        return parsed.sortsOn(timeField);
    }

    /** {@code query | LIMIT 0}: resolves the output columns without reading rows. Text-level so it also works before parsing. */
    static String limitZeroQuery(String query) {
        return query + "\n| " + LIMIT_ZERO;
    }

    /** The user query sorted on the job time field and, when {@link #injectsRowLimit}, capped after the sort. */
    String orderedQuery() {
        return parsed.withPipeline(injectsRowLimit() ? TIME_SORT + ROW_LIMIT : TIME_SORT);
    }

    /** MIN/MAX/COUNT over the source command only, so the estimate does not depend on the user pipeline. */
    String sourceRangeSummaryQuery() {
        return parsed.sourceWithPipeline(SOURCE_RANGE_SUMMARY_STATS);
    }

    /** Counts the output rows of the full user pipeline. */
    String outputRowsProbeQuery() {
        return parsed.withPipeline(OUTPUT_ROWS_STATS);
    }

    /** Resolves {@code ??sourceTimeField} against the source command, as the extractor time filter does. */
    String sourceTimeFieldProbeQuery() {
        return parsed.sourceWithPipeline(KEEP_SOURCE_TIME_FIELD_LIMIT_ZERO);
    }
}
