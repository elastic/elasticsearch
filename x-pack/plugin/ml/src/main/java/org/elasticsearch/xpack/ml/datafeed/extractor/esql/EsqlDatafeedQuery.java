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
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryWithParams;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.ParsedEsqlQuery;

import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.OptionalLong;

/** An ES|QL datafeed query with its parsed form: the datafeed rules that depend on the query shape, and the query texts ML runs. */
public final class EsqlDatafeedQuery {

    /**
     * Row cap appended when no user LIMIT caps the output rows. Truncation detection assumes the server returns up to this many rows;
     * lowering {@code esql.query.result_truncation_max_size} (dynamic, default 10_000) below this value defeats detection.
     */
    public static final long INJECTED_ROW_LIMIT = 10_000L;

    private static final String LIMIT_ZERO = "LIMIT 0";

    private final String query;
    private final ParsedEsqlQuery parsed;

    EsqlDatafeedQuery(String query, ParsedEsqlQuery parsed) {
        this.query = Objects.requireNonNull(query);
        this.parsed = Objects.requireNonNull(parsed);
    }

    /** Parses {@code query}; throws a 400 if it is not valid ES|QL, uses an unsupported construct, or has no index source. */
    public static EsqlDatafeedQuery parse(EsqlQueryParser parser, String query) {
        ParsedEsqlQuery parsed = parser.parse(query);
        // Checked here, at the boundary every execution path parses through, so a saved or inline query cannot skip them.
        if (parsed.setsProjectRoutingInQuery()) {
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_ESQL_PROJECT_ROUTING_SETTING_NOT_SUPPORTED));
        }
        ZoneId timeZone = parsed.timeZone();
        if (timeZone.normalized().equals(ZoneOffset.UTC) == false) {
            // ES|QL honors the last SET, so the UTC ML prepends to every request would lose to this one.
            throw ExceptionsHelper.badRequestException(
                Messages.getMessage(Messages.DATAFEED_ESQL_TIME_ZONE_SETTING_NOT_UTC, timeZone.getId())
            );
        }
        if (parsed.indexPatterns().isEmpty()) {
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

    /** Whether the query aggregates, so source document count is a poor proxy for output rows. */
    public boolean hasAggregation() {
        return parsed.isAggregating();
    }

    /** Whether {@link #orderedQuery} appends {@link #INJECTED_ROW_LIMIT}: no user LIMIT caps the output rows. */
    public boolean injectsRowLimit() {
        return parsed.outputLimit().isEmpty();
    }

    /** The user LIMIT that caps the output rows, if any. */
    OptionalInt outputLimit() {
        return parsed.outputLimit();
    }

    /** {@code query | LIMIT 0}: resolves the output columns without reading rows. Text-level so it also works before parsing. */
    static String limitZeroQuery(String query) {
        return query + "\n| " + LIMIT_ZERO;
    }

    /** The user query sorted on {@code timeField} and, when {@link #injectsRowLimit}, capped after the sort. */
    EsqlQueryWithParams orderedQuery(String timeField) {
        return parsed.orderedByTimeQuery(timeField, injectsRowLimit() ? OptionalLong.of(INJECTED_ROW_LIMIT) : OptionalLong.empty());
    }

    /** MIN/MAX/COUNT over the source command only, so the estimate does not depend on the user pipeline. */
    EsqlQueryWithParams sourceRangeSummaryQuery(String sourceTimeField) {
        return parsed.sourceRangeSummaryQuery(sourceTimeField);
    }

    /** Counts the output rows of the full user pipeline. */
    EsqlQueryWithParams outputRowsProbeQuery() {
        return parsed.outputRowCountQuery();
    }

    /** Resolves {@code sourceTimeField} against the source command, as the extractor time filter does. */
    EsqlQueryWithParams sourceTimeFieldProbeQuery(String sourceTimeField) {
        return parsed.sourceTimeFieldProbeQuery(sourceTimeField);
    }
}
