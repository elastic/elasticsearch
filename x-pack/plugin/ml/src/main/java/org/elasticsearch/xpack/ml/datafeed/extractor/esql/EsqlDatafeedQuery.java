/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.extractor.esql;

import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryAnalyzer;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.EsqlQueryStructure;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.PipelineStep;
import org.elasticsearch.xpack.ml.datafeed.esql.spi.PipelineStep.Kind;

import java.util.List;
import java.util.Objects;

/**
 * An ES|QL datafeed query with its parsed structure: the datafeed rules that depend on the query shape, and the query texts ML runs.
 * Generated pipelines start on a new line so a trailing {@code //} comment in the user query cannot swallow them.
 */
public final class EsqlDatafeedQuery {

    /**
     * Row cap appended when no user LIMIT caps the output. Truncation detection assumes the server returns up to this many rows;
     * lowering {@code esql.query.result_truncation_max_size} (dynamic, default 10_000) below this value defeats detection.
     */
    public static final long INJECTED_ROW_LIMIT = 10_000L;

    private static final String TIME_SORT = "\n| SORT ??timeField ASC";
    private static final String ROW_LIMIT = "\n| LIMIT " + INJECTED_ROW_LIMIT;
    private static final String SOURCE_RANGE_SUMMARY_STATS =
        "\n| STATS earliest_time = MIN(??timeField), latest_time = MAX(??timeField), total_hits = COUNT(*)";
    private static final String OUTPUT_ROWS_STATS = "\n| STATS probe_output_rows = COUNT(*)";
    private static final String KEEP_SOURCE_TIME_FIELD_LIMIT_ZERO = "\n| KEEP ??sourceTimeField | LIMIT 0";
    private static final String LIMIT_ZERO = "\n| LIMIT 0";

    private final String query;
    private final EsqlQueryStructure structure;

    EsqlDatafeedQuery(String query, EsqlQueryStructure structure) {
        this.query = Objects.requireNonNull(query);
        this.structure = Objects.requireNonNull(structure);
    }

    /** Parses {@code query}; throws a 400 if it is not valid ES|QL or does not read from indices with FROM or TS. */
    public static EsqlDatafeedQuery parse(EsqlQueryAnalyzer analyzer, String query) {
        EsqlQueryStructure structure = analyzer.analyze(query);
        if (structure.sourceKind() == EsqlQueryStructure.SourceKind.OTHER) {
            // Source probes and the extractor time filter need an index source.
            throw ExceptionsHelper.badRequestException(Messages.getMessage(Messages.DATAFEED_ESQL_SOURCE_COMMAND_REQUIRED));
        }
        return new EsqlDatafeedQuery(query, structure);
    }

    /** {@code query | LIMIT 0}: resolves the output columns without reading rows. */
    static String limitZeroQuery(String query) {
        return query + LIMIT_ZERO;
    }

    String query() {
        return query;
    }

    EsqlQueryStructure structure() {
        return structure;
    }

    /** Whether the top-level pipeline aggregates, so source document count is a poor proxy for output rows. */
    public boolean hasAggregation() {
        return steps().stream().anyMatch(step -> step.kind() == Kind.STATS);
    }

    /** Whether {@link #orderedQuery} appends {@link #INJECTED_ROW_LIMIT}: no user LIMIT caps the output rows. */
    public boolean injectsRowLimit() {
        for (PipelineStep step : steps()) {
            if (step.kind() == Kind.LIMIT) {
                return false;
            }
            if (step.kind() == Kind.STATS) {
                return true;
            }
        }
        return true;
    }

    /** Whether a LIMIT runs before the first STATS, so each extraction window aggregates only some of its source rows. */
    boolean hasLimitBeforeAggregation() {
        List<PipelineStep> steps = steps();
        // Steps are listed last command first, so the first STATS of the query is the last STATS step.
        int firstStats = -1;
        for (int i = 0; i < steps.size(); i++) {
            if (steps.get(i).kind() == Kind.STATS) {
                firstStats = i;
            }
        }
        return firstStats >= 0 && steps.subList(firstStats + 1, steps.size()).stream().anyMatch(step -> step.kind() == Kind.LIMIT);
    }

    boolean hasOuterLimit() {
        return steps().stream().anyMatch(step -> step.kind() == Kind.LIMIT);
    }

    boolean hasOuterTimeFilter(String timeField) {
        return referencesField(Kind.WHERE, timeField);
    }

    boolean hasOuterTimeSort(String timeField) {
        return referencesField(Kind.SORT, timeField);
    }

    /** The user query sorted on the job time field and, when {@link #injectsRowLimit}, capped after the sort. */
    String orderedQuery() {
        String sorted = query + TIME_SORT;
        return injectsRowLimit() ? sorted + ROW_LIMIT : sorted;
    }

    /** MIN/MAX/COUNT over the source command only, so the estimate does not depend on the user pipeline. */
    String sourceRangeSummaryQuery() {
        return sourceQuery() + SOURCE_RANGE_SUMMARY_STATS;
    }

    /** Counts the output rows of the full user pipeline. */
    String outputRowsProbeQuery() {
        return query + OUTPUT_ROWS_STATS;
    }

    /** Resolves {@code ??sourceTimeField} against the source command, as the extractor time filter does. */
    String sourceTimeFieldProbeQuery() {
        return sourceQuery() + KEEP_SOURCE_TIME_FIELD_LIMIT_ZERO;
    }

    private String sourceQuery() {
        String settingsText = structure.settingsText();
        return settingsText.isEmpty() ? structure.sourceCommand() : settingsText + "\n" + structure.sourceCommand();
    }

    private List<PipelineStep> steps() {
        return structure.outerPipeline();
    }

    private boolean referencesField(Kind kind, String field) {
        return steps().stream().anyMatch(step -> step.kind() == kind && step.referencedFields().contains(field));
    }
}
