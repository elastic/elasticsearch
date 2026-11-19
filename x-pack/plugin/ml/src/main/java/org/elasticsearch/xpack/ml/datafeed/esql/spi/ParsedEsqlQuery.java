/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import org.elasticsearch.xpack.core.esql.action.ColumnInfo;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;

/**
 * A parsed ES|QL datafeed query. The commands are read as one pipeline: {@code FORK}, {@code FROM} with several subqueries,
 * {@code RENAME}, {@code MV_EXPAND}, {@code LOOKUP JOIN} and {@code LET} are rejected by the parser. Names are as written, nothing is
 * checked against mappings, and {@code EVAL} is not followed. No ES|QL text leaves the implementation: callers get finished queries.
 */
public interface ParsedEsqlQuery {

    /** Columns of {@link #sourceRangeSummaryQuery}. */
    String EARLIEST_TIME_COLUMN = "earliest_time";
    String LATEST_TIME_COLUMN = "latest_time";
    String TOTAL_HITS_COLUMN = "total_hits";

    /** Column of {@link #outputRowCountQuery}. */
    String OUTPUT_ROWS_COLUMN = "probe_output_rows";

    /** Index patterns the query reads, one pattern per entry; empty for a query without an index source such as {@code ROW}. */
    Set<String> indexPatterns();

    /** Whether the query has a {@code STATS}. */
    boolean isAggregating();

    /** The smallest {@code LIMIT} after the last {@code STATS}, or any without {@code STATS}: it caps the output rows. */
    OptionalInt outputLimit();

    /** Whether the query itself sets {@code project_routing}, as opposed to a cluster default. */
    boolean setsProjectRoutingInQuery();

    /** The time zone the query runs in: its own {@code SET}, else the cluster default, else UTC. */
    ZoneId timeZone();

    /** The query with no rows read: resolves the output columns. */
    EsqlQueryWithParams limitZeroQuery();

    /** The query sorted ascending on {@code timeField}, then capped to {@code rowLimit} when present. */
    EsqlQueryWithParams orderedByTimeQuery(String timeField, OptionalLong rowLimit);

    /**
     * One row with {@link #OUTPUT_ROWS_COLUMN}: the number of rows the whole query returns.
     *
     * @throws IllegalStateException for a {@code TS} query without {@code STATS}, which cannot be counted; check {@link #isAggregating}
     */
    EsqlQueryWithParams outputRowCountQuery();

    /** One row with the {@code *_COLUMN} constants above for {@code sourceTimeField}, over the source command only. Needs an index. */
    EsqlQueryWithParams sourceRangeSummaryQuery(String sourceTimeField);

    /** No rows, one column {@code sourceTimeField}, over the source command only: checks it resolves. Needs an index source. */
    EsqlQueryWithParams sourceTimeFieldProbeQuery(String sourceTimeField);

    /**
     * The query with {@code column_metadata} enabled. Run it with a request filter that matches no documents, not {@code LIMIT 0}:
     * ES|QL drops the metadata when the plan is empty. Pass the response columns to {@link #timeBucket}.
     */
    EsqlQueryWithParams columnMetadataProbeQuery();

    /**
     * The bucket span of {@code timeColumn} in the response to {@link #columnMetadataProbeQuery}, as reported by ES|QL itself rather than
     * read from the query. Empty when ES|QL reports none, as for {@code DATE_TRUNC} or after {@code RENAME}.
     */
    Optional<TimeBucket> timeBucket(List<? extends ColumnInfo> columns, String timeColumn);
}
