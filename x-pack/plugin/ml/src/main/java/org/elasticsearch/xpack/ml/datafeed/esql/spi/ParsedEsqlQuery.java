/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.datafeed.esql.spi;

import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;

/**
 * A parsed ES|QL datafeed query. Top-level means the commands after the source, up to a {@code FORK}; subqueries and branches are
 * not considered. Names are as written: nothing is checked against mappings and {@code EVAL} or {@code RENAME} are not followed.
 */
public interface ParsedEsqlQuery {

    /** Whether the query reads indices with {@code FROM} or {@code TS}, as opposed to {@code ROW} or {@code SHOW}. */
    boolean readsIndices();

    /** Index patterns the query reads, one pattern per entry, including subqueries. Lookup indices are not included. */
    Set<String> indexPatterns();

    /** The value of {@code SET name=value;}, as text. */
    Optional<String> setting(String name);

    /** Whether the top-level pipeline has a {@code STATS}. */
    boolean aggregates();

    /** The smallest {@code LIMIT} that caps the output rows: one after the last {@code STATS}, or any without {@code STATS}. */
    OptionalInt outputLimit();

    /** The smallest {@code LIMIT} that runs before the first {@code STATS}, so it restricts the aggregated source rows. */
    OptionalInt limitBeforeAggregation();

    /** Whether a top-level {@code WHERE} reads {@code field}. */
    boolean filtersOn(String field);

    /** Whether a top-level {@code SORT} reads {@code field}. */
    boolean sortsOn(String field);

    /** How the top-most {@code STATS} of the top-level pipeline buckets time. */
    TimeBucketing timeBucketing();

    /** The source field that the output column {@code outputColumn} carries unchanged, as traced through EVAL, RENAME, KEEP and DROP. */
    Optional<String> sourceFieldOf(String outputColumn);

    /** The query followed by {@code pipeline}, for example {@code SORT ??timeField ASC}. Pass it without a leading pipe. */
    String withPipeline(String pipeline);

    /** The {@code SET} statements and source command, then {@code pipeline}. Needs {@link #readsIndices}; no METADATA with subqueries. */
    String sourceWithPipeline(String pipeline);
}
