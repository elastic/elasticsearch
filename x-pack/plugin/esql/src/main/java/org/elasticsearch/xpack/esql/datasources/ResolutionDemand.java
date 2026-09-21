/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.Set;

/**
 * What a query needs from one path's resolution: how much of its glob must be listed and how many of its files
 * read. Ordered by how much of the dataset each state touches.
 * <p>
 * The states are exclusive by construction: {@link ExternalStatsRequirementExtractor} selects a path only when an
 * ungrouped aggregate sits above it, which is exactly what stops {@link SchemaOnlyPathExtractor} calling it
 * schema-only. One value rather than two sets keeps that a property of the type.
 */
public enum ResolutionDemand {

    /**
     * No rows are read from this path, so resolution owes it a schema and nothing else. The only state that may
     * bound a listing; how far the bound goes is the dataset's resolution mode, not the query's.
     */
    SCHEMA_ONLY,

    /** Rows are read. Resolution produces the full file set, because split discovery takes it from the plan. */
    ROWS,

    /**
     * An ungrouped aggregate answerable from file metadata: resolution reads every footer up front and split
     * discovery is skipped. The footers it reads are ones a later phase would have read anyway.
     */
    EAGER_STATS;

    /**
     * @param requiringStats paths under an ungrouped aggregate, or {@code null} to resolve every path eagerly
     * @param readingNoRows  paths whose rows are all discarded, or {@code null} if the shape was not examined
     */
    public static ResolutionDemand of(String path, Set<String> requiringStats, Set<String> readingNoRows) {
        if (requiringStats == null || requiringStats.contains(path)) {
            return EAGER_STATS;
        }
        return readingNoRows != null && readingNoRows.contains(path) ? SCHEMA_ONLY : ROWS;
    }

    /** Whether resolution must eagerly aggregate global statistics across every file. */
    public boolean requiresStats() {
        return this == EAGER_STATS;
    }

    /** Whether resolution may stop listing once it has what the schema needs. */
    public boolean schemaOnly() {
        return this == SCHEMA_ONLY;
    }
}
