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
 * ungrouped aggregate sits above it, which is exactly what stops {@link SchemaDiscoveryPathExtractor} calling it
 * schema discovery. One value rather than two sets keeps that a property of the type.
 */
public enum ResolutionDemand {

    /**
     * No rows are read from this path, so resolution owes it a schema and nothing else. It bounds a listing
     * wherever the dataset's mode allows one, which {@link #ROWS} now does too: what a schema costs is the
     * dataset's business, and a query that discards every row asks no less of it than one that reads five.
     */
    SCHEMA_DISCOVERY,

    /**
     * Rows are read. Resolution still lists only as far as the schema needs, and the files this query reads are
     * discovered by split discovery, which lists the dataset itself when what resolution held was a prefix.
     */
    ROWS,

    /**
     * An ungrouped aggregate answerable from file metadata: resolution reads footers up front and split
     * discovery is skipped. The footers it reads are ones a later phase would have read anyway.
     * <p>
     * Elected from the query's shape alone, so it is asked of formats that cannot answer it. The gather
     * then stops once a read would feed neither the fold nor the schema cache; see
     * {@code remainingReadsBuyNothing}.
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
        return readingNoRows != null && readingNoRows.contains(path) ? SCHEMA_DISCOVERY : ROWS;
    }

    /** Whether resolution must eagerly aggregate global statistics across every file. */
    public boolean requiresStats() {
        return this == EAGER_STATS;
    }

    /**
     * Whether the query discards every row from this path. No longer decides how far a listing runs - the
     * dataset's mode does that - and its one remaining caller is the declared rail's coercibility check, which a
     * query reading no rows never performs the cast for.
     */
    public boolean isSchemaDiscovery() {
        return this == SCHEMA_DISCOVERY;
    }
}
