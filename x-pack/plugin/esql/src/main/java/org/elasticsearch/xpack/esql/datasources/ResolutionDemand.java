/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import java.util.Set;

/**
 * What a query needs from one path's resolution, which is what decides how much of that path's glob has to be
 * listed and how many of its files have to be read.
 * <p>
 * The three states are ordered by how much of the dataset each one has to touch, and they are exclusive by
 * construction rather than by convention: {@link ExternalStatsRequirementExtractor} selects a path only when an
 * ungrouped aggregate sits above it, and an aggregate above a relation is exactly what stops
 * {@link SchemaOnlyPathExtractor} calling that relation schema-only. Holding them as one value keeps that
 * exclusivity a property of the type instead of an agreement between two sets that happen to be passed together.
 */
public enum ResolutionDemand {

    /**
     * The query reads no rows from this path, so resolution owes it a schema and nothing else. How much of the
     * glob a schema needs is a property of the dataset, not of the query: a declared mapping is the schema
     * outright, {@code first_file_wins} takes it from one file, and {@code union_by_name} and {@code strict} read
     * every file by contract. Only this state may bound a listing.
     */
    SCHEMA_ONLY,

    /**
     * The query reads rows. Resolution produces the full file set, because split discovery reads it from the plan.
     */
    ROWS,

    /**
     * The query is an ungrouped aggregate this path can answer from file metadata, so resolution reads every
     * file's footer up front and split discovery is skipped entirely. The most expensive state, and worth it:
     * the footers it reads are ones the query would otherwise read in a later phase.
     */
    EAGER_STATS;

    /**
     * The demand for {@code path}, given the two plan-derived sets.
     *
     * @param requiringStats paths under an ungrouped aggregate, or {@code null} for the legacy behaviour in which
     *                       every path resolves eagerly
     * @param readingNoRows  paths whose rows are all discarded, or {@code null} when the query shape was not
     *                       examined
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
