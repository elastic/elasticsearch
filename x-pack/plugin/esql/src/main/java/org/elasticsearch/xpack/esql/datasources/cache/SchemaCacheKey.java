/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;

/**
 * Cache key for schema inference results. Includes mtime-in-key for invalidation.
 * <p>
 * {@code dataset} is which dataset, read through which data source, this record belongs to - see
 * {@link DatasetIdentity}. One reference in place of the dataset's share of the address. It is NOT shared
 * one instance per resolve: the resolver derives it per mint site, because the participant fold resolves a
 * reader per object name.
 * <p>
 * This key addresses a per-file record and nothing else. A dataset-level fold is a
 * {@link DatasetAggregateKey} in its own store, so no component here distinguishes the two kinds and no
 * consumer has to test for it.
 * <p>
 * {@code declaredStrict} separates a per-file record on the strict-declared warm rail from the inferred
 * record for the same file, which is a different answer about the same bytes. A component the key
 * compares, rather than anything encoded inside another field, because the reconcile's contribution
 * matching must still reach these records.
 * <p>
 * {@code readConfig} addresses a STATISTICS record by the read that produced it, and is {@code null} on
 * every schema record. A statistic measures the rows one read produced, so two reads of one file that
 * resolved different schemas measured different things and must not share an address; a schema record
 * describes the file itself and is the same answer whoever asks, so it keeps the address it has.
 * <p>
 * The key carries no format name, and what separates two reads of one object as different formats is not
 * a component of its own. A
 * reader's identity renders the recognized settings its config carries and nothing else, so it holds no
 * format name and two readers over a config carrying no format-specific setting vend the same string.
 * The discriminator is the coordinator lane: {@code format} is one of
 * {@code FileSourceFactory#COORDINATOR_KEYS} and deliberately not inert, so an explicit format separates
 * the addresses there, and an implied one is separated by the path's own extension.
 */
public record SchemaCacheKey(
    DatasetIdentity dataset,
    String canonicalPath,
    long lastModifiedEpochMillis,
    boolean declaredStrict,
    @Nullable String readConfig
) {
    /**
     * Key for a per-file record.
     *
     * @param declaredStrict true for the strict-declared warm rail, whose record is a different answer about the
     *                       same file than the inferred one and must not share its address
     */
    public static SchemaCacheKey build(String canonicalPath, long mtime, DatasetIdentity dataset, boolean declaredStrict) {
        return new SchemaCacheKey(dataset, canonicalPath, mtime, declaredStrict, null);
    }

    /**
     * This key's sibling that addresses the statistics harvested under {@code readConfig}, leaving every other
     * component alone. Derived from the schema key rather than built from parts so the two cannot drift: a
     * statistics record is always about the same path, dataset and rail as the schema record beside it, and only
     * the read differs.
     * <p>
     * A {@code null} or empty {@code readConfig} returns {@code this}. The producing rail stamps no read
     * configuration in that case - {@link ReadConfigFingerprint#UNKNOWN} territory - and an address asserting a
     * read nobody recorded would claim more than the harvest does.
     */
    public SchemaCacheKey withReadConfig(@Nullable String readConfig) {
        if (readConfig == null || readConfig.isEmpty()) {
            return this;
        }
        return new SchemaCacheKey(dataset, canonicalPath, lastModifiedEpochMillis, declaredStrict, readConfig);
    }

    /** True when this key addresses a statistics record rather than the schema record beside it. */
    boolean isStatisticsRecord() {
        return readConfig != null;
    }
}
