/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

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
 * {@code answer} separates the records that are different answers about the same bytes:
 * <ul>
 *   <li>{@link #DECLARED_STRICT}: the strict-declared warm rail's record, which holds the declared schema;</li>
 *   <li>{@link #INFERRED}: the schema inferred from the whole sample;</li>
 *   <li>a positive count: the schema inferred from a share of the sample ({@code FormatReader#withSchemaSampleShare}),
 *       that many rows (lines) of this file. The effective per-file sample rather than the number of files sharing
 *       it, so shares that all reach the per-file floor reuse one record.</li>
 * </ul>
 * One compared component, rather than anything encoded inside another field, because the reconcile's contribution
 * matching must still reach every one of these records: they are reads of the same object, and none of this may
 * enter {@code dataset}, whose participants the enrich refusal compares. One {@code int} rather than a flag and a
 * count because both describe how the answer was reached, and a fifth component would grow every key by 8 bytes.
 * <p>
 * The key carries no format name, and what separates two reads of one object as different formats is not
 * a component of its own. A
 * reader's identity renders the recognized settings its config carries and nothing else, so it holds no
 * format name and two readers over a config carrying no format-specific setting vend the same string.
 * The discriminator is the coordinator lane: {@code format} is one of
 * {@code FileSourceFactory#COORDINATOR_KEYS} and deliberately not inert, so an explicit format separates
 * the addresses there, and an implied one is separated by the path's own extension.
 */
public record SchemaCacheKey(DatasetIdentity dataset, String location, long lastModifiedEpochMillis, int answer) {

    /** {@link #answer()} of the strict-declared warm rail's record. */
    public static final int DECLARED_STRICT = -1;
    /** {@link #answer()} of a record inferred from the whole schema sample. */
    public static final int INFERRED = 0;

    public SchemaCacheKey {
        if (answer < DECLARED_STRICT) {
            throw new IllegalArgumentException(
                "a schema record is strict-declared, inferred or inferred from a shared sample of rows, got answer [" + answer + "]"
            );
        }
    }

    /**
     * Key for a per-file record.
     *
     * @param declaredStrict true for the strict-declared warm rail, whose record is a different answer about the
     *                       same file than the inferred one and must not share its address
     */
    public static SchemaCacheKey build(String location, long mtime, DatasetIdentity dataset, boolean declaredStrict) {
        return new SchemaCacheKey(dataset, location, mtime, declaredStrict ? DECLARED_STRICT : INFERRED);
    }

    /**
     * Key for a per-file record inferred from a shared schema sample of {@code sharedSchemaSampleSize} rows (lines)
     * of this file. Only inference samples, so there is no strict-declared variant.
     */
    public static SchemaCacheKey buildShared(String location, long mtime, DatasetIdentity dataset, int sharedSchemaSampleSize) {
        if (sharedSchemaSampleSize < 1) {
            throw new IllegalArgumentException("a shared schema sample takes at least one row, got [" + sharedSchemaSampleSize + "]");
        }
        return new SchemaCacheKey(dataset, location, mtime, sharedSchemaSampleSize);
    }

    /** Whether this addresses the strict-declared warm rail's record. */
    public boolean declaredStrict() {
        return answer == DECLARED_STRICT;
    }

    /**
     * This address with a shared sample's depth dropped: the whole-sample {@link #INFERRED} record's address for a
     * shared-sample key, and this key otherwise. How deep inference sampled decides which schema a record holds,
     * not what a read of the file measures, so the statistics address is built from this (see
     * {@link StatisticsKey}).
     */
    public SchemaCacheKey withoutSampleDepth() {
        return answer > INFERRED ? new SchemaCacheKey(dataset, location, lastModifiedEpochMillis, INFERRED) : this;
    }
}
