/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

import java.util.Objects;

/**
 * Cache key for schema inference results. Includes mtime-in-key for invalidation.
 * <p>
 * {@code dataset} is which dataset, read through which data source, this record belongs to - see
 * {@link DatasetIdentity}. It replaces three strings this key used to carry; it is NOT yet shared one
 * instance per resolve, because the resolver derives it per mint site - the participant fold resolves a
 * reader per object name - so what this saves is the component count rather than instance sharing.
 * <p>
 * {@code fileSetFingerprint} carries the 128-bit fingerprint of the resolved file set for a
 * dataset-level aggregate key (see {@link #forDatasetAggregate}); it is {@code null} for every
 * per-file key, and that is what {@link #isDatasetAggregate} tests. It used to be tested by an
 * {@code endsWith} against a marker suffix smuggled into the format name, with the fingerprint saying
 * the same thing a second time and a {@code requireNonNull} keeping the two encodings in agreement. One
 * encoding is enough.
 * <p>
 * {@code declaredStrict} separates a per-file record on the strict-declared warm rail from the inferred
 * record for the same file, which is a different answer about the same bytes. A named boolean rather
 * than the second marker suffix: the reconcile's contribution matching MUST still reach these records,
 * so the distinction has to be a component the key compares. Nothing ever parsed that suffix - only
 * {@code isDatasetAggregate} parsed one, and it parsed the other marker - but concatenating it onto the
 * format name made that field mean two things at once, which is what this removes.
 * <p>
 * {@code readConfig} addresses a STATISTICS record by the read that produced it, and is {@code null} on
 * every schema record. A statistic measures the rows one read produced, so two reads of one file that
 * resolved different schemas measured different things and must not share an address; a schema record
 * describes the file itself and is the same answer whoever asks, so it keeps the address it has.
 * <p>
 * The format name is gone from the key entirely. Nothing read it for its value, and it is derivable from
 * the path and the config by {@code detectFormatType}; what it actually did here was carry two unrelated
 * things, which reader produced the record and which kind of record it was.
 */
public record SchemaCacheKey(
    DatasetIdentity dataset,
    String canonicalPath,
    long lastModifiedEpochMillis,
    @Nullable FileSetFingerprint fileSetFingerprint,
    boolean declaredStrict,
    @Nullable String readConfig
) {
    /** True when this key addresses a dataset-level aggregate rather than a per-file record. */
    public boolean isDatasetAggregate() {
        return fileSetFingerprint != null;
    }

    /**
     * Key for a per-file record.
     *
     * @param declaredStrict true for the strict-declared warm rail, whose record is a different answer about the
     *                       same file than the inferred one and must not share its address
     */
    public static SchemaCacheKey build(String canonicalPath, long mtime, DatasetIdentity dataset, boolean declaredStrict) {
        return new SchemaCacheKey(dataset, canonicalPath, mtime, null, declaredStrict, null);
    }

    /**
     * Key for a dataset-level aggregate entry: the memoized multi-file stats fold for one resolved file SET.
     * Identity is the listing's 128-bit file-set fingerprint (a commutative fold of every file's path + mtime +
     * size, plus the file count), which makes the key correct-or-miss by construction: any file added, removed,
     * or modified derives a different key, and the stale entry ages out via LRU - no invalidation protocol.
     * {@code canonicalPath} is the glob pattern, which is diagnostics only.
     * <p>
     * <b>The aggregate does NOT carry a read configuration today.</b> It stores a bare row count with no stamp
     * and no licence, so the serve path's unstamped pass-through - which exists for the columnar readers, that
     * harvest without stamping - fires on it, and nothing compares the configuration that produced the aggregate
     * against the one consuming it. It is not a wrong answer, and each reason is an accident rather than a guard:
     * the strict multi-file rail never reaches the aggregate, a non-strict overlay only retypes and renames in
     * place so a projection-less {@code COUNT(*)} sees the same survivor set, and a projection-decided drop
     * suppresses its publish at the producer. Change any one of those and this becomes a silent wrong count with
     * no failing test.
     */
    public static SchemaCacheKey forDatasetAggregate(String pattern, FileSetFingerprint fingerprint, DatasetIdentity dataset) {
        // Load-bearing, not defensive: a null fingerprint here would make isDatasetAggregate() answer false for
        // an aggregate key, and the reconcile would then enrich it with a per-file contribution.
        Objects.requireNonNull(fingerprint, "dataset aggregate key requires a non-null file-set fingerprint");
        return new SchemaCacheKey(dataset, pattern == null ? "" : pattern, 0L, fingerprint, false, null);
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
        return new SchemaCacheKey(dataset, canonicalPath, lastModifiedEpochMillis, fileSetFingerprint, declaredStrict, readConfig);
    }

    /** True when this key addresses a statistics record rather than the schema record beside it. */
    boolean isStatisticsRecord() {
        return readConfig != null;
    }
}
