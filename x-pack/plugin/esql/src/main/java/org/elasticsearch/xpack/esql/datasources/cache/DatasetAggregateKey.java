/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.cache;

import org.elasticsearch.xpack.esql.datasources.FileSetFingerprint;

import java.util.Objects;

/**
 * Address of the memoized multi-file statistics fold for one resolved file SET.
 * <p>
 * Identity is the listing's 128-bit file-set fingerprint - a commutative fold of every file's path, mtime and
 * size, plus the file count - which makes this key correct-or-miss by construction: any file added, removed or
 * modified derives a different key, and the stale entry ages out through the LRU with no invalidation protocol.
 * <p>
 * A kind of its own, rather than a per-file key with a fingerprint hung off it. The per-file reconcile and
 * lookup paths cannot reach this address at all now, which is what the previous shape needed a
 * {@code requireNonNull} tripwire and an {@code isDatasetAggregate()} test on every consumer to approximate.
 * <p>
 * {@code pattern} is the glob this fold was resolved from. It is part of the address and not diagnostics: two
 * different globs that happen to resolve to one file set are two datasets, and sharing a fold between them
 * would be a behaviour change rather than a saving.
 * <p>
 * <b>This address carries no read configuration.</b> It stores a bare row count with no stamp and no licence,
 * so the serve path's unstamped pass-through - which exists for the columnar readers, that harvest without
 * stamping - fires on it, and nothing compares the configuration that produced the fold against the one
 * consuming it. It is not a wrong answer today, and every reason is an accident rather than a guard: the strict
 * multi-file rail never reaches the aggregate, a non-strict overlay only retypes and renames in place so a
 * projection-less {@code COUNT(*)} sees the same survivor set, and a projection-decided drop suppresses its
 * publish at the producer. Change any one of those and this becomes a silent wrong count with no failing test.
 */
public record DatasetAggregateKey(DatasetIdentity dataset, String pattern, FileSetFingerprint fileSet) {

    public DatasetAggregateKey {
        Objects.requireNonNull(dataset, "dataset aggregate key requires a dataset identity");
        Objects.requireNonNull(fileSet, "dataset aggregate key requires a file-set fingerprint");
    }

    public static DatasetAggregateKey of(String pattern, FileSetFingerprint fileSet, DatasetIdentity dataset) {
        return new DatasetAggregateKey(dataset, pattern == null ? "" : pattern, fileSet);
    }
}
