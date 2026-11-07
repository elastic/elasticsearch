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
 * consuming it.
 * <p>
 * <b>That is reachable as a wrong count, and it is not new here.</b> A dynamic-declared dataset and an
 * inferred one over the same resource and settings mint the same address: {@code DefinitionVersion} folds the
 * resource, the dataset settings and the parent, and a mapping is neither of those, so the identity cannot
 * tell them apart. A dynamic mapping is not {@code isDeclaredSchema}, so it takes the first-file-wins rail and
 * reaches this address; and the non-strict overlay does more than retype in place - appending an absent
 * declared column upgrades a CSV or TSV read to DECLARED, which binds a headerless file differently from an
 * inferred read of it, so the two do NOT see the same survivor set. Once the per-file records are evicted and
 * the fold is not, the declared read is served the inferred read's count having read nothing. Measured, not
 * argued. The previous shape keyed this address equally blind to the mapping, so the exposure predates the
 * split; what the split changes is that the fold now outlives the per-file records in its own slice.
 * <p>
 * The fix is to fold the bound read's configuration into this key, or to refuse the memoized fold whenever a
 * declared mapping is in play.
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
