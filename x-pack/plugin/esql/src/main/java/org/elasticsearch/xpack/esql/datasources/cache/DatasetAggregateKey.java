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
 * <b>{@code datasetVersion} is which definition exactly</b> - {@code DefinitionVersion.ofDataset}, which folds
 * the dataset's and the data source's names, the resource, the settings, the credentials and the declared
 * mapping. A dataset-level fold is determined by one definition in its entirety, so it is addressed by that
 * definition and shared with no other dataset: two datasets over identical bytes with identical settings are
 * still two datasets, and a count measured under one mapping is not the other's to serve, because a
 * declaration that drops rows under a lenient policy counts fewer of them.
 * <p>
 * Any edit to the dataset therefore moves this address and the fold measured under the previous definition
 * becomes unreachable, which is the invalidation protocol this tier has instead of a notification: nothing has
 * to notice a change and tell the cache about it.
 * <p>
 * This is the dataset tier only. A per-file record stays addressed by the content-derived
 * {@link DatasetIdentity}, because one file's schema and measurements are legitimately reusable by any dataset
 * that reads that file - and there the read configuration disambiguates, because one file can be read several
 * ways. Here it cannot: one definition over one file set performs one read, so there is nothing for a read
 * configuration to separate.
 */
public record DatasetAggregateKey(String datasetVersion, String pattern, FileSetFingerprint fileSet) {

    public DatasetAggregateKey {
        Objects.requireNonNull(fileSet, "dataset aggregate key requires a file-set fingerprint");
        if (datasetVersion == null || datasetVersion.isEmpty()) {
            throw new IllegalArgumentException("a dataset aggregate address needs the definition it belongs to");
        }
    }

    public static DatasetAggregateKey of(String pattern, FileSetFingerprint fileSet, String datasetVersion) {
        return new DatasetAggregateKey(datasetVersion, pattern == null ? "" : pattern, fileSet);
    }
}
