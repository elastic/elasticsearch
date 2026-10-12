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
 * The file-set fingerprint - a commutative fold of every file's path, mtime and size, plus the count - makes this
 * key correct-or-miss by construction: any file added, removed or modified derives a different key and the stale
 * entry ages out through the LRU. {@code pattern} is part of the address rather than diagnostics, because two
 * globs that happen to resolve to one file set are two datasets.
 * <p>
 * {@code datasetVersion} is which definition exactly ({@code DefinitionVersion.ofDataset}). A dataset-level fold
 * is determined by one definition entire, so any edit to it moves this address and the previous definition's fold
 * becomes unreachable - this tier's invalidation protocol, in place of a notification.
 * <p>
 * No read configuration: one definition over one file set performs one read. A per-file record is addressed by
 * {@link DatasetIdentity} instead, because one file's measurements are reusable by any dataset reading it and
 * there a read configuration does have two reads to separate.
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
