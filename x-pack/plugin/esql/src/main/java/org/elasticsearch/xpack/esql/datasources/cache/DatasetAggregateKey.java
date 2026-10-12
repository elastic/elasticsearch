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
 * No read configuration: one owner over one file set performs one read. A per-file record is addressed by
 * {@link DatasetIdentity} plus its read instead, because one file's measurements are reusable by any dataset
 * reading it and there a read configuration does have two reads to separate.
 */
public record DatasetAggregateKey(Owner owner, String pattern, FileSetFingerprint fileSet) {

    /**
     * What a dataset-level fold belongs to. Two kinds, because there are two kinds of query, and a sum type
     * rather than two nullable fields so the key cannot represent an entry owned by neither or by both.
     */
    public sealed interface Owner {}

    /**
     * A registered dataset, owned by its definition. A dataset-level fold is determined by one definition
     * entire, so any edit to it moves this address and the previous definition's fold becomes unreachable -
     * this tier's invalidation protocol, in place of a notification.
     */
    public record ByDefinition(String datasetVersion) implements Owner {
        public ByDefinition {
            if (datasetVersion == null || datasetVersion.isEmpty()) {
                throw new IllegalArgumentException("a definition-owned aggregate address needs the definition version");
            }
        }
    }

    /**
     * A bare {@code FROM} over a URI, which has no stored definition to own the fold, so the resolved identity
     * owns it - as this tier was addressed before a definition could. Holding the {@link DatasetIdentity} itself
     * rather than its rendering keeps its own {@code equals} authoritative: its {@code toString} is documented as
     * being for a log line, and an address built from that would make the rendering part of the contract.
     */
    public record ByIdentity(DatasetIdentity identity) implements Owner {
        public ByIdentity {
            Objects.requireNonNull(identity, "an identity-owned aggregate address needs the resolved identity");
        }
    }

    public DatasetAggregateKey {
        Objects.requireNonNull(fileSet, "dataset aggregate key requires a file-set fingerprint");
        Objects.requireNonNull(owner, "a dataset aggregate address needs the owner the fold belongs to");
    }

    public static DatasetAggregateKey ofDefinition(String pattern, FileSetFingerprint fileSet, String datasetVersion) {
        return new DatasetAggregateKey(new ByDefinition(datasetVersion), pattern == null ? "" : pattern, fileSet);
    }

    public static DatasetAggregateKey ofIdentity(String pattern, FileSetFingerprint fileSet, DatasetIdentity identity) {
        return new DatasetAggregateKey(new ByIdentity(identity), pattern == null ? "" : pattern, fileSet);
    }
}
