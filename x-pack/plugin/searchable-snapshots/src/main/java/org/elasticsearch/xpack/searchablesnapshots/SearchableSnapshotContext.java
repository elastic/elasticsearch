/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots;

import java.util.Objects;

/**
 * {@link SearchableSnapshotContext} contains all the information required to access the files of a shard that has been snapshotted.
 */
public class SearchableSnapshotContext {

    private final String repository;
    private final String snapshot;
    private final String index;
    private final int shard;

    public SearchableSnapshotContext(String repository, String snapshot, String index, int shard) {
        this.repository = Objects.requireNonNull(repository, "repository name must not be null");
        this.snapshot = Objects.requireNonNull(snapshot, "snapshot id must not be null");
        this.index = Objects.requireNonNull(index, "index id must not be null");
        this.shard = shard;
    }

    public String getRepository() {
        return repository;
    }

    public String getSnapshot() {
        return snapshot;
    }

    public String getIndex() {
        return index;
    }

    public int getShard() {
        return shard;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchableSnapshotContext that = (SearchableSnapshotContext) o;
        return shard == that.shard &&
            Objects.equals(repository, that.repository) &&
            Objects.equals(snapshot, that.snapshot) &&
            Objects.equals(index, that.index);
    }

    @Override
    public int hashCode() {
        return Objects.hash(repository, snapshot, index, shard);
    }

    @Override
    public String toString() {
        return "SearchableSnapshotContext{" +
            "repository='" + repository + '\'' +
            ", snapshot='" + snapshot + '\'' +
            ", index='" + index + '\'' +
            ", shard=" + shard +
            '}';
    }
}
