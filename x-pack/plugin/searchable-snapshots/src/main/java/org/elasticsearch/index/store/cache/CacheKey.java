/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.index.store.cache;

import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.SnapshotId;

import java.util.Objects;

public class CacheKey {

    private final SnapshotId snapshotId;
    private final IndexId indexId;
    private final ShardId shardId;
    private final String fileName;

    public CacheKey(SnapshotId snapshotId, IndexId indexId, ShardId shardId, String fileName) {
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indexId = Objects.requireNonNull(indexId);
        this.shardId = Objects.requireNonNull(shardId);
        this.fileName = Objects.requireNonNull(fileName);
    }

    SnapshotId getSnapshotId() {
        return snapshotId;
    }

    IndexId getIndexId() {
        return indexId;
    }

    ShardId getShardId() {
        return shardId;
    }

    String getFileName() {
        return fileName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final CacheKey cacheKey = (CacheKey) o;
        return Objects.equals(snapshotId, cacheKey.snapshotId)
            && Objects.equals(indexId, cacheKey.indexId)
            && Objects.equals(shardId, cacheKey.shardId)
            && Objects.equals(fileName, cacheKey.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotId, indexId, shardId, fileName);
    }

    @Override
    public String toString() {
        return "[" + "snapshotId=" + snapshotId + ", indexId=" + indexId + ", shardId=" + shardId + ", fileName='" + fileName + "']";
    }

    public boolean belongsTo(SnapshotId snapshotId, IndexId indexId, ShardId shardId) {
        return Objects.equals(this.snapshotId, snapshotId)
            && Objects.equals(this.indexId, indexId)
            && Objects.equals(this.shardId, shardId);
    }
}
