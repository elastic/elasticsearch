/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache.common;

import org.elasticsearch.index.shard.ShardId;

import java.util.Objects;

public class CacheKey {

    private final String snapshotUUID;
    private final String snapshotIndexName;
    private final ShardId shardId;
    private final String fileName;

    public CacheKey(String snapshotUUID, String snapshotIndexName, ShardId shardId, String fileName) {
        this.snapshotUUID = Objects.requireNonNull(snapshotUUID);
        this.snapshotIndexName = Objects.requireNonNull(snapshotIndexName);
        this.shardId = Objects.requireNonNull(shardId);
        this.fileName = Objects.requireNonNull(fileName);
    }

    public String getSnapshotUUID() {
        return snapshotUUID;
    }

    public String getSnapshotIndexName() {
        return snapshotIndexName;
    }

    public ShardId getShardId() {
        return shardId;
    }

    public String getFileName() {
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
        return Objects.equals(snapshotUUID, cacheKey.snapshotUUID)
            && Objects.equals(snapshotIndexName, cacheKey.snapshotIndexName)
            && Objects.equals(shardId, cacheKey.shardId)
            && Objects.equals(fileName, cacheKey.fileName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(snapshotUUID, snapshotIndexName, shardId, fileName);
    }

    @Override
    public String toString() {
        return "[snapshotUUID="
            + snapshotUUID
            + ", snapshotIndexName="
            + snapshotIndexName
            + ", shardId="
            + shardId
            + ", fileName='"
            + fileName
            + "']";
    }
}
