/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.searchablesnapshots.cache.common;

import org.elasticsearch.index.shard.ShardId;

import java.util.Objects;

public record CacheKey(String snapshotUUID, String snapshotIndexName, ShardId shardId, String fileName) {

    public CacheKey(String snapshotUUID, String snapshotIndexName, ShardId shardId, String fileName) {
        this.snapshotUUID = Objects.requireNonNull(snapshotUUID);
        this.snapshotIndexName = Objects.requireNonNull(snapshotIndexName);
        this.shardId = Objects.requireNonNull(shardId);
        this.fileName = Objects.requireNonNull(fileName);
    }
}
