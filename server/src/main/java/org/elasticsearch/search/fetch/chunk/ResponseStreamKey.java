/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch.chunk;

import org.elasticsearch.index.shard.ShardId;

import java.util.Objects;

/**
 * Composite key for identifying a response stream.
 * Combines the coordinating task ID with a shard-specific identifier.
 */
public final class ResponseStreamKey {
    private final long coordinatingTaskId;
    private final ShardId shardId;

    public ResponseStreamKey(long coordinatingTaskId, ShardId shardId) {
        this.coordinatingTaskId = coordinatingTaskId;
        this.shardId = shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponseStreamKey that = (ResponseStreamKey) o;
        return coordinatingTaskId == that.coordinatingTaskId
            && shardId.equals(that.shardId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(coordinatingTaskId, shardId);
    }

    @Override
    public String toString() {
        return "ResponseStreamKey[taskId=" + coordinatingTaskId + ", shardId=" + shardId + "]";
    }
}
