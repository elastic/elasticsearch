/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", and the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

/** Snapshot restore shards that could not be placed during a desired-balance computation because of disk capacity. */
public record SnapshotRestoreAllocationResult(Map<ShardId, Long> unplaceableShardSizes) {

    public static final SnapshotRestoreAllocationResult EMPTY = new SnapshotRestoreAllocationResult(Map.of());

    public SnapshotRestoreAllocationResult {
        unplaceableShardSizes = Map.copyOf(unplaceableShardSizes);
    }
}
