/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

/**
 * Per-shard heap usage estimates from an {@link EstimatedHeapUsageCollector}, plus a default for any shard that has no entry in
 * {@link #perShard()}.
 */
public record ShardHeapUsageEstimates(
    Map<ShardId, ShardAndIndexHeapUsage> perShard,
    ShardAndIndexHeapUsage defaultForShardsWithoutMetrics
) {

    public ShardHeapUsageEstimates {
        assert perShard != null;
        assert defaultForShardsWithoutMetrics != null;
        perShard = Map.copyOf(perShard);
    }

    public static ShardHeapUsageEstimates empty() {
        return new ShardHeapUsageEstimates(Map.of(), ShardAndIndexHeapUsage.ZERO);
    }
}
