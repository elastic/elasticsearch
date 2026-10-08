/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import java.util.Map;

/**
 * Node heap usage estimates and individual shard heap usage estimates collected from the same source snapshot.
 */
public record EstimatedHeapUsageStats(Map<String, NodeHeapEstimates> nodeHeapEstimates, ShardHeapUsageEstimates shardHeapUsageEstimates) {

    public static final EstimatedHeapUsageStats EMPTY = new EstimatedHeapUsageStats(Map.of(), ShardHeapUsageEstimates.empty());

    public EstimatedHeapUsageStats {
        nodeHeapEstimates = Map.copyOf(nodeHeapEstimates);
        assert shardHeapUsageEstimates != null;
    }
}
