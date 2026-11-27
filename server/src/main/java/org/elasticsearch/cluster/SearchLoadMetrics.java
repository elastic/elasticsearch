/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.cluster.ClusterInfo.NodeAndShard;

import java.util.Map;

/**
 * Per-copy search demand and per-node search capacity collected from one cluster state snapshot.
 */
public record SearchLoadMetrics(
    Map<NodeAndShard, SearchCopyDemand> searchCopyDemands,
    Map<String, NodeSearchCapacity> nodeSearchCapacities
) {

    public static final SearchLoadMetrics EMPTY = new SearchLoadMetrics(Map.of(), Map.of());

    public SearchLoadMetrics {
        searchCopyDemands = Map.copyOf(searchCopyDemands);
        nodeSearchCapacities = Map.copyOf(nodeSearchCapacities);
    }
}
