/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterInfo.NodeAndShard;

import java.util.Map;

/**
 * Collects per-copy search demand for {@link ClusterInfo}. A copy absent from the returned map has no measurement.
 */
public interface SearchCopyDemandCollector {

    /**
     * Used when no search-copy demand collector is available, so no copy has a measurement.
     */
    SearchCopyDemandCollector EMPTY = (clusterState, listener) -> listener.onResponse(Map.of());

    /**
     * Collects per-copy search demand, keyed by the node and shard the copy is on.
     *
     * @param clusterState The cluster state snapshot for this collection.
     * @param listener The listener which will receive the results.
     */
    void collectSearchCopyDemands(ClusterState clusterState, ActionListener<Map<NodeAndShard, SearchCopyDemand>> listener);
}
