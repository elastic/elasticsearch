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
import org.elasticsearch.index.shard.ShardId;

import java.util.Map;

/**
 * Collects the per-shard search-lane requirement in vCPU for {@link ClusterInfo}, read by an allocation decider. A shard absent from the
 * returned map has no reported requirement; a decider takes no action for it.
 */
public interface SearchLaneRequirementsCollector {

    /**
     * Used when no search-lane requirements collector is available, so no shard has a lane requirement.
     */
    SearchLaneRequirementsCollector EMPTY = (clusterState, listener) -> listener.onResponse(Map.of());

    /**
     * Collects the per-shard search-lane requirement in vCPU.
     *
     * @param clusterState The cluster state snapshot for this collection.
     * @param listener The listener which will receive the results.
     */
    void collectSearchLaneRequirements(ClusterState clusterState, ActionListener<Map<ShardId, Double>> listener);
}
