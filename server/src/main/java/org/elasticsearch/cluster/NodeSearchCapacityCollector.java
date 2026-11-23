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

import java.util.Map;

/**
 * Collects per-node search capacity for {@link ClusterInfo}. A node absent from the returned map has unknown capacity.
 */
public interface NodeSearchCapacityCollector {

    /**
     * Used when no node search capacity collector is available, so no node has a reported capacity.
     */
    NodeSearchCapacityCollector EMPTY = (clusterState, listener) -> listener.onResponse(Map.of());

    /**
     * Collects per-node search capacity, keyed by node id.
     *
     * @param clusterState The cluster state snapshot for this collection.
     * @param listener The listener which will receive the results.
     */
    void collectNodeSearchCapacities(ClusterState clusterState, ActionListener<Map<String, NodeSearchCapacity>> listener);
}
