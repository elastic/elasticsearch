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

/**
 * Collects per-copy search demand and per-node search capacity for {@link ClusterInfo}.
 */
public interface SearchLoadMetricsCollector {

    /**
     * Used when no search load metrics collector is available.
     */
    SearchLoadMetricsCollector EMPTY = (clusterState, listener) -> listener.onResponse(SearchLoadMetrics.EMPTY);

    /**
     * Collects per-copy search demand and per-node search capacity from one cluster state snapshot.
     *
     * @param clusterState The cluster state snapshot for this collection.
     * @param listener The listener which will receive the results.
     */
    void collectSearchLoadMetrics(ClusterState clusterState, ActionListener<SearchLoadMetrics> listener);
}
