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
import org.elasticsearch.client.internal.Client;

import java.util.Map;

/**
 * Collects the thread pool usage stats for each node in the cluster.
 * <p>
 * Results are returned as a map of node ID to node usage stats.
 */
public interface NodeUsageStatsForThreadPoolsCollector {
    /**
     * This will be used when there is no NodeUsageLoadCollector available.
     */
    NodeUsageStatsForThreadPoolsCollector EMPTY = (client, listener) -> listener.onResponse(Map.of());

    /**
     * Collects the thread pool usage stats ({@link NodeUsageStatsForThreadPools}) for each node in the cluster.
     *
     * @param listener The listener to receive the usage results.
     */
    void collectUsageStats(Client client, ActionListener<Map<String, NodeUsageStatsForThreadPools>> listener);
}
