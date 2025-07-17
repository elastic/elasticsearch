/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.client.internal.Client;

import java.util.Map;

// NOMERGE: replace the NodeUsageStatsForThreadPoolsCollector interface with this class.
public class NodeUsageStatsForThreadPoolsCollectorImpl implements NodeUsageStatsForThreadPoolsCollector {
    private static final Logger logger = LogManager.getLogger(NodeUsageStatsForThreadPoolsCollectorImpl.class);

    @Override
    public void collectUsageStats(
        Client client,
        ClusterState clusterState,
        ActionListener<Map<String, NodeUsageStatsForThreadPools>> listener
    ) {
        if (clusterState.getMinTransportVersion().onOrAfter(TransportVersions.TRANSPORT_NODE_USAGE_STATS_FOR_THREAD_POOLS_ACTION)) {
            client.execute(
                TransportNodeUsageStatsForThreadPoolsAction.TYPE,
                new NodeUsageStatsForThreadPoolsAction.Request(),
                listener.map(response -> response.getAllNodeUsageStatsForThreadPools())
            );
        } else {
            listener.onResponse(Map.of());
        }
    }
}
