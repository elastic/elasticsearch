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
import org.elasticsearch.action.admin.cluster.node.usage.NodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.client.internal.Client;

import java.util.Map;

// TODO: replace the NodeUsageStatsForThreadPoolsCollector interface with this class.
public class NodeUsageStatsForThreadPoolsCollectorImpl implements NodeUsageStatsForThreadPoolsCollector {

    @Override
    public void collectUsageStats(Client client, ActionListener<Map<String, NodeUsageStatsForThreadPools>> listener) {
        client.execute(
            TransportNodeUsageStatsForThreadPoolsAction.TYPE,
            new NodeUsageStatsForThreadPoolsAction.Request(),
            listener.map(response -> response.getAllNodeUsageStatsForThreadPools())
        );
    }
}
