/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;

public class NodeAllocationService {

    private final ClusterService clusterService;
    private final WriteLoadForecaster writeLoadForecaster;

    public NodeAllocationService(ClusterService clusterService, WriteLoadForecaster writeLoadForecaster) {
        this.clusterService = clusterService;
        this.writeLoadForecaster = writeLoadForecaster;
    }

    public NodeAllocationStats stats(String localNodeId) {
        var state = clusterService.state();
        var node = state.getRoutingNodes().node(localNodeId);

        double forecastedWriteLoad = 0.0;
        long forecastedDiskUsage = 0;
        for (ShardRouting shardRouting : node) {
            IndexMetadata indexMetadata = state.metadata().getIndexSafe(shardRouting.index());
            forecastedWriteLoad += writeLoadForecaster.getForecastedWriteLoad(indexMetadata).orElse(0.0);
            forecastedDiskUsage += indexMetadata.getForecastedShardSizeInBytes().orElse(0);// TODO fallback?
        }

        return new NodeAllocationStats(node.size(), forecastedWriteLoad, forecastedDiskUsage);
    }
}
