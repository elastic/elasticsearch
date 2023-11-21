/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.datastreams.lifecycle.downsampling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.SimpleBatchedExecutor;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.core.Tuple;

import static org.elasticsearch.cluster.routing.allocation.allocator.AllocationActionListener.rerouteCompletionIsNotRequired;

/**
 * Cluster service task (batched) executor that deletes the source index and adds its downsample index to the data stream.
 */
public class DeleteSourceAndAddDownsampleIndexExecutor extends SimpleBatchedExecutor<DeleteSourceAndAddDownsampleToDS, Void> {
    private static final Logger LOGGER = LogManager.getLogger(DeleteSourceAndAddDownsampleToDS.class);
    private final AllocationService allocationService;

    public DeleteSourceAndAddDownsampleIndexExecutor(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    public Tuple<ClusterState, Void> executeTask(DeleteSourceAndAddDownsampleToDS task, ClusterState clusterState) throws Exception {
        return Tuple.tuple(task.execute(clusterState), null);
    }

    @Override
    public void taskSucceeded(DeleteSourceAndAddDownsampleToDS task, Void unused) {
        LOGGER.trace(
            "Updated cluster state and replaced index [{}] with index [{}] in data stream [{}]. Index [{}] was deleted",
            task.getSourceBackingIndex(),
            task.getDownsampleIndex(),
            task.getDataStreamName(),
            task.getSourceBackingIndex()
        );
        task.getListener().onResponse(null);
    }

    @Override
    public ClusterState afterBatchExecution(ClusterState clusterState, boolean clusterStateChanged) {
        if (clusterStateChanged) {
            return allocationService.reroute(
                clusterState,
                "deleted indices",
                rerouteCompletionIsNotRequired() // it is not required to balance shard to report index deletion success
            );
        }
        return clusterState;
    }
}
