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
 * Cluster service task (batched) executor that executes the replacement of data stream backing index with its
 * downsampled index.
 * After the task is executed the executor issues a delete API call for the source index however, it doesn't
 * hold up the task listener (nb we notify the listener before we call the delete API so we don't introduce
 * weird partial failure scenarios - if the delete API fails the
 * {@link org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService} will retry on the next run so the source index will get
 * deleted)
 */
public class ReplaceBackingWithDownsampleIndexExecutor extends SimpleBatchedExecutor<ReplaceSourceWithDownsampleIndexTask, Void> {
    private static final Logger LOGGER = LogManager.getLogger(ReplaceSourceWithDownsampleIndexTask.class);
    private final AllocationService allocationService;

    public ReplaceBackingWithDownsampleIndexExecutor(AllocationService allocationService) {
        this.allocationService = allocationService;
    }

    @Override
    public Tuple<ClusterState, Void> executeTask(ReplaceSourceWithDownsampleIndexTask task, ClusterState clusterState) throws Exception {
        return Tuple.tuple(task.execute(clusterState), null);
    }

    @Override
    public void taskSucceeded(ReplaceSourceWithDownsampleIndexTask task, Void unused) {
        LOGGER.trace(
            "Updated cluster state and replaced index [{}] with index [{}] in data stream [{}]",
            task.getSourceBackingIndex(),
            task.getDownsampleIndex(),
            task.getDataStreamName()
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
