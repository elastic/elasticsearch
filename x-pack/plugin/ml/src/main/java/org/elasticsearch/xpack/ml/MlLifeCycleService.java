/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlController;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;

public class MlLifeCycleService {

    private final ClusterService clusterService;
    private final DatafeedRunner datafeedRunner;
    private final MlController mlController;
    private final AutodetectProcessManager autodetectProcessManager;
    private final DataFrameAnalyticsManager analyticsManager;
    private final MlMemoryTracker memoryTracker;

    MlLifeCycleService(ClusterService clusterService, DatafeedRunner datafeedRunner, MlController mlController,
                       AutodetectProcessManager autodetectProcessManager, DataFrameAnalyticsManager analyticsManager,
                       MlMemoryTracker memoryTracker) {
        this.clusterService = Objects.requireNonNull(clusterService);
        this.datafeedRunner = Objects.requireNonNull(datafeedRunner);
        this.mlController = Objects.requireNonNull(mlController);
        this.autodetectProcessManager = Objects.requireNonNull(autodetectProcessManager);
        this.analyticsManager = Objects.requireNonNull(analyticsManager);
        this.memoryTracker = Objects.requireNonNull(memoryTracker);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void beforeStop() {
                stop();
            }
        });
    }

    public synchronized void stop() {
        try {
            // This prevents data frame analytics from being marked as failed due to exceptions occurring while the node is shutting down.
            analyticsManager.markNodeAsShuttingDown();
            // This prevents datafeeds from sending data to autodetect processes WITHOUT stopping the datafeeds, so they get reassigned.
            // We have to do this first, otherwise the datafeeds could fail if they send data to a dead autodetect process.
            datafeedRunner.prepareForImmediateShutdown();
            // This kills autodetect processes WITHOUT closing the jobs, so they get reassigned.
            autodetectProcessManager.killAllProcessesOnThisNode();
            mlController.stop();
        } catch (IOException e) {
            // We're stopping anyway, so don't let this complicate the shutdown sequence
        }
        memoryTracker.stop();
    }

    /**
     * Is it safe to shut down a particular node without any ML rework being required?
     * @param nodeId ID of the node being shut down.
     * @return Has all active ML work vacated the specified node?
     */
    public boolean isNodeSafeToShutdown(String nodeId) {
        return isNodeSafeToShutdown(nodeId, clusterService.state());
    }

    static boolean isNodeSafeToShutdown(String nodeId, ClusterState state) {
        // If we are in a mixed version cluster that doesn't support locally aborting persistent tasks then
        // we cannot perform graceful shutdown, so just revert to the behaviour of previous versions where
        // the node shutdown API didn't exist
        if (PersistentTasksService.isLocalAbortSupported(state) == false) {
            return true;
        }
        PersistentTasksCustomMetadata tasks = state.metadata().custom(PersistentTasksCustomMetadata.TYPE);
        // TODO: currently only considering anomaly detection jobs - could extend in the future
        // Ignore failed jobs - the persistent task still exists to remember the failure (because no
        // persistent task means closed), but these don't need to be relocated to another node.
        return MlTasks.nonFailedJobTasksOnNode(tasks, nodeId).isEmpty() &&
            MlTasks.nonFailedSnapshotUpgradeTasksOnNode(tasks, nodeId).isEmpty();
    }

    /**
     * Called when nodes have been marked for shutdown.
     * This method will only react if the local node is in the collection provided.
     * (The assumption is that this method will be called on every node, so each node will get to react.)
     * If the local node is marked for shutdown then ML jobs running on it will be told to gracefully
     * persist state and then unassigned so that they relocate to a different node.
     * @param shutdownNodeIds IDs of all nodes being shut down.
     */
    public void signalGracefulShutdown(Collection<String> shutdownNodeIds) {
        signalGracefulShutdown(clusterService.state(), shutdownNodeIds);
    }

    void signalGracefulShutdown(ClusterState state, Collection<String> shutdownNodeIds) {

        // If we are in a mixed version cluster that doesn't support locally aborting persistent tasks then
        // we cannot perform graceful shutdown, so just revert to the behaviour of previous versions where
        // the node shutdown API didn't exist
        if (PersistentTasksService.isLocalAbortSupported(state) == false) {
            return;
        }

        if (shutdownNodeIds.contains(state.nodes().getLocalNodeId())) {

            datafeedRunner.vacateAllDatafeedsOnThisNode(
                "previously assigned node [" + state.nodes().getLocalNode().getName() + "] is shutting down");
            autodetectProcessManager.vacateOpenJobsOnThisNode();
        }
    }
}
