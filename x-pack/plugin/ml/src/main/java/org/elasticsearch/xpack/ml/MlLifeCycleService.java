/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedRunner;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.process.MlController;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import static org.elasticsearch.core.Strings.format;

public class MlLifeCycleService {

    /**
     * Maximum time we'll wait for jobs to gracefully persist their state and stop their associated
     * processes. We expect this to take a minute or two at most if all goes to plan. The longer
     * timeout here is to avoid the need for user intervention if something doesn't work and the
     * graceful shutdown gets stuck.
     */
    public static final Duration MAX_GRACEFUL_SHUTDOWN_TIME = Duration.of(10, ChronoUnit.MINUTES);

    private static final Logger logger = LogManager.getLogger(MlLifeCycleService.class);

    private final ClusterService clusterService;
    private final DatafeedRunner datafeedRunner;
    private final MlController mlController;
    private final AutodetectProcessManager autodetectProcessManager;
    private final DataFrameAnalyticsManager analyticsManager;
    private final MlMemoryTracker memoryTracker;
    private final Map<String, Instant> shutdownStartTimes = new ConcurrentHashMap<>();

    MlLifeCycleService(
        ClusterService clusterService,
        DatafeedRunner datafeedRunner,
        MlController mlController,
        AutodetectProcessManager autodetectProcessManager,
        DataFrameAnalyticsManager analyticsManager,
        MlMemoryTracker memoryTracker
    ) {
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
        return isNodeSafeToShutdown(nodeId, clusterService.state(), shutdownStartTimes.get(nodeId), Clock.systemUTC());
    }

    static boolean isNodeSafeToShutdown(String nodeId, ClusterState state, Instant shutdownStartTime, Clock clock) {

        // If the shutdown has taken too long then any remaining tasks will just be cut off when the node dies
        if (shutdownStartTime != null && shutdownStartTime.isBefore(clock.instant().minus(MAX_GRACEFUL_SHUTDOWN_TIME))) {
            return true;
        }

        logger.debug(() -> format("Checking shutdown safety for node id [%s]", nodeId));

        boolean nodeHasRunningDeployments = nodeHasRunningDeployments(nodeId, state);

        logger.debug(() -> format("Node id [%s] has running deployments: %s", nodeId, nodeHasRunningDeployments));

        PersistentTasksCustomMetadata tasks = state.metadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        // Ignore failed jobs - the persistent task still exists to remember the failure (because no
        // persistent task means closed), but these don't need to be relocated to another node.
        return MlTasks.nonFailedJobTasksOnNode(tasks, nodeId).isEmpty()
            && MlTasks.nonFailedSnapshotUpgradeTasksOnNode(tasks, nodeId).isEmpty()
            && nodeHasRunningDeployments == false;
    }

    private static boolean nodeHasRunningDeployments(String nodeId, ClusterState state) {
        TrainedModelAssignmentMetadata metadata = TrainedModelAssignmentMetadata.fromState(state);

        return metadata.allAssignments().values().stream().anyMatch(assignment -> {
            if (assignment.isRoutedToNode(nodeId)) {
                RoutingInfo routingInfo = assignment.getNodeRoutingTable().get(nodeId);
                logger.debug(
                    () -> format(
                        "Assignment deployment id [%s] is routed to shutting down nodeId %s state: %s",
                        assignment.getDeploymentId(),
                        nodeId,
                        routingInfo.getState()
                    )
                );

                // A routing could exist in the stopped state if the deployment has successfully drained any remaining requests
                // If a route is starting, started, or stopping then the node is not ready to shut down yet
                return routingInfo.getState().isNoneOf(RoutingState.STOPPED, RoutingState.FAILED);
            }

            return false;
        });
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
        signalGracefulShutdown(clusterService.state(), shutdownNodeIds, Clock.systemUTC());
    }

    void signalGracefulShutdown(ClusterState state, Collection<String> shutdownNodeIds, Clock clock) {
        String localNodeId = state.nodes().getLocalNodeId();
        updateShutdownStartTimes(shutdownNodeIds, localNodeId, clock);
        if (shutdownNodeIds.contains(localNodeId)) {
            datafeedRunner.vacateAllDatafeedsOnThisNode(
                "previously assigned node [" + state.nodes().getLocalNode().getName() + "] is shutting down"
            );
            autodetectProcessManager.vacateOpenJobsOnThisNode();
        }
    }

    Instant getShutdownStartTime(String nodeId) {
        return shutdownStartTimes.get(nodeId);
    }

    private void updateShutdownStartTimes(Collection<String> shutdownNodeIds, String localNodeId, Clock clock) {
        for (String shutdownNodeId : shutdownNodeIds) {
            shutdownStartTimes.computeIfAbsent(shutdownNodeId, key -> {
                if (key.equals(localNodeId)) {
                    logger.info("Starting node shutdown sequence for ML");
                }
                return Instant.now(clock);
            });
        }
        shutdownStartTimes.keySet().retainAll(shutdownNodeIds);
    }
}
