/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference.autoscaling;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ClientHelper;
import org.elasticsearch.xpack.core.ml.action.GetDeploymentStatsAction;
import org.elasticsearch.xpack.core.ml.action.UpdateTrainedModelDeploymentAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.AssignmentStats;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;

import java.util.HashMap;
import java.util.Map;

public class AutoscalerService implements ClusterStateListener {

    record Stats(long successCount, long pendingCount, long failedCount, double inferenceTime) {

        long requestCount() {
            return successCount + pendingCount + failedCount;
        }

        double totalInferenceTime() {
            return successCount * inferenceTime;
        }

        Stats add(Stats value) {
            long newSuccessCount = successCount + value.successCount;
            long newPendingCount = pendingCount + value.pendingCount;
            long newFailedCount = failedCount + value.failedCount;
            double newInferenceTime = newSuccessCount > 0
                ? (totalInferenceTime() + value.totalInferenceTime()) / newSuccessCount
                : Double.NaN;
            return new Stats(newSuccessCount, newPendingCount, newFailedCount, newInferenceTime);
        }

        Stats sub(Stats value) {
            long newSuccessCount = successCount - value.successCount;
            long newPendingCount = pendingCount - value.pendingCount;
            long newFailedCount = failedCount - value.failedCount;
            double newInferenceTime = newSuccessCount > 0
                ? (totalInferenceTime() - value.totalInferenceTime()) / newSuccessCount
                : Double.NaN;
            return new Stats(newSuccessCount, newPendingCount, newFailedCount, newInferenceTime);
        }
    }

    private static final int DEFAULT_TIME_INTERVAL_SECONDS = 10;

    private static final Logger logger = LogManager.getLogger(AutoscalerService.class);

    private final int timeIntervalSeconds;
    private final ThreadPool threadPool;
    private final ClusterService clusterService;
    private final Client client;
    private final boolean isNlpEnabled;

    private final Map<String, Stats> lastInferenceStatsByDeploymentNode;
    private final Map<String, Autoscaler> autoscalers;

    private volatile Scheduler.Cancellable cancellable;

    public AutoscalerService(ThreadPool threadPool, ClusterService clusterService, Client client, boolean isNlpEnabled) {
        this(threadPool, clusterService, client, isNlpEnabled, DEFAULT_TIME_INTERVAL_SECONDS);
    }

    // visible for testing
    AutoscalerService(ThreadPool threadPool, ClusterService clusterService, Client client, boolean isNlpEnabled, int timeIntervalSeconds) {
        this.threadPool = threadPool;
        this.clusterService = clusterService;
        this.client = client;
        this.isNlpEnabled = isNlpEnabled;
        this.timeIntervalSeconds = timeIntervalSeconds;

        lastInferenceStatsByDeploymentNode = new HashMap<>();
        autoscalers = new HashMap<>();
    }

    public synchronized void start() {
        updateAutoscalers(clusterService.state());
        clusterService.addListener(this);
        if (autoscalers.isEmpty() == false) {
            startScheduling();
        }
    }

    public synchronized void stop() {
        stopScheduling();
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        updateAutoscalers(event.state());
        if (autoscalers.isEmpty() == false) {
            startScheduling();
        }  else {
            stopScheduling();
        }
    }

    private synchronized void updateAutoscalers(ClusterState state) {
        if (isNlpEnabled == false) {
            return;
        }

        TrainedModelAssignmentMetadata assignments = TrainedModelAssignmentMetadata.fromState(state);
        for (TrainedModelAssignment assignment : assignments.allAssignments().values()) {
            if (assignment.getAutoscalingSettings() != null && assignment.getAutoscalingSettings().getEnabled()) {
                Autoscaler autoscaler = autoscalers.computeIfAbsent(
                    assignment.getDeploymentId(),
                    key -> new Autoscaler(assignment.getDeploymentId(), assignment.totalTargetAllocations())
                );
                autoscaler.setMinMaxNumberOfAllocations(
                    assignment.getAutoscalingSettings().getMinNumberOfAllocations(),
                    assignment.getAutoscalingSettings().getMaxNumberOfAllocations()
                );
            } else {
                autoscalers.remove(assignment.getDeploymentId());
            }
        }
    }

    private synchronized void startScheduling() {
        if (cancellable == null) {
            logger.debug("Starting ML inference autoscaler");
            try {
                cancellable = threadPool.scheduleWithFixedDelay(this::trigger, TimeValue.timeValueSeconds(timeIntervalSeconds), threadPool.generic());
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown() == false) {
                    throw e;
                }
            }
        }
    }

    private synchronized void stopScheduling() {
        if (cancellable != null && cancellable.isCancelled() == false) {
            logger.debug("Stopping ML inference autoscaler");
            cancellable.cancel();
            cancellable = null;
        }
    }

    private synchronized void trigger() {
        getDeploymentStats(ActionListener.wrap(this::processDeploymentStats, e -> logger.warn("Error in inference autoscaling", e)));
    }

    private synchronized void getDeploymentStats(ActionListener<GetDeploymentStatsAction.Response> processDeploymentStats) {
        String deploymentIds = String.join(",", autoscalers.keySet());
        ClientHelper.executeAsyncWithOrigin(
            client,
            ClientHelper.ML_ORIGIN,
            GetDeploymentStatsAction.INSTANCE,
            new GetDeploymentStatsAction.Request(deploymentIds),
            processDeploymentStats
        );
    }

    private synchronized void processDeploymentStats(GetDeploymentStatsAction.Response statsResponse) {
        Map<String, Stats> recentStatsByDeployment = new HashMap<>();
        Map<String, Integer> numberOfAllocations = new HashMap<>();

        for (AssignmentStats assignmentStats : statsResponse.getStats().results()) {
            numberOfAllocations.put(assignmentStats.getDeploymentId(), assignmentStats.getNumberOfAllocations());
            for (AssignmentStats.NodeStats nodeStats : assignmentStats.getNodeStats()) {
                String statsId = assignmentStats.getDeploymentId() + "@" + nodeStats.getNode().getId();
                Stats lastStats = lastInferenceStatsByDeploymentNode.get(statsId);
                Stats nextStats = new Stats(
                    nodeStats.getInferenceCount().orElse(0L),
                    nodeStats.getPendingCount() == null ? 0 : nodeStats.getPendingCount(),
                    nodeStats.getErrorCount() + nodeStats.getTimeoutCount() + nodeStats.getRejectedExecutionCount(),
                    nodeStats.getAvgInferenceTime().orElse(0.0) / 1000.0
                );
                lastInferenceStatsByDeploymentNode.put(statsId, nextStats);

                Stats recentStats = (lastStats == null ? nextStats : nextStats.sub(lastStats));
                recentStatsByDeployment.compute(
                    assignmentStats.getDeploymentId(),
                    (key, value) -> value == null ? recentStats : value.add(recentStats)
                );
            }
        }

        for (Map.Entry<String, Stats> deploymentAndStats : recentStatsByDeployment.entrySet()) {
            String deploymentId = deploymentAndStats.getKey();
            Stats stats = deploymentAndStats.getValue();
            Autoscaler autoscaler = autoscalers.get(deploymentId);
            autoscaler.process(stats, timeIntervalSeconds, numberOfAllocations.get(deploymentId));
            Integer newNumberOfAllocations = autoscaler.autoscale();
            if (newNumberOfAllocations != null) {
                UpdateTrainedModelDeploymentAction.Request updateRequest = new UpdateTrainedModelDeploymentAction.Request(deploymentId);
                updateRequest.setNumberOfAllocations(newNumberOfAllocations);
                ClientHelper.executeAsyncWithOrigin(
                    client,
                    ClientHelper.ML_ORIGIN,
                    UpdateTrainedModelDeploymentAction.INSTANCE,
                    updateRequest,
                    ActionListener.wrap(
                        updateResponse -> logger.info(
                            "Autoscaled deployment [{}] to [{}] allocations.",
                            deploymentId,
                            newNumberOfAllocations
                        ),
                        e -> logger.atLevel(Level.WARN)
                            .withThrowable(e)
                            .log("Autoscaling deployment [{}] to [{}] allocations failed.", deploymentId, newNumberOfAllocations)
                    )
                );
            }
        }
    }
}
