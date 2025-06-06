/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingInfo;
import org.elasticsearch.xpack.core.ml.inference.assignment.RoutingState;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignmentMetadata;
import org.elasticsearch.xpack.ml.dataframe.DataFrameAnalyticsManager;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.xpack.core.ml.MachineLearningField.USE_AUTO_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATAFEED_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME;
import static org.elasticsearch.xpack.core.ml.MlTasks.JOB_TASK_NAME;
import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD;

/**
 * This class adds two types of ML metrics to the meter registry, such that they can be collected by Elastic APM.
 * <p>
 * 1. Per-node ML native memory statistics for ML nodes
 * 2. Cluster-wide job/model statuses for master-eligible nodes
 * <p>
 * The memory metrics relate solely to the ML node they are collected from.
 * <p>
 * The job/model metrics are cluster-wide because a key problem we want to be able to detect is when there are
 * jobs or models that are not assigned to any node. The consumer of the data needs to account for the fact that
 * multiple master-eligible nodes are reporting the same information. The es.ml.is_master attribute in the records
 * indicates which one was actually master, so can be used to deduplicate.
 */
public final class MlMetrics extends AbstractLifecycleComponent implements ClusterStateListener {

    private static final Logger logger = LogManager.getLogger(MlMetrics.class);

    private final MeterRegistry meterRegistry;
    private final ClusterService clusterService;
    private final AutodetectProcessManager autodetectProcessManager;
    private final DataFrameAnalyticsManager dataFrameAnalyticsManager;
    private final boolean hasMasterRole;
    private final boolean hasMlRole;
    private final List<AutoCloseable> metrics = new ArrayList<>();

    private static final Map<String, Object> MASTER_TRUE_MAP = Map.of("es.ml.is_master", Boolean.TRUE);
    private static final Map<String, Object> MASTER_FALSE_MAP = Map.of("es.ml.is_master", Boolean.FALSE);
    private volatile Map<String, Object> isMasterMap = MASTER_FALSE_MAP;
    private volatile boolean firstTime = true;

    private volatile MlTaskStatusCounts mlTaskStatusCounts = MlTaskStatusCounts.EMPTY;
    private volatile TrainedModelAllocationCounts trainedModelAllocationCounts = TrainedModelAllocationCounts.EMPTY;

    private volatile long nativeMemLimit;
    private volatile long nativeMemAdUsage;
    private volatile long nativeMemDfaUsage;
    private volatile long nativeMemTrainedModelUsage;
    private volatile long nativeMemFree;

    public MlMetrics(
        MeterRegistry meterRegistry,
        ClusterService clusterService,
        Settings settings,
        AutodetectProcessManager autodetectProcessManager,
        DataFrameAnalyticsManager dataFrameAnalyticsManager
    ) {
        this.meterRegistry = meterRegistry;
        this.clusterService = clusterService;
        this.autodetectProcessManager = autodetectProcessManager;
        this.dataFrameAnalyticsManager = dataFrameAnalyticsManager;
        hasMasterRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.MASTER_ROLE);
        hasMlRole = DiscoveryNode.hasRole(settings, DiscoveryNodeRole.ML_ROLE);
        if (hasMasterRole || hasMlRole) {
            clusterService.addListener(this);
        }
    }

    private void registerMlNodeMetrics(MeterRegistry meterRegistry) {
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.native_memory.limit.size",
                "ML native memory limit on this node.",
                "bytes",
                () -> new LongWithAttributes(nativeMemLimit, Map.of())
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.native_memory.anomaly_detectors.usage",
                "ML native memory used by anomaly detection jobs on this node.",
                "bytes",
                () -> new LongWithAttributes(nativeMemAdUsage, Map.of())
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.native_memory.data_frame_analytics.usage",
                "ML native memory used by data frame analytics jobs on this node.",
                "bytes",
                () -> new LongWithAttributes(nativeMemDfaUsage, Map.of())
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.native_memory.trained_models.usage",
                "ML native memory used by trained models on this node.",
                "bytes",
                () -> new LongWithAttributes(nativeMemTrainedModelUsage, Map.of())
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.native_memory.free.size",
                "Free ML native memory on this node.",
                "bytes",
                () -> new LongWithAttributes(nativeMemFree, Map.of())
            )
        );
    }

    private void registerMasterNodeMetrics(MeterRegistry meterRegistry) {
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.anomaly_detectors.opening.current",
                "Count of anomaly detection jobs in the opening state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.adOpeningCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.anomaly_detectors.opened.current",
                "Count of anomaly detection jobs in the opened state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.adOpenedCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.anomaly_detectors.closing.current",
                "Count of anomaly detection jobs in the closing state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.adClosingCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.anomaly_detectors.failed.current",
                "Count of anomaly detection jobs in the failed state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.adFailedCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.datafeeds.starting.current",
                "Count of datafeeds in the starting state cluster-wide.",
                "datafeeds",
                () -> new LongWithAttributes(mlTaskStatusCounts.datafeedStartingCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.datafeeds.started.current",
                "Count of datafeeds in the started state cluster-wide.",
                "datafeeds",
                () -> new LongWithAttributes(mlTaskStatusCounts.datafeedStartedCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.datafeeds.stopping.current",
                "Count of datafeeds in the stopping state cluster-wide.",
                "datafeeds",
                () -> new LongWithAttributes(mlTaskStatusCounts.datafeedStoppingCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.data_frame_analytics.starting.current",
                "Count of data frame analytics jobs in the starting state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.dfaStartingCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.data_frame_analytics.started.current",
                "Count of data frame analytics jobs in the started state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.dfaStartedCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.data_frame_analytics.reindexing.current",
                "Count of data frame analytics jobs in the reindexing state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.dfaReindexingCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.data_frame_analytics.analyzing.current",
                "Count of data frame analytics jobs in the analyzing state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.dfaAnalyzingCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.data_frame_analytics.stopping.current",
                "Count of data frame analytics jobs in the stopping state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.dfaStoppingCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.data_frame_analytics.failed.current",
                "Count of data frame analytics jobs in the failed state cluster-wide.",
                "jobs",
                () -> new LongWithAttributes(mlTaskStatusCounts.dfaFailedCount, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.trained_models.deployment.target_allocations.current",
                "Sum of target trained model allocations across all deployments cluster-wide.",
                "allocations",
                () -> new LongWithAttributes(trainedModelAllocationCounts.trainedModelsTargetAllocations, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.trained_models.deployment.current_allocations.current",
                "Sum of current trained model allocations across all deployments cluster-wide.",
                "allocations",
                () -> new LongWithAttributes(trainedModelAllocationCounts.trainedModelsCurrentAllocations, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.trained_models.deployment.failed_allocations.current",
                "Sum of failed trained model allocations across all deployments cluster-wide.",
                "allocations",
                () -> new LongWithAttributes(trainedModelAllocationCounts.trainedModelsFailedAllocations, isMasterMap)
            )
        );
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.trained_models.deployment.fixed_allocations.current",
                "Sum of current trained model allocations that do not use adaptive allocations (either enabled or disabled)",
                "allocations",
                () -> new LongWithAttributes(trainedModelAllocationCounts.trainedModelsFixedAllocations, isMasterMap)
            )
        );
        /*
         * AdaptiveAllocationsScalerService tracks the number of allocations with adaptive allocations enabled.
         */
        metrics.add(
            meterRegistry.registerLongGauge(
                "es.ml.trained_models.deployment.disabled_adaptive_allocations.current",
                "Sum of current trained model allocations that have adaptive allocations disabled",
                "allocations",
                () -> new LongWithAttributes(trainedModelAllocationCounts.trainedModelsDisabledAdaptiveAllocations, isMasterMap)
            )
        );
    }

    @Override
    protected void doStart() {
        metrics.clear();
        if (hasMasterRole) {
            registerMasterNodeMetrics(meterRegistry);
        }
        if (hasMlRole) {
            registerMlNodeMetrics(meterRegistry);
        }
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        metrics.forEach(metric -> {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("metrics close() method should not throw Exception", e);
            }
        });
    }

    /**
     * Metric values are recalculated in response to cluster state changes and then cached.
     * This means that the telemetry provider can poll the metrics registry as often as it
     * likes without causing extra work in recalculating the metric values.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        isMasterMap = event.localNodeMaster() ? MASTER_TRUE_MAP : MASTER_FALSE_MAP;

        if (event.state().blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)) {
            // Wait until the gateway has recovered from disk.
            return;
        }

        boolean mustRecalculateFreeMem = false;

        final ClusterState currentState = event.state();
        final ClusterState previousState = event.previousState();

        if (firstTime || event.metadataChanged()) {
            final PersistentTasksCustomMetadata tasks = currentState.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
            final PersistentTasksCustomMetadata oldTasks = firstTime
                ? null
                : previousState.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
            if (tasks != null && tasks.equals(oldTasks) == false) {
                if (hasMasterRole) {
                    mlTaskStatusCounts = findTaskStatuses(tasks);
                }
                if (hasMlRole) {
                    nativeMemAdUsage = findAdMemoryUsage(autodetectProcessManager);
                    nativeMemDfaUsage = findDfaMemoryUsage(dataFrameAnalyticsManager, tasks);
                    mustRecalculateFreeMem = true;
                }
            }
        }

        final TrainedModelAssignmentMetadata currentMetadata = TrainedModelAssignmentMetadata.fromState(currentState);
        final TrainedModelAssignmentMetadata previousMetadata = firstTime ? null : TrainedModelAssignmentMetadata.fromState(previousState);
        if (currentMetadata != null && currentMetadata.equals(previousMetadata) == false) {
            if (hasMasterRole) {
                trainedModelAllocationCounts = findTrainedModelAllocationCounts(currentMetadata);
            }
            if (hasMlRole) {
                nativeMemTrainedModelUsage = findTrainedModelMemoryUsage(currentMetadata, currentState.nodes().getLocalNode().getId());
                mustRecalculateFreeMem = true;
            }
        }

        if (firstTime) {
            firstTime = false;
            nativeMemLimit = findNativeMemoryLimit(currentState.nodes().getLocalNode(), clusterService.getClusterSettings());
            mustRecalculateFreeMem = true;
            // Install a listener to recalculate limit and free in response to settings changes.
            // This isn't done in the constructor, but instead only after the three usage variables
            // have been populated. Doing this means that immediately after startup, when the stats
            // are inaccurate, they'll _all_ be zero. Installing the settings listeners immediately
            // could mean that free would be misleadingly set based on zero usage when actual usage
            // is _not_ zero.
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(USE_AUTO_MACHINE_MEMORY_PERCENT, s -> memoryLimitClusterSettingUpdated());
            clusterService.getClusterSettings()
                .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, s -> memoryLimitClusterSettingUpdated());
        }

        if (mustRecalculateFreeMem) {
            nativeMemFree = findNativeMemoryFree(nativeMemLimit, nativeMemAdUsage, nativeMemDfaUsage, nativeMemTrainedModelUsage);
        }
    }

    /**
     * This method is registered to be called whenever a cluster setting is changed that affects
     * any of the calculations this class performs.
     */
    private void memoryLimitClusterSettingUpdated() {
        nativeMemLimit = findNativeMemoryLimit(clusterService.localNode(), clusterService.getClusterSettings());
        nativeMemFree = findNativeMemoryFree(nativeMemLimit, nativeMemAdUsage, nativeMemDfaUsage, nativeMemTrainedModelUsage);
    }

    /**
     * Returns up-to-date stats about the states of the ML entities that are persistent tasks.
     * Currently this includes:
     * - Anomaly detection jobs
     * - Datafeeds
     * - Data frame analytics jobs
     * <p>
     * In the future it could possibly also include model snapshot upgrade tasks.
     * <p>
     * These stats relate to the whole cluster and <em>not</em> just the current node.
     * <p>
     * The caller is expected to cache the returned stats to avoid unnecessary recalculation.
     */
    static MlTaskStatusCounts findTaskStatuses(PersistentTasksCustomMetadata tasks) {

        int adOpeningCount = 0;
        int adOpenedCount = 0;
        int adClosingCount = 0;
        int adFailedCount = 0;
        int datafeedStartingCount = 0;
        int datafeedStartedCount = 0;
        int datafeedStoppingCount = 0;
        int dfaStartingCount = 0;
        int dfaStartedCount = 0;
        int dfaReindexingCount = 0;
        int dfaAnalyzingCount = 0;
        int dfaStoppingCount = 0;
        int dfaFailedCount = 0;

        for (PersistentTasksCustomMetadata.PersistentTask<?> task : tasks.tasks()) {
            switch (task.getTaskName()) {
                case JOB_TASK_NAME:
                    switch (MlTasks.getJobStateModifiedForReassignments(task)) {
                        case OPENING -> ++adOpeningCount;
                        case OPENED -> ++adOpenedCount;
                        case CLOSING -> ++adClosingCount;
                        case FAILED -> ++adFailedCount;
                    }
                    break;
                case DATAFEED_TASK_NAME:
                    switch (MlTasks.getDatafeedState(task)) {
                        case STARTING -> ++datafeedStartingCount;
                        case STARTED -> ++datafeedStartedCount;
                        case STOPPING -> ++datafeedStoppingCount;
                    }
                    break;
                case DATA_FRAME_ANALYTICS_TASK_NAME:
                    switch (MlTasks.getDataFrameAnalyticsState(task)) {
                        case STARTING -> ++dfaStartingCount;
                        case STARTED -> ++dfaStartedCount;
                        case REINDEXING -> ++dfaReindexingCount;
                        case ANALYZING -> ++dfaAnalyzingCount;
                        case STOPPING -> ++dfaStoppingCount;
                        case FAILED -> ++dfaFailedCount;
                    }
                    break;
                case JOB_SNAPSHOT_UPGRADE_TASK_NAME:
                    // Not currently tracked
                    // TODO: consider in the future, especially when we're at the stage of needing to upgrade serverless model snapshots
                    break;
            }
        }

        return new MlTaskStatusCounts(
            adOpeningCount,
            adOpenedCount,
            adClosingCount,
            adFailedCount,
            datafeedStartingCount,
            datafeedStartedCount,
            datafeedStoppingCount,
            dfaStartingCount,
            dfaStartedCount,
            dfaReindexingCount,
            dfaAnalyzingCount,
            dfaStoppingCount,
            dfaFailedCount
        );
    }

    /**
     * Return the memory usage, in bytes, of the anomaly detection jobs that are running on the
     * current node.
     */
    static long findAdMemoryUsage(AutodetectProcessManager autodetectProcessManager) {
        return autodetectProcessManager.getOpenProcessMemoryUsage().getBytes();
    }

    /**
     * Return the memory usage, in bytes, of the data frame analytics jobs that are running on the
     * current node.
     */
    static long findDfaMemoryUsage(DataFrameAnalyticsManager dataFrameAnalyticsManager, PersistentTasksCustomMetadata tasks) {
        return dataFrameAnalyticsManager.getActiveTaskMemoryUsage(tasks).getBytes();
    }

    /**
     * Returns up-to-date stats about the numbers of allocations of ML trained models.
     * <p>
     * These stats relate to the whole cluster and <em>not</em> just the current node.
     * <p>
     * The caller is expected to cache the returned stats to avoid unnecessary recalculation.
     */
    static TrainedModelAllocationCounts findTrainedModelAllocationCounts(TrainedModelAssignmentMetadata metadata) {
        int trainedModelsTargetAllocations = 0;
        int trainedModelsCurrentAllocations = 0;
        int trainedModelsFailedAllocations = 0;
        int trainedModelsFixedAllocations = 0;
        int trainedModelsDisabledAdaptiveAllocations = 0;

        for (TrainedModelAssignment trainedModelAssignment : metadata.allAssignments().values()) {
            trainedModelsTargetAllocations += trainedModelAssignment.totalTargetAllocations();
            trainedModelsFailedAllocations += trainedModelAssignment.totalFailedAllocations();

            trainedModelsCurrentAllocations += trainedModelAssignment.totalCurrentAllocations();
            if (trainedModelAssignment.getAdaptiveAllocationsSettings() == null) {
                trainedModelsFixedAllocations += trainedModelAssignment.totalCurrentAllocations();
            } else if ((trainedModelAssignment.getAdaptiveAllocationsSettings().getEnabled() == null)
                || (trainedModelAssignment.getAdaptiveAllocationsSettings().getEnabled() == false)) {
                    trainedModelsDisabledAdaptiveAllocations += trainedModelAssignment.totalCurrentAllocations();
                }
        }

        return new TrainedModelAllocationCounts(
            trainedModelsTargetAllocations,
            trainedModelsCurrentAllocations,
            trainedModelsFailedAllocations,
            trainedModelsFixedAllocations,
            trainedModelsDisabledAdaptiveAllocations
        );
    }

    /**
     * Return the memory usage, in bytes, of the trained models that are running on the
     * current node.
     */
    static long findTrainedModelMemoryUsage(TrainedModelAssignmentMetadata metadata, String localNodeId) {
        long trainedModelMemoryUsageBytes = 0;
        for (TrainedModelAssignment assignment : metadata.allAssignments().values()) {
            if (Optional.ofNullable(assignment.getNodeRoutingTable().get(localNodeId))
                .map(RoutingInfo::getState)
                .orElse(RoutingState.STOPPED)
                .consumesMemory()) {
                trainedModelMemoryUsageBytes += assignment.getTaskParams().estimateMemoryUsageBytes();
            }
        }
        return trainedModelMemoryUsageBytes;
    }

    /**
     * Return the maximum amount of memory, in bytes, permitted for ML processes running on the
     * current node.
     */
    static long findNativeMemoryLimit(DiscoveryNode localNode, ClusterSettings settings) {
        return NativeMemoryCalculator.allowedBytesForMl(localNode, settings).orElse(0L);
    }

    /**
     * Return the amount of free memory, in bytes, that remains available for ML processes running on the
     * current node.
     */
    static long findNativeMemoryFree(long nativeMemLimit, long nativeMemAdUsage, long nativeMemDfaUsage, long nativeMemTrainedModelUsage) {
        long totalUsage = nativeMemAdUsage + nativeMemDfaUsage + nativeMemTrainedModelUsage;
        if (totalUsage > 0) {
            totalUsage += NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
        }
        return nativeMemLimit - totalUsage;
    }

    record MlTaskStatusCounts(
        int adOpeningCount,
        int adOpenedCount,
        int adClosingCount,
        int adFailedCount,
        int datafeedStartingCount,
        int datafeedStartedCount,
        int datafeedStoppingCount,
        int dfaStartingCount,
        int dfaStartedCount,
        int dfaReindexingCount,
        int dfaAnalyzingCount,
        int dfaStoppingCount,
        int dfaFailedCount
    ) {
        static final MlTaskStatusCounts EMPTY = new MlTaskStatusCounts(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
    }

    record TrainedModelAllocationCounts(
        int trainedModelsTargetAllocations,
        int trainedModelsCurrentAllocations,
        int trainedModelsFailedAllocations,
        int trainedModelsFixedAllocations,
        int trainedModelsDisabledAdaptiveAllocations
    ) {
        static final TrainedModelAllocationCounts EMPTY = new TrainedModelAllocationCounts(0, 0, 0, 0, 0);
    }
}
