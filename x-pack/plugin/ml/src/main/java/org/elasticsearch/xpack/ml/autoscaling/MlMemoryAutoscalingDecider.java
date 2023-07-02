/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.autoscaling;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.inference.assignment.TrainedModelAssignment;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.MlProcessors;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Stream;

import static java.time.Instant.ofEpochMilli;
import static org.elasticsearch.common.xcontent.XContentElasticsearchExtension.DEFAULT_FORMATTER;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_MACHINE_MEMORY_PERCENT;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;
import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD;

class MlMemoryAutoscalingDecider {

    private static final Logger logger = LogManager.getLogger(MlMemoryAutoscalingDecider.class);

    private static final String MEMORY_STALE = "unable to make scaling decision as job memory requirements are stale";
    // If ensureScaleDown changes the calculation by more than this much, log the error
    private static final long ACCEPTABLE_DIFFERENCE = ByteSizeValue.ofMb(1).getBytes();

    private final MlMemoryTracker mlMemoryTracker;
    private final NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper;
    private final NodeLoadDetector nodeLoadDetector;
    private final ScaleTimer scaleTimer;

    private volatile int maxMachineMemoryPercent;
    private volatile int maxOpenJobs;
    private volatile boolean useAuto;
    private volatile long mlNativeMemoryForLargestMlNode;

    MlMemoryAutoscalingDecider(
        Settings settings,
        ClusterService clusterService,
        NodeAvailabilityZoneMapper nodeAvailabilityZoneMapper,
        NodeLoadDetector nodeLoadDetector,
        ScaleTimer scaleTimer
    ) {
        this.nodeAvailabilityZoneMapper = Objects.requireNonNull(nodeAvailabilityZoneMapper);
        this.nodeLoadDetector = Objects.requireNonNull(nodeLoadDetector);
        this.mlMemoryTracker = Objects.requireNonNull(nodeLoadDetector.getMlMemoryTracker());
        this.scaleTimer = Objects.requireNonNull(scaleTimer);

        this.maxMachineMemoryPercent = MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxOpenJobs = MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.useAuto = MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings);
        setMaxMlNodeSize(MachineLearning.MAX_ML_NODE_SIZE.get(settings));
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_MACHINE_MEMORY_PERCENT, this::setMaxMachineMemoryPercent);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT, this::setUseAuto);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_ML_NODE_SIZE, this::setMaxMlNodeSize);
    }

    void setMaxMachineMemoryPercent(int maxMachineMemoryPercent) {
        this.maxMachineMemoryPercent = maxMachineMemoryPercent;
    }

    void setMaxOpenJobs(int maxOpenJobs) {
        this.maxOpenJobs = maxOpenJobs;
    }

    void setUseAuto(boolean useAuto) {
        this.useAuto = useAuto;
    }

    void setMaxMlNodeSize(ByteSizeValue maxMlNodeSize) {
        long maxMlNodeSizeBytes = maxMlNodeSize.getBytes();
        // 0 means no known max size
        if (maxMlNodeSizeBytes <= 0) {
            mlNativeMemoryForLargestMlNode = Long.MAX_VALUE;
        } else {
            mlNativeMemoryForLargestMlNode = NativeMemoryCalculator.allowedBytesForMl(maxMlNodeSizeBytes, maxMachineMemoryPercent, useAuto);
        }
    }

    public MlMemoryAutoscalingCapacity scale(Settings configuration, AutoscalingDeciderContext context, MlAutoscalingContext mlContext) {
        final ClusterState clusterState = context.state();

        scaleTimer.lastScaleToScaleIntervalMillis()
            .ifPresent(scaleInterval -> mlMemoryTracker.setAutoscalingCheckInterval(Duration.ofMillis(scaleInterval)));
        final int numAnalyticsJobsInQueue = MlAutoscalingDeciderService.NUM_ANALYTICS_JOBS_IN_QUEUE.get(configuration);
        final int numAnomalyJobsInQueue = MlAutoscalingDeciderService.NUM_ANOMALY_JOBS_IN_QUEUE.get(configuration);
        final NativeMemoryCapacity currentScale = currentScale(mlContext.mlNodes);

        // There are no ML nodes, scale up as quick as possible, no matter if memory is stale or not
        if (mlContext.mlNodes.isEmpty() && mlContext.hasWaitingTasks()) {
            return scaleUpFromZero(mlContext);
        }

        // This is the sole check for memory staleness. It's possible that memory becomes stale while we execute the rest
        // of the code of this method, but it's best that all the code runs with the same view of whether the last refresh
        // was done in time.
        if (mlMemoryTracker.isRecentlyRefreshed() == false) {
            logger.debug(
                "view of job memory is stale given duration [{}]. Not attempting to make scaling decision",
                mlMemoryTracker.getStalenessDuration()
            );
            return refreshMemoryTrackerAndBuildEmptyDecision(MEMORY_STALE);
        }
        // We need the current node loads to determine if we need to scale up or down
        List<NodeLoad> nodeLoads = new ArrayList<>(mlContext.mlNodes.size());
        boolean nodeLoadIsMemoryAccurate = true;
        for (DiscoveryNode node : mlContext.mlNodes) {
            NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(clusterState, node, maxOpenJobs, maxMachineMemoryPercent, useAuto);
            if (nodeLoad.getError() != null) {
                logger.warn("[{}] failed to gather node load limits, failure [{}]. Returning no scale", node.getId(), nodeLoad.getError());
                return refreshMemoryTrackerAndBuildEmptyDecision(
                    "Passing currently perceived capacity as there was a failure gathering node limits [" + nodeLoad.getError() + "]"
                );
            }
            nodeLoads.add(nodeLoad);
            if (nodeLoad.isUseMemory() == false) {
                nodeLoadIsMemoryAccurate = false;
                logger.debug("[{}] failed to gather node load - memory usage for one or more tasks not available.", node.getId());
            }
        }
        // This is an exceptional case, the memory tracking became stale between us checking previously and calculating the loads (for
        // example because a new job started that hasn't yet been added to the memory tracker). We should return a no scale in this case.
        if (nodeLoadIsMemoryAccurate == false) {
            return refreshMemoryTrackerAndBuildEmptyDecision(
                "Passing currently perceived capacity as nodes were unable to provide an accurate view of their memory usage"
            );
        }

        final Optional<MlMemoryAutoscalingCapacity> scaleUpDecision = checkForScaleUp(
            numAnomalyJobsInQueue,
            numAnalyticsJobsInQueue,
            nodeLoads,
            mlContext.waitingAnomalyJobs,
            mlContext.waitingSnapshotUpgrades,
            mlContext.waitingAnalyticsJobs,
            mlContext.waitingAllocatedModels,
            calculateFutureAvailableCapacity(mlContext.persistentTasks, nodeLoads).orElse(null),
            currentScale
        );
        if (scaleUpDecision.isPresent()) {
            scaleTimer.resetScaleDownCoolDown();
            return scaleUpDecision.get();
        }

        final List<String> partiallyAllocatedModels = mlContext.findPartiallyAllocatedModels();

        if (mlContext.waitingAnalyticsJobs.isEmpty() == false
            || mlContext.waitingSnapshotUpgrades.isEmpty() == false
            || mlContext.waitingAnomalyJobs.isEmpty() == false
            || partiallyAllocatedModels.isEmpty() == false) {
            // We don't want to continue to consider a scale down if there are now waiting jobs
            scaleTimer.resetScaleDownCoolDown();
            return MlMemoryAutoscalingCapacity.from(context.currentCapacity())
                .setReason(
                    String.format(
                        Locale.ROOT,
                        "Passing currently perceived capacity as there are [%d] model snapshot upgrades, "
                            + "[%d] analytics and [%d] anomaly detection jobs in the queue, "
                            + "[%d] trained models not fully-allocated, "
                            + "but the number in the queue is less than the configured maximum allowed "
                            + "or the queued jobs will eventually be assignable at the current size.",
                        mlContext.waitingSnapshotUpgrades.size(),
                        mlContext.waitingAnalyticsJobs.size(),
                        mlContext.waitingAnomalyJobs.size(),
                        partiallyAllocatedModels.size()
                    )
                )
                .build();
        }

        long maxTaskMemoryBytes = maxMemoryBytes(mlContext);

        // This should rarely happen, it could imply a bug. However, it is possible to happen
        // if there are persistent tasks that do not have matching configs stored.
        // Also, it could be that we have tasks where the required job memory is 0, which should be impossible.
        // This can also happen if a job that is awaiting assignment ceases to have the AWAITING_LAZY_ASSIGNMENT
        // assignment explanation, for example because some other explanation overrides it. (This second situation
        // arises because, for example, anomalyDetectionTasks contains a task that is waiting but waitingAnomalyJobs
        // doesn't because its assignment explanation didn't match AWAITING_LAZY_ASSIGNMENT.)
        if (maxTaskMemoryBytes == 0L) {
            // We shouldn't need to check this condition because it's the exact opposite of the condition that
            // would have sent us down the scale down to zero branch higher up this method.
            assert mlContext.isEmpty() == false : "No tasks or models at all should have put us in the scale down to zero branch";
            logger.warn(
                "The calculated minimum required node size was unexpectedly [0] as there are [{}] anomaly job tasks, "
                    + "[{}] model snapshot upgrade tasks, [{}] data frame analytics tasks and [{}] model assignments",
                mlContext.anomalyDetectionTasks.size(),
                mlContext.snapshotUpgradeTasks.size(),
                mlContext.dataframeAnalyticsTasks.size(),
                mlContext.modelAssignments.size()
            );
            // This next message could obviously be pretty big, but should only get logged very rarely as it
            // requires both debug enabled and some other bug to exist to cause us to be in this branch
            logger.debug(
                () -> format(
                    "persistent tasks that caused unexpected scaling situation: [%s]",
                    (mlContext.persistentTasks == null) ? "null" : Strings.toString(mlContext.persistentTasks)
                )
            );
            return refreshMemoryTrackerAndBuildEmptyDecision(
                "Passing currently perceived capacity as there are running analytics and anomaly jobs or deployed models, "
                    + "but their assignment explanations are unexpected or their memory usage estimates are inaccurate."
            );
        }

        final Optional<MlMemoryAutoscalingCapacity> maybeScaleDown = checkForScaleDown(nodeLoads, maxTaskMemoryBytes, currentScale)
            // Due to rounding bugs, it may be that a scale down result COULD cause a scale up.
            // Ensuring the scaleDown here forces the scale down result to always be lower than the current capacity.
            // This is safe as we know that ALL jobs are assigned at the current capacity.
            .map(result -> {
                MlMemoryAutoscalingCapacity capacity = ensureScaleDown(
                    result,
                    MlMemoryAutoscalingCapacity.from(context.currentCapacity()).build()
                );
                if (capacity == null) {
                    return null;
                }
                // We should keep this check here as well as in the processor decider while cloud is not
                // reacting to processor autoscaling.
                if (modelAssignmentsRequireMoreThanHalfCpu(mlContext.modelAssignments.values(), mlContext.mlNodes)) {
                    logger.debug("not down-scaling; model assignments require more than half of the ML tier's allocated processors");
                    return null;
                }
                return capacity;
            });
        if (maybeScaleDown.isPresent()) {
            final MlMemoryAutoscalingCapacity scaleDownDecisionResult = maybeScaleDown.get();

            // Given maxOpenJobs, could we scale down to just one node?
            // We have no way of saying "we need X nodes"
            if (nodeLoads.size() > 1) {
                long totalAssignedJobs = nodeLoads.stream().mapToLong(NodeLoad::getNumAssignedJobsAndModels).sum();
                // one volatile read
                long maxOpenJobsCopy = this.maxOpenJobs;
                if (totalAssignedJobs > maxOpenJobsCopy) {
                    String msg = String.format(
                        Locale.ROOT,
                        "not scaling down as the total number of jobs [%d] exceeds the setting [%s (%d)]. "
                            + "To allow a scale down [%s] must be increased.",
                        totalAssignedJobs,
                        MAX_OPEN_JOBS_PER_NODE.getKey(),
                        maxOpenJobsCopy,
                        MAX_OPEN_JOBS_PER_NODE.getKey()
                    );
                    logger.info(() -> format("%s Calculated potential scaled down capacity [%s]", msg, scaleDownDecisionResult));
                    return MlMemoryAutoscalingCapacity.from(context.currentCapacity()).setReason(msg).build();
                }
            }

            long msLeftToScale = scaleTimer.markDownScaleAndGetMillisLeftFromDelay(configuration);
            if (msLeftToScale <= 0) {
                return scaleDownDecisionResult;
            }
            TimeValue downScaleDelay = MlAutoscalingDeciderService.DOWN_SCALE_DELAY.get(configuration);
            logger.debug(
                () -> format(
                    "not scaling down as the current scale down delay [%s] is not satisfied."
                        + " The last time scale down was detected [%s]. Calculated scaled down capacity [%s] ",
                    downScaleDelay.getStringRep(),
                    DEFAULT_FORMATTER.format(ofEpochMilli(scaleTimer.downScaleDetectedMillis())),
                    scaleDownDecisionResult
                )
            );
            return MlMemoryAutoscalingCapacity.from(context.currentCapacity())
                .setReason(
                    String.format(
                        Locale.ROOT,
                        "Passing currently perceived capacity as down scale delay has not been satisfied; configured delay [%s] "
                            + "last detected scale down event [%s]. Will request scale down in approximately [%s]",
                        downScaleDelay.getStringRep(),
                        XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(scaleTimer.downScaleDetectedMillis())),
                        TimeValue.timeValueMillis(msLeftToScale).getStringRep()
                    )
                )
                .build();
        }

        return MlMemoryAutoscalingCapacity.from(context.currentCapacity())
            .setReason("Passing currently perceived capacity as no scaling changes are necessary")
            .build();
    }

    NativeMemoryCapacity currentScale(final List<DiscoveryNode> machineLearningNodes) {
        return NativeMemoryCapacity.currentScale(machineLearningNodes, maxMachineMemoryPercent, useAuto);
    }

    MlMemoryAutoscalingCapacity capacityFromNativeMemory(NativeMemoryCapacity nativeMemoryCapacity) {
        return nativeMemoryCapacity.autoscalingCapacity(
            maxMachineMemoryPercent,
            useAuto,
            mlNativeMemoryForLargestMlNode,
            nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().orElse(1)
        ).build();
    }

    private MlMemoryAutoscalingCapacity refreshMemoryTrackerAndBuildEmptyDecision(String reason) {
        mlMemoryTracker.asyncRefresh();
        return MlMemoryAutoscalingCapacity.builder(null, null).setReason(reason).build();
    }

    private long maxMemoryBytes(MlAutoscalingContext mlContext) {
        long maxMemoryBytes = Math.max(
            mlContext.anomalyDetectionTasks.stream()
                .filter(PersistentTasksCustomMetadata.PersistentTask::isAssigned)
                // Memory SHOULD be recently refreshed, so in our current state, we should at least have an idea of the memory used
                .mapToLong(t -> {
                    Long mem = getAnomalyMemoryRequirement(t);
                    if (mem == null) {
                        logger.warn("unexpected null for anomaly detection memory requirement for [{}]", MlTasks.jobId(t.getId()));
                    }
                    assert mem != null : "unexpected null for anomaly memory requirement after recent stale check";
                    return mem == null ? 0 : mem;
                })
                .max()
                .orElse(0L),
            mlContext.snapshotUpgradeTasks.stream()
                .filter(PersistentTasksCustomMetadata.PersistentTask::isAssigned)
                // Memory SHOULD be recently refreshed, so in our current state, we should at least have an idea of the memory used
                .mapToLong(t -> {
                    Long mem = getAnomalyMemoryRequirement(t);
                    if (mem == null) {
                        logger.warn("unexpected null for snapshot upgrade memory requirement for [{}]", MlTasks.jobId(t.getId()));
                    }
                    assert mem != null : "unexpected null for anomaly memory requirement after recent stale check";
                    return mem == null ? 0 : mem;
                })
                .max()
                .orElse(0L)
        );
        maxMemoryBytes = Math.max(
            maxMemoryBytes,
            mlContext.dataframeAnalyticsTasks.stream()
                .filter(PersistentTasksCustomMetadata.PersistentTask::isAssigned)
                // Memory SHOULD be recently refreshed, so in our current state, we should at least have an idea of the memory used
                .mapToLong(t -> {
                    Long mem = this.getAnalyticsMemoryRequirement(t);
                    if (mem == null) {
                        logger.warn("unexpected null for analytics memory requirement for [{}]", MlTasks.dataFrameAnalyticsId(t.getId()));
                    }
                    assert mem != null : "unexpected null for analytics memory requirement after recent stale check";
                    return mem == null ? 0 : mem;
                })
                .max()
                .orElse(0L)
        );
        maxMemoryBytes = Math.max(
            maxMemoryBytes,
            mlContext.modelAssignments.values().stream().mapToLong(t -> t.getTaskParams().estimateMemoryUsageBytes()).max().orElse(0L)
        );
        return maxMemoryBytes;
    }

    // This doesn't allow any jobs to wait in the queue, this is because in a "normal" scaling event, we also verify if a job
    // can eventually start, and given the current cluster, no job can eventually start.
    MlMemoryAutoscalingCapacity scaleUpFromZero(MlAutoscalingContext mlContext) {
        final Optional<NativeMemoryCapacity> analyticsCapacity = requiredCapacityExcludingPerNodeOverheadForUnassignedJobs(
            mlContext.waitingAnalyticsJobs,
            this::getAnalyticsMemoryRequirement,
            0
        );
        final Optional<NativeMemoryCapacity> anomalyCapacity = requiredCapacityExcludingPerNodeOverheadForUnassignedJobs(
            mlContext.waitingAnomalyJobs,
            this::getAnomalyMemoryRequirement,
            0
        );
        final Optional<NativeMemoryCapacity> snapshotUpgradeCapacity = requiredCapacityExcludingPerNodeOverheadForUnassignedJobs(
            mlContext.waitingSnapshotUpgrades,
            this::getAnomalyMemoryRequirement,
            0
        );
        final Optional<NativeMemoryCapacity> allocatedModelCapacity = requiredCapacityExcludingPerNodeOverheadForUnassignedJobs(
            mlContext.waitingAllocatedModels,
            this::getAllocatedModelRequirement,
            0
        );
        NativeMemoryCapacity updatedCapacity = anomalyCapacity.orElse(NativeMemoryCapacity.ZERO)
            .merge(snapshotUpgradeCapacity.orElse(NativeMemoryCapacity.ZERO))
            .merge(analyticsCapacity.orElse(NativeMemoryCapacity.ZERO))
            .merge(allocatedModelCapacity.orElse(NativeMemoryCapacity.ZERO));
        // If we still have calculated zero, this means the ml memory tracker does not have the required info.
        // So, request a scale for the default. This is only for the 0 -> N scaling case.
        if (updatedCapacity.getNodeMlNativeMemoryRequirementExcludingOverhead() == 0L) {
            updatedCapacity = updatedCapacity.merge(
                new NativeMemoryCapacity(
                    ByteSizeValue.ofMb(AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB).getBytes(),
                    ByteSizeValue.ofMb(AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB).getBytes()
                )
            );
        }
        MlMemoryAutoscalingCapacity.Builder requiredCapacity = updatedCapacity.autoscalingCapacity(
            maxMachineMemoryPercent,
            useAuto,
            mlNativeMemoryForLargestMlNode,
            nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().orElse(1)
        );
        return requiredCapacity.setReason(
            "requesting scale up as number of jobs in queues exceeded configured limit and there are no machine learning nodes"
        ).build();
    }

    /**
     * @param unassignedJobs The list of unassigned jobs
     * @param sizeFunction   Function providing the memory required for a job
     * @param maxNumInQueue  The number of unassigned jobs allowed.
     * @return The capacity needed to reduce the length of `unassignedJobs` to `maxNumInQueue`
     */
    static Optional<NativeMemoryCapacity> requiredCapacityExcludingPerNodeOverheadForUnassignedJobs(
        List<String> unassignedJobs,
        Function<String, Long> sizeFunction,
        int maxNumInQueue
    ) {
        if (unassignedJobs.isEmpty()) {
            return Optional.empty();
        }
        List<Long> jobSizes = unassignedJobs.stream()
            .map(sizeFunction)
            .map(l -> l == null ? 0L : l)
            .sorted(Comparator.comparingLong(Long::longValue).reversed())
            .toList();

        long tierMemory = 0L;
        // Node memory needs to be AT LEAST the size of the largest job + the required overhead.
        long nodeMemory = jobSizes.get(0);
        Iterator<Long> iter = jobSizes.iterator();
        while (jobSizes.size() > maxNumInQueue && iter.hasNext()) {
            tierMemory += iter.next();
            iter.remove();
        }
        return Optional.of(new NativeMemoryCapacity(tierMemory, nodeMemory));
    }

    /**
     * @param numAnomalyJobsInQueue How many anomaly detection jobs (including model snapshot upgrades)
     *                              are permitted to queue for space to become available by other jobs
     *                              completing?
     * @param numAnalyticsJobsInQueue How many data frame analytics jobs are permitted to queue for space
     *                                to become available by other jobs completing?
     * @param nodeLoads Node loads on ML nodes in the current cluster.
     * @param waitingAnomalyJobs Job IDs of waiting anomaly detection jobs.
     * @param waitingSnapshotUpgrades Job IDs of waiting model snapshot upgrades.
     * @param waitingAnalyticsJobs Job IDs of waiting data frame analytics jobs.
     * @param waitingAllocatedModels IDs of waiting trained models that require a native process.
     * @param futureFreedCapacity Optionally, the combination of free memory and memory used by
     *                            jobs that are expected to terminate after completing a batch
     *                            analysis.
     * @param currentScale The current total ML <em>allowance</em> irrespective of what's in use.
     *                     It is <em>not</em> space already used or free space.
     * @return The scale up decision, or {@link Optional#empty} if no decision is made.
     */
    Optional<MlMemoryAutoscalingCapacity> checkForScaleUp(
        int numAnomalyJobsInQueue,
        int numAnalyticsJobsInQueue,
        List<NodeLoad> nodeLoads,
        List<String> waitingAnomalyJobs,
        List<String> waitingSnapshotUpgrades,
        List<String> waitingAnalyticsJobs,
        List<String> waitingAllocatedModels,
        @Nullable NativeMemoryCapacity futureFreedCapacity,
        NativeMemoryCapacity currentScale
    ) {
        logger.debug(
            () -> format(
                "Checking for scale up -"
                    + " waiting data frame analytics jobs [%s]"
                    + " data frame analytics jobs allowed to queue [%s]"
                    + " waiting anomaly detection jobs (including model snapshot upgrades) [%s]"
                    + " anomaly detection jobs allowed to queue [%s]"
                    + " waiting models [%s]"
                    + " future freed capacity [%s]"
                    + " current scale [%s]",
                waitingAnalyticsJobs.size(),
                numAnalyticsJobsInQueue,
                waitingAnomalyJobs.size() + waitingSnapshotUpgrades.size(),
                numAnomalyJobsInQueue,
                waitingAllocatedModels.size(),
                futureFreedCapacity,
                currentScale
            )
        );

        // Are we in breach of maximum waiting jobs?
        if (waitingAnalyticsJobs.size() > numAnalyticsJobsInQueue
            || waitingAnomalyJobs.size() + waitingSnapshotUpgrades.size() > numAnomalyJobsInQueue
            || waitingAllocatedModels.size() > 0) {

            Tuple<NativeMemoryCapacity, List<NodeLoad>> anomalyCapacityAndNewLoad = determineUnassignableJobs(
                Stream.concat(waitingAnomalyJobs.stream(), waitingSnapshotUpgrades.stream()).toList(),
                this::getAnomalyMemoryRequirement,
                NodeLoad.Builder::incNumAssignedAnomalyDetectorJobs,
                numAnomalyJobsInQueue,
                nodeLoads
            ).orElse(Tuple.tuple(NativeMemoryCapacity.ZERO, nodeLoads));

            Tuple<NativeMemoryCapacity, List<NodeLoad>> analyticsCapacityAndNewLoad = determineUnassignableJobs(
                waitingAnalyticsJobs,
                this::getAnalyticsMemoryRequirement,
                NodeLoad.Builder::incNumAssignedDataFrameAnalyticsJobs,
                numAnalyticsJobsInQueue,
                anomalyCapacityAndNewLoad.v2()
            ).orElse(Tuple.tuple(NativeMemoryCapacity.ZERO, anomalyCapacityAndNewLoad.v2()));

            Tuple<NativeMemoryCapacity, List<NodeLoad>> modelCapacityAndNewLoad = determineUnassignableJobs(
                waitingAllocatedModels,
                this::getAllocatedModelRequirement,
                NodeLoad.Builder::incNumAssignedNativeInferenceModels,
                0,
                analyticsCapacityAndNewLoad.v2()
            ).orElse(Tuple.tuple(NativeMemoryCapacity.ZERO, analyticsCapacityAndNewLoad.v2()));

            if (analyticsCapacityAndNewLoad.v1().equals(NativeMemoryCapacity.ZERO)
                && anomalyCapacityAndNewLoad.v1().equals(NativeMemoryCapacity.ZERO)
                && modelCapacityAndNewLoad.v1().equals(NativeMemoryCapacity.ZERO)) {
                logger.debug("no_scale event as current capacity, even though there are waiting jobs, is adequate to run the queued jobs");
                return Optional.empty();
            }

            // We don't have enough information to get a perfect answer here. Even though there
            // are jobs that cannot be assigned, there is likely some free memory on the current
            // nodes. If we don't consider it then we can scale up a level too far. For example,
            // suppose we're currently on a 1GB node with a 21MB job running and a 970MB job waiting.
            // If we scale up to a 2GB node then both will fit. But if we don't consider the free
            // memory on the 1GB node then we'll scale up to 4GB, then later scale back down to 2GB.
            // However, there's a complication. Assigning jobs is in reality a bin-packing problem
            // but we're modelling it as a simple summation problem. If we had 970MB of free space
            // spread over multiple existing nodes then we very well might need to scale up to fit
            // a 970MB job, but subtracting the current free memory from the requirement would lead
            // to us not scaling up at all. We don't have enough control to solve this correctly,
            // but a heuristic that's better than doing nothing is to at least consider the amount
            // of free space on the current node with the most free space and subtract that from the
            // requirement. In our example with the 21MB and 970MB jobs on the 1GB node, we'll then
            // correctly scale to 2GB. The more nodes in the cluster the worse the heuristic will do
            // but it won't ever be worse than doing nothing, many clusters only have a small number
            // of ML nodes, and by the time we get to large nodes the scaling steps are big anyway
            // so we are less likely to incorrectly skip a level due to this problem.
            long maxFreeNodeMemAfterPossibleAssignments = modelCapacityAndNewLoad.v2()
                .stream()
                .filter(nodeLoad -> nodeLoad.getError() == null && nodeLoad.isUseMemory())
                .map(NodeLoad::getFreeMemoryExcludingPerNodeOverhead)
                .max(Long::compareTo)
                .orElse(0L);
            if (maxFreeNodeMemAfterPossibleAssignments > currentScale.getNodeMlNativeMemoryRequirementExcludingOverhead()
                || maxFreeNodeMemAfterPossibleAssignments > currentScale.getTierMlNativeMemoryRequirementExcludingOverhead()) {
                assert false
                    : "highest free node memory after possible assignments ["
                        + maxFreeNodeMemAfterPossibleAssignments
                        + "] greater than current scale ["
                        + currentScale
                        + "]";
                // If we get here in production it means there's a bug somewhere else, but it's
                // better to scale in the pre-8.3 way than not scale at all if this happens
                logger.warn(
                    "Highest free node memory after possible assignments ["
                        + maxFreeNodeMemAfterPossibleAssignments
                        + "] greater than current scale ["
                        + currentScale
                        + "] - will scale up without considering current free memory"
                );
                maxFreeNodeMemAfterPossibleAssignments = 0;
            }

            NativeMemoryCapacity updatedCapacity = new NativeMemoryCapacity(-maxFreeNodeMemAfterPossibleAssignments, 0).merge(currentScale)
                .merge(analyticsCapacityAndNewLoad.v1())
                .merge(anomalyCapacityAndNewLoad.v1())
                .merge(modelCapacityAndNewLoad.v1());
            MlMemoryAutoscalingCapacity requiredCapacity = updatedCapacity.autoscalingCapacity(
                maxMachineMemoryPercent,
                useAuto,
                mlNativeMemoryForLargestMlNode,
                nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().orElse(1)
            )
                .setReason(
                    "requesting scale up as number of jobs in queues exceeded configured limit "
                        + "or there is at least one trained model waiting for assignment "
                        + "and current capacity is not large enough for waiting jobs or models"
                )
                .build();
            return Optional.of(requiredCapacity);
        }

        // Could the currently waiting jobs ever be assigned?
        // NOTE: the previous predicate catches if an allocated model isn't assigned
        if (waitingAnalyticsJobs.isEmpty() == false
            || waitingSnapshotUpgrades.isEmpty() == false
            || waitingAnomalyJobs.isEmpty() == false) {
            // we are unable to determine new tier size, but maybe we can see if our nodes are big enough.
            if (futureFreedCapacity == null) {
                Optional<Long> maxSize = Stream.concat(
                    waitingAnalyticsJobs.stream().map(this::getAnalyticsMemoryRequirement),
                    Stream.concat(
                        waitingAnomalyJobs.stream().map(this::getAnomalyMemoryRequirement),
                        waitingSnapshotUpgrades.stream().map(this::getAnomalyMemoryRequirement)
                    )
                ).filter(Objects::nonNull).max(Long::compareTo);
                if (maxSize.isPresent() && maxSize.get() > currentScale.getNodeMlNativeMemoryRequirementExcludingOverhead()) {
                    MlMemoryAutoscalingCapacity requiredCapacity = new NativeMemoryCapacity(
                        Math.max(currentScale.getTierMlNativeMemoryRequirementExcludingOverhead(), maxSize.get()),
                        maxSize.get()
                    ).autoscalingCapacity(
                        maxMachineMemoryPercent,
                        useAuto,
                        mlNativeMemoryForLargestMlNode,
                        nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().orElse(1)
                    ).setReason("requesting scale up as there is no node large enough to handle queued jobs").build();
                    return Optional.of(requiredCapacity);
                }
                // we have no info, allow the caller to make the appropriate action, probably returning a no_scale
                logger.debug(
                    "Cannot make a scaling decision as future freed capacity is not known and largest job could fit on an existing node"
                );
                return Optional.empty();
            }
            long newTierNeeded = -futureFreedCapacity.getTierMlNativeMemoryRequirementExcludingOverhead();
            // could any of the nodes actually run the job?
            long newNodeMax = currentScale.getNodeMlNativeMemoryRequirementExcludingOverhead();
            for (String analyticsJob : waitingAnalyticsJobs) {
                Long requiredMemory = getAnalyticsMemoryRequirement(analyticsJob);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                newTierNeeded += requiredMemory;
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            for (String anomalyJob : waitingAnomalyJobs) {
                Long requiredMemory = getAnomalyMemoryRequirement(anomalyJob);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                newTierNeeded += requiredMemory;
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            for (String snapshotUpgrade : waitingSnapshotUpgrades) {
                Long requiredMemory = getAnomalyMemoryRequirement(snapshotUpgrade);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                newTierNeeded += requiredMemory;
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            if (newNodeMax > currentScale.getNodeMlNativeMemoryRequirementExcludingOverhead() || newTierNeeded > 0L) {
                NativeMemoryCapacity newCapacity = new NativeMemoryCapacity(Math.max(0L, newTierNeeded), newNodeMax);
                MlMemoryAutoscalingCapacity requiredCapacity = currentScale.merge(newCapacity)
                    .autoscalingCapacity(
                        maxMachineMemoryPercent,
                        useAuto,
                        mlNativeMemoryForLargestMlNode,
                        nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().orElse(1)
                    )
                    .setReason("scaling up as adequate space would not automatically become available when running jobs finish")
                    .build();
                return Optional.of(requiredCapacity);
            }
        }

        return Optional.empty();
    }

    static Optional<Tuple<NativeMemoryCapacity, List<NodeLoad>>> determineUnassignableJobs(
        List<String> unassignedJobs,
        Function<String, Long> sizeFunction,
        Consumer<NodeLoad.Builder> incrementCountFunction,
        int maxNumInQueue,
        List<NodeLoad> nodeLoads
    ) {
        if (unassignedJobs.isEmpty()) {
            return Optional.empty();
        }
        if (unassignedJobs.size() < maxNumInQueue) {
            return Optional.empty();
        }
        PriorityQueue<NodeLoad.Builder> mostFreeMemoryFirst = new PriorityQueue<>(
            nodeLoads.size(),
            // If we have no more remaining jobs, it's the same as having no more free memory
            Comparator.<NodeLoad.Builder>comparingLong(v -> v.remainingJobs() == 0 ? 0L : v.getFreeMemory()).reversed()
        );
        for (NodeLoad load : nodeLoads) {
            mostFreeMemoryFirst.add(NodeLoad.builder(load));
        }
        List<Long> jobSizes = unassignedJobs.stream()
            .map(sizeFunction)
            .map(l -> l == null ? 0L : l)
            .sorted(Comparator.comparingLong(Long::longValue).reversed())
            .toList();

        Iterator<Long> assignmentIter = jobSizes.iterator();
        while (jobSizes.size() > maxNumInQueue && assignmentIter.hasNext()) {
            long requiredMemory = assignmentIter.next();
            long requiredNativeCodeOverhead = 0;
            NodeLoad.Builder nodeLoad = mostFreeMemoryFirst.peek();
            assert nodeLoad != null : "unexpected null value while calculating assignable memory";
            // Add per-node overhead if this is the first assignment
            if (nodeLoad.getNumAssignedJobs() == 0) {
                requiredNativeCodeOverhead = NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
            }
            // Since we have the least loaded node (by memory) first, if it can't fit here, it can't fit anywhere
            if (nodeLoad.getFreeMemory() >= requiredMemory + requiredNativeCodeOverhead) {
                assignmentIter.remove();
                // Remove and add to the priority queue to make sure the biggest node with availability is first
                nodeLoad = mostFreeMemoryFirst.poll();
                incrementCountFunction.accept(nodeLoad);
                mostFreeMemoryFirst.add(
                    nodeLoad.incAssignedNativeCodeOverheadMemory(requiredNativeCodeOverhead)
                        .incAssignedAnomalyDetectorMemory(requiredMemory)
                );
            }
        }
        List<NodeLoad> adjustedLoads = mostFreeMemoryFirst.stream().map(NodeLoad.Builder::build).toList();

        List<Long> unassignableMemory = new ArrayList<>();
        Iterator<Long> unassignableIter = jobSizes.iterator();
        // If we cannot assign enough jobs given the current cluster size
        while (jobSizes.size() > maxNumInQueue && unassignableIter.hasNext()) {
            unassignableMemory.add(unassignableIter.next());
            unassignableIter.remove();
        }
        if (unassignableMemory.isEmpty()) {
            // We don't need to scale but we have adjusted node load given what we could assign
            return Optional.of(Tuple.tuple(NativeMemoryCapacity.ZERO, adjustedLoads));
        }
        return Optional.of(
            Tuple.tuple(
                new NativeMemoryCapacity(
                    unassignableMemory.stream().mapToLong(Long::longValue).sum(),
                    // Node memory excluding overhead needs to be AT LEAST the size of the largest job.
                    unassignableMemory.get(0)
                ),
                adjustedLoads
            )
        );
    }

    Optional<MlMemoryAutoscalingCapacity> checkForScaleDown(
        List<NodeLoad> nodeLoads,
        long largestJob,
        NativeMemoryCapacity currentCapacity
    ) {
        long currentlyNecessaryTier = nodeLoads.stream().mapToLong(NodeLoad::getAssignedJobMemoryExcludingPerNodeOverhead).sum();
        // We consider a scale down if we are not fully utilizing the tier
        // Or our largest job could be on a smaller node (meaning the same size tier but smaller nodes are possible).
        if (currentlyNecessaryTier < currentCapacity.getTierMlNativeMemoryRequirementExcludingOverhead()
            || largestJob < currentCapacity.getNodeMlNativeMemoryRequirementExcludingOverhead()) {
            NativeMemoryCapacity nativeMemoryCapacity = new NativeMemoryCapacity(
                // Since we are in the `scaleDown` branch, we know jobs are running and we could be smaller
                // If we have some weird rounding errors, it may be that the `currentlyNecessary` values are larger than
                // current capacity. We never want to accidentally say "scale up" via a scale down.
                Math.min(currentlyNecessaryTier, currentCapacity.getTierMlNativeMemoryRequirementExcludingOverhead()),
                Math.min(largestJob, currentCapacity.getNodeMlNativeMemoryRequirementExcludingOverhead()),
                null
            );
            MlMemoryAutoscalingCapacity requiredCapacity = nativeMemoryCapacity.autoscalingCapacity(
                maxMachineMemoryPercent,
                useAuto,
                mlNativeMemoryForLargestMlNode,
                nodeAvailabilityZoneMapper.getNumMlAvailabilityZones().orElse(1)
            ).setReason("Requesting scale down as tier and/or node size could be smaller").build();
            return Optional.of(requiredCapacity);
        }

        return Optional.empty();
    }

    static MlMemoryAutoscalingCapacity ensureScaleDown(
        MlMemoryAutoscalingCapacity scaleDownResult,
        MlMemoryAutoscalingCapacity currentCapacity
    ) {
        if (scaleDownResult == null || currentCapacity == null) {
            return null;
        }
        MlMemoryAutoscalingCapacity newCapacity = MlMemoryAutoscalingCapacity.builder(
            ByteSizeValue.ofBytes(Math.min(scaleDownResult.nodeSize().getBytes(), currentCapacity.nodeSize().getBytes())),
            ByteSizeValue.ofBytes(Math.min(scaleDownResult.tierSize().getBytes(), currentCapacity.tierSize().getBytes()))
        ).setReason(scaleDownResult.reason()).build();
        if (scaleDownResult.nodeSize().getBytes() - newCapacity.nodeSize().getBytes() > ACCEPTABLE_DIFFERENCE
            || scaleDownResult.tierSize().getBytes() - newCapacity.tierSize().getBytes() > ACCEPTABLE_DIFFERENCE) {
            logger.warn(
                "scale down accidentally requested a scale up, auto-corrected; initial scaling [{}], corrected [{}]",
                scaleDownResult,
                newCapacity
            );
        }
        return newCapacity;
    }

    static boolean modelAssignmentsRequireMoreThanHalfCpu(Collection<TrainedModelAssignment> assignments, List<DiscoveryNode> mlNodes) {
        int totalRequiredProcessors = assignments.stream()
            .mapToInt(t -> t.getTaskParams().getNumberOfAllocations() * t.getTaskParams().getThreadsPerAllocation())
            .sum();
        int totalMlProcessors = mlNodes.stream().mapToInt(node -> MlProcessors.get(node).roundUp()).sum();
        return totalRequiredProcessors * 2 > totalMlProcessors;
    }

    /**
     * This calculates the potential future free capacity.
     * Since jobs with lookback-only datafeeds, and data frame analytics jobs all have some potential future end date
     * we can assume (without user intervention) that these will eventually stop and free their currently occupied resources.
     *
     * The capacity is as follows:
     * - tier: The sum total of the resources that will eventually be available.
     * - node: The largest block of memory that will be free on a given node.
     * - If > 1 "batch" ml tasks are running on the same node, we sum their resources.
     */
    Optional<NativeMemoryCapacity> calculateFutureAvailableCapacity(Collection<DiscoveryNode> mlNodes, ClusterState clusterState) {
        return calculateFutureAvailableCapacity(
            clusterState.metadata().custom(PersistentTasksCustomMetadata.TYPE),
            mlNodes.stream()
                .map(node -> nodeLoadDetector.detectNodeLoad(clusterState, node, maxOpenJobs, maxMachineMemoryPercent, useAuto))
                .toList()
        );
    }

    /**
     * This calculates the potential future free capacity.
     * Since jobs with lookback-only datafeeds, and data frame analytics jobs all have some potential future end date
     * we can assume (without user intervention) that these will eventually stop and free their currently occupied resources.
     *
     * The capacity is as follows:
     * - tier: The sum total of the resources that will eventually be available.
     * - node: The largest block of memory that will be free on a given node.
     * - If > 1 "batch" ml tasks are running on the same node, we sum their resources.
     */
    Optional<NativeMemoryCapacity> calculateFutureAvailableCapacity(PersistentTasksCustomMetadata tasks, List<NodeLoad> nodeLoads) {
        final List<PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams>> jobsWithLookbackDatafeeds =
            datafeedTasks(tasks).stream().filter(t -> t.getParams().getEndTime() != null && t.getExecutorNode() != null).toList();
        final List<PersistentTasksCustomMetadata.PersistentTask<?>> assignedAnalyticsJobs = MlAutoscalingContext.dataframeAnalyticsTasks(
            tasks
        ).stream().filter(t -> t.getExecutorNode() != null).toList();

        // What is the future freed capacity, knowing the current capacity and what could be freed up in the future?
        Map<String, Long> freeMemoryByNodeId = new HashMap<>();
        for (NodeLoad nodeLoad : nodeLoads) {
            if (nodeLoad.getError() != null || nodeLoad.isUseMemory() == false) {
                logger.debug("[{}] node free memory not available", nodeLoad.getNodeId());
                return Optional.empty();
            }
            freeMemoryByNodeId.put(nodeLoad.getNodeId(), nodeLoad.getFreeMemoryExcludingPerNodeOverhead());
        }
        for (PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams> lookbackOnlyDf : jobsWithLookbackDatafeeds) {
            Long jobSize = getAnomalyMemoryRequirement(lookbackOnlyDf.getParams().getJobId());
            if (jobSize == null) {
                return Optional.empty();
            }
            freeMemoryByNodeId.compute(lookbackOnlyDf.getExecutorNode(), (k, v) -> v == null ? jobSize : jobSize + v);
        }
        for (PersistentTasksCustomMetadata.PersistentTask<?> task : assignedAnalyticsJobs) {
            Long jobSize = getAnalyticsMemoryRequirement(MlTasks.dataFrameAnalyticsId(task.getId()));
            if (jobSize == null) {
                return Optional.empty();
            }
            freeMemoryByNodeId.compute(task.getExecutorNode(), (k, v) -> v == null ? jobSize : jobSize + v);
        }
        return Optional.of(
            new NativeMemoryCapacity(
                freeMemoryByNodeId.values().stream().mapToLong(Long::longValue).sum(),
                freeMemoryByNodeId.values().stream().mapToLong(Long::longValue).max().orElse(0L)
            )
        );
    }

    @SuppressWarnings("unchecked")
    private static Collection<PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams>> datafeedTasks(
        PersistentTasksCustomMetadata tasksCustomMetadata
    ) {
        if (tasksCustomMetadata == null) {
            return List.of();
        }

        return tasksCustomMetadata.findTasks(MlTasks.DATAFEED_TASK_NAME, t -> true)
            .stream()
            .map(p -> (PersistentTasksCustomMetadata.PersistentTask<StartDatafeedAction.DatafeedParams>) p)
            .toList();
    }

    private Long getAnalyticsMemoryRequirement(String analyticsId) {
        Long mem = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(analyticsId);
        if (mem == null) {
            logger.debug("[{}] data frame analytics job memory requirement not available", analyticsId);
        }
        return mem;
    }

    private Long getAllocatedModelRequirement(String modelId) {
        Long mem = mlMemoryTracker.getTrainedModelAssignmentMemoryRequirement(modelId);
        if (mem == null) {
            logger.debug("[{}] trained model memory requirement not available", modelId);
        }
        return mem;
    }

    private Long getAnalyticsMemoryRequirement(PersistentTasksCustomMetadata.PersistentTask<?> task) {
        return getAnalyticsMemoryRequirement(MlTasks.dataFrameAnalyticsId(task.getId()));
    }

    private Long getAnomalyMemoryRequirement(String anomalyId) {
        Long mem = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(anomalyId);
        if (mem == null) {
            logger.debug("[{}] anomaly detection job memory requirement not available", anomalyId);
        }
        return mem;
    }

    private Long getAnomalyMemoryRequirement(PersistentTasksCustomMetadata.PersistentTask<?> task) {
        return getAnomalyMemoryRequirement(MlTasks.jobId(task.getId()));
    }
}
