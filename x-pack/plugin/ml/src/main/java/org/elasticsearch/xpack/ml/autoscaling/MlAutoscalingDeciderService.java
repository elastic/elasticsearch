/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction.DatafeedParams;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.inference.allocation.AllocationState;
import org.elasticsearch.xpack.core.ml.inference.allocation.TrainedModelAllocation;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeState;
import org.elasticsearch.xpack.core.ml.job.snapshot.upgrade.SnapshotUpgradeTaskParams;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.inference.allocation.TrainedModelAllocationMetadata;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.MlTasks.getDataFrameAnalyticsState;
import static org.elasticsearch.xpack.core.ml.MlTasks.getJobStateModifiedForReassignments;
import static org.elasticsearch.xpack.core.ml.MlTasks.getSnapshotUpgradeState;
import static org.elasticsearch.xpack.ml.MachineLearning.MAX_OPEN_JOBS_PER_NODE;
import static org.elasticsearch.xpack.ml.MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

public class MlAutoscalingDeciderService implements AutoscalingDeciderService, LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(MlAutoscalingDeciderService.class);
    private static final Duration DEFAULT_MEMORY_REFRESH_RATE = Duration.ofMinutes(15);
    private static final String MEMORY_STALE = "unable to make scaling decision as job memory requirements are stale";
    private static final long NO_SCALE_DOWN_POSSIBLE = -1L;
    // If ensureScaleDown changes the calculation by more than this much, log the error
    private static final long ACCEPTABLE_DIFFERENCE = ByteSizeValue.ofMb(1).getBytes();

    public static final String NAME = "ml";
    public static final Setting<Integer> NUM_ANOMALY_JOBS_IN_QUEUE = Setting.intSetting("num_anomaly_jobs_in_queue", 0, 0);
    public static final Setting<Integer> NUM_ANALYTICS_JOBS_IN_QUEUE = Setting.intSetting("num_analytics_jobs_in_queue", 0, 0);
    public static final Setting<TimeValue> DOWN_SCALE_DELAY = Setting.timeSetting("down_scale_delay", TimeValue.timeValueHours(1));

    private final NodeLoadDetector nodeLoadDetector;
    private final MlMemoryTracker mlMemoryTracker;
    private final LongSupplier timeSupplier;

    private volatile boolean isMaster;
    private volatile int maxMachineMemoryPercent;
    private volatile int maxOpenJobs;
    private volatile boolean useAuto;
    private volatile long lastTimeToScale;
    private volatile long scaleDownDetected;

    public MlAutoscalingDeciderService(MlMemoryTracker memoryTracker, Settings settings, ClusterService clusterService) {
        this(new NodeLoadDetector(memoryTracker), settings, clusterService, System::currentTimeMillis);
    }

    MlAutoscalingDeciderService(
        NodeLoadDetector nodeLoadDetector,
        Settings settings,
        ClusterService clusterService,
        LongSupplier timeSupplier
    ) {
        this.nodeLoadDetector = nodeLoadDetector;
        this.mlMemoryTracker = nodeLoadDetector.getMlMemoryTracker();
        this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxOpenJobs = MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.useAuto = MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings);
        this.timeSupplier = timeSupplier;
        this.scaleDownDetected = NO_SCALE_DOWN_POSSIBLE;
        clusterService.getClusterSettings()
            .addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT, this::setMaxMachineMemoryPercent);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT, this::setUseAuto);
        clusterService.addLocalNodeMasterListener(this);
    }

    static OptionalLong getNodeJvmSize(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        OptionalLong value = OptionalLong.empty();
        String valueStr = nodeAttributes.get(MachineLearning.MAX_JVM_SIZE_NODE_ATTR);
        try {
            value = OptionalLong.of(Long.parseLong(valueStr));
        } catch (NumberFormatException e) {
            logger.debug(
                () -> new ParameterizedMessage(
                    "could not parse stored string value [{}] in node attribute [{}]",
                    valueStr,
                    MachineLearning.MAX_JVM_SIZE_NODE_ATTR
                )
            );
        }
        return value;
    }

    static List<DiscoveryNode> getNodes(final ClusterState clusterState) {
        return clusterState.nodes().mastersFirstStream().filter(MachineLearning::isMlNode).collect(Collectors.toList());
    }

    /**
     * @param unassignedJobs The list of unassigned jobs
     * @param sizeFunction   Function providing the memory required for a job
     * @param maxNumInQueue  The number of unassigned jobs allowed.
     * @return The capacity needed to reduce the length of `unassignedJobs` to `maxNumInQueue`
     */
    static Optional<NativeMemoryCapacity> requiredCapacityForUnassignedJobs(
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
            .collect(Collectors.toList());

        long tierMemory = 0L;
        // Node memory needs to be AT LEAST the size of the largest job + the required overhead.
        long nodeMemory = jobSizes.get(0) + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
        Iterator<Long> iter = jobSizes.iterator();
        while (jobSizes.size() > maxNumInQueue && iter.hasNext()) {
            tierMemory += iter.next();
            iter.remove();
        }
        return Optional.of(new NativeMemoryCapacity(tierMemory, nodeMemory));
    }

    static Optional<Tuple<NativeMemoryCapacity, List<NodeLoad>>> determineUnassignableJobs(
        List<String> unassignedJobs,
        Function<String, Long> sizeFunction,
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
            .collect(Collectors.toList());

        Iterator<Long> assignmentIter = jobSizes.iterator();
        while (jobSizes.size() > maxNumInQueue && assignmentIter.hasNext()) {
            long requiredMemory = assignmentIter.next();
            NodeLoad.Builder nodeLoad = mostFreeMemoryFirst.peek();
            assert nodeLoad != null : "unexpected null value while calculating assignable memory";
            // We can assign it given our current size
            if (nodeLoad.getNumAssignedJobs() == 0) {
                requiredMemory += NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
            }
            // Since we have the least loaded node (by memory) first, if it can't fit here, it can't fit anywhere
            if (nodeLoad.getFreeMemory() >= requiredMemory) {
                assignmentIter.remove();
                // Remove and add to the priority queue to make sure the biggest node with availability is first
                mostFreeMemoryFirst.add(mostFreeMemoryFirst.poll().incNumAssignedJobs().incAssignedAnomalyDetectorMemory(requiredMemory));
            }
        }
        List<NodeLoad> adjustedLoads = mostFreeMemoryFirst.stream().map(NodeLoad.Builder::build).collect(Collectors.toList());

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
                    // Node memory needs to be AT LEAST the size of the largest job + the required overhead.
                    unassignableMemory.get(0) + NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
                ),
                adjustedLoads
            )
        );
    }

    private static Collection<PersistentTask<?>> anomalyDetectionTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.JOB_TASK_NAME, t -> taskStateFilter(getJobStateModifiedForReassignments(t)));
    }

    private static Collection<PersistentTask<?>> snapshotUpgradeTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.JOB_SNAPSHOT_UPGRADE_TASK_NAME, t -> taskStateFilter(getSnapshotUpgradeState(t)));
    }

    private static Collection<PersistentTask<?>> dataframeAnalyticsTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME, t -> taskStateFilter(getDataFrameAnalyticsState(t)));
    }

    @SuppressWarnings("unchecked")
    private static Collection<PersistentTask<DatafeedParams>> datafeedTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.DATAFEED_TASK_NAME, t -> true)
            .stream()
            .map(p -> (PersistentTask<DatafeedParams>) p)
            .collect(Collectors.toList());
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

    @Override
    public void onMaster() {
        isMaster = true;
    }

    private void resetScaleDownCoolDown() {
        this.scaleDownDetected = NO_SCALE_DOWN_POSSIBLE;
    }

    private boolean newScaleDownCheck() {
        return scaleDownDetected == NO_SCALE_DOWN_POSSIBLE;
    }

    public static NativeMemoryCapacity currentScale(
        final List<DiscoveryNode> machineLearningNodes,
        int maxMachineMemoryPercent,
        boolean useAuto
    ) {
        long[] mlMemory = machineLearningNodes.stream()
            .mapToLong(node -> NativeMemoryCalculator.allowedBytesForMl(node, maxMachineMemoryPercent, useAuto).orElse(0L))
            .toArray();

        return new NativeMemoryCapacity(
            Arrays.stream(mlMemory).sum(),
            Arrays.stream(mlMemory).max().orElse(0L),
            // We assume that JVM size is universal, at least, the largest JVM indicates the largest node
            machineLearningNodes.stream()
                .map(MlAutoscalingDeciderService::getNodeJvmSize)
                .mapToLong(l -> l.orElse(0L))
                .boxed()
                .max(Long::compare)
                .orElse(null)
        );
    }

    NativeMemoryCapacity currentScale(final List<DiscoveryNode> machineLearningNodes) {
        return currentScale(machineLearningNodes, maxMachineMemoryPercent, useAuto);
    }

    @Override
    public void offMaster() {
        isMaster = false;
    }

    @Override
    public AutoscalingDeciderResult scale(Settings configuration, AutoscalingDeciderContext context) {
        if (isMaster == false) {
            throw new IllegalArgumentException("request for scaling information is only allowed on the master node");
        }
        long previousTimeStamp = lastTimeToScale;
        lastTimeToScale = timeSupplier.getAsLong();
        if (previousTimeStamp > 0L && lastTimeToScale > previousTimeStamp) {
            mlMemoryTracker.setAutoscalingCheckInterval(Duration.ofMillis(lastTimeToScale - previousTimeStamp));
        }

        final ClusterState clusterState = context.state();

        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        Collection<PersistentTask<?>> anomalyDetectionTasks = anomalyDetectionTasks(tasks);
        Collection<PersistentTask<?>> snapshotUpgradeTasks = snapshotUpgradeTasks(tasks);
        Collection<PersistentTask<?>> dataframeAnalyticsTasks = dataframeAnalyticsTasks(tasks);
        Map<String, TrainedModelAllocation> modelAllocations = TrainedModelAllocationMetadata.fromState(clusterState).modelAllocations();
        final List<String> waitingAnomalyJobs = anomalyDetectionTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> ((OpenJobAction.JobParams) t.getParams()).getJobId())
            .collect(Collectors.toList());
        final List<String> waitingSnapshotUpgrades = snapshotUpgradeTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> ((SnapshotUpgradeTaskParams) t.getParams()).getJobId())
            .collect(Collectors.toList());
        final List<String> waitingAnalyticsJobs = dataframeAnalyticsTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> ((StartDataFrameAnalyticsAction.TaskParams) t.getParams()).getId())
            .collect(Collectors.toList());
        final List<String> waitingAllocatedModels = modelAllocations.entrySet()
            .stream()
            // TODO: Eventually care about those that are STARTED but not FULLY_ALLOCATED
            .filter(e -> e.getValue().getAllocationState().equals(AllocationState.STARTING) && e.getValue().getNodeRoutingTable().isEmpty())
            .map(Map.Entry::getKey)
            .collect(Collectors.toList());

        final int numAnalyticsJobsInQueue = NUM_ANALYTICS_JOBS_IN_QUEUE.get(configuration);
        final int numAnomalyJobsInQueue = NUM_ANOMALY_JOBS_IN_QUEUE.get(configuration);

        final List<DiscoveryNode> nodes = getNodes(clusterState);
        final NativeMemoryCapacity currentScale = currentScale(nodes);

        final MlScalingReason.Builder reasonBuilder = MlScalingReason.builder()
            .setWaitingAnomalyJobs(waitingAnomalyJobs)
            .setWaitingSnapshotUpgrades(waitingSnapshotUpgrades)
            .setWaitingAnalyticsJobs(waitingAnalyticsJobs)
            .setWaitingModels(waitingAllocatedModels)
            .setCurrentMlCapacity(currentScale.autoscalingCapacity(maxMachineMemoryPercent, useAuto))
            .setPassedConfiguration(configuration);

        // There are no ML nodes, scale up as quick as possible, no matter if memory is stale or not
        if (nodes.isEmpty()
            && (waitingAnomalyJobs.isEmpty() == false
                || waitingSnapshotUpgrades.isEmpty() == false
                || waitingAnalyticsJobs.isEmpty() == false
                || waitingAllocatedModels.isEmpty() == false)) {
            return scaleUpFromZero(
                waitingAnomalyJobs,
                waitingSnapshotUpgrades,
                waitingAnalyticsJobs,
                waitingAllocatedModels,
                reasonBuilder
            );
        }

        // We don't need to check anything as there are no tasks
        // This is a quick path to downscale.
        // simply return `0` for scale down if delay is satisfied
        if (anomalyDetectionTasks.isEmpty() && dataframeAnalyticsTasks.isEmpty() && modelAllocations.isEmpty()) {
            long msLeftToScale = msLeftToDownScale(configuration);
            if (msLeftToScale > 0) {
                return new AutoscalingDeciderResult(
                    context.currentCapacity(),
                    reasonBuilder.setSimpleReason(
                        String.format(
                            Locale.ROOT,
                            "Passing currently perceived capacity as down scale delay has not been satisfied; configured delay [%s]"
                                + "last detected scale down event [%s]. Will request scale down in approximately [%s]",
                            DOWN_SCALE_DELAY.get(configuration).getStringRep(),
                            XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(scaleDownDetected)),
                            TimeValue.timeValueMillis(msLeftToScale).getStringRep()
                        )
                    ).build()
                );
            }
            return new AutoscalingDeciderResult(
                AutoscalingCapacity.ZERO,
                reasonBuilder.setRequiredCapacity(AutoscalingCapacity.ZERO)
                    .setSimpleReason("Requesting scale down as tier and/or node size could be smaller")
                    .build()
            );
        }

        if (mlMemoryTracker.isRecentlyRefreshed() == false) {
            logger.debug(
                "view of job memory is stale given duration [{}]. Not attempting to make scaling decision",
                mlMemoryTracker.getStalenessDuration()
            );
            return buildDecisionAndRequestRefresh(reasonBuilder);
        }
        // We need the current node loads to determine if we need to scale up or down
        List<NodeLoad> nodeLoads = new ArrayList<>(nodes.size());
        boolean nodeIsMemoryAccurate = true;
        for (DiscoveryNode node : nodes) {
            NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(clusterState, node, maxOpenJobs, maxMachineMemoryPercent, useAuto);
            if (nodeLoad.getError() != null) {
                logger.warn("[{}] failed to gather node load limits, failure [{}]. Returning no scale", node.getId(), nodeLoad.getError());
                return noScaleResultOrRefresh(
                    reasonBuilder,
                    true,
                    new AutoscalingDeciderResult(
                        context.currentCapacity(),
                        reasonBuilder.setSimpleReason(
                            "Passing currently perceived capacity as there was a failure gathering node limits ["
                                + nodeLoad.getError()
                                + "]"
                        ).build()
                    )
                );
            }
            nodeLoads.add(nodeLoad);
            nodeIsMemoryAccurate = nodeIsMemoryAccurate && nodeLoad.isUseMemory();
        }
        // This is an exceptional case, the memory tracking became stale between us checking previously and calculating the loads
        // We should return a no scale in this case
        if (nodeIsMemoryAccurate == false) {
            return noScaleResultOrRefresh(
                reasonBuilder,
                true,
                new AutoscalingDeciderResult(
                    context.currentCapacity(),
                    reasonBuilder.setSimpleReason(
                        "Passing currently perceived capacity as nodes were unable to provide an accurate view of their memory usage"
                    ).build()
                )
            );
        }

        Optional<NativeMemoryCapacity> futureFreedCapacity = calculateFutureAvailableCapacity(tasks, nodes, clusterState);

        final Optional<AutoscalingDeciderResult> scaleUpDecision = checkForScaleUp(
            numAnomalyJobsInQueue,
            numAnalyticsJobsInQueue,
            nodeLoads,
            waitingAnomalyJobs,
            waitingSnapshotUpgrades,
            waitingAnalyticsJobs,
            waitingAllocatedModels,
            futureFreedCapacity.orElse(null),
            currentScale,
            reasonBuilder
        );

        if (scaleUpDecision.isPresent()) {
            resetScaleDownCoolDown();
            return scaleUpDecision.get();
        }
        if (waitingAnalyticsJobs.isEmpty() == false
            || waitingSnapshotUpgrades.isEmpty() == false
            || waitingAnomalyJobs.isEmpty() == false) {
            // We don't want to continue to consider a scale down if there are now waiting jobs
            resetScaleDownCoolDown();
            return noScaleResultOrRefresh(
                reasonBuilder,
                mlMemoryTracker.isRecentlyRefreshed() == false,
                new AutoscalingDeciderResult(
                    context.currentCapacity(),
                    reasonBuilder.setSimpleReason(
                        String.format(
                            Locale.ROOT,
                            "Passing currently perceived capacity as there are [%d] model snapshot upgrades, "
                                + "[%d] analytics and [%d] anomaly detection jobs in the queue, "
                                + "but the number in the queue is less than the configured maximum allowed "
                                + " or the queued jobs will eventually be assignable at the current size. ",
                            waitingSnapshotUpgrades.size(),
                            waitingAnalyticsJobs.size(),
                            waitingAnomalyJobs.size()
                        )
                    ).build()
                )
            );
        }

        long largestJobOrModel = Math.max(
            anomalyDetectionTasks.stream()
                .filter(PersistentTask::isAssigned)
                // Memory SHOULD be recently refreshed, so in our current state, we should at least have an idea of the memory used
                .mapToLong(t -> {
                    Long mem = this.getAnomalyMemoryRequirement(t);
                    assert mem != null : "unexpected null for anomaly memory requirement after recent stale check";
                    return mem;
                })
                .max()
                .orElse(0L),
            dataframeAnalyticsTasks.stream()
                .filter(PersistentTask::isAssigned)
                // Memory SHOULD be recently refreshed, so in our current state, we should at least have an idea of the memory used
                .mapToLong(t -> {
                    Long mem = this.getAnalyticsMemoryRequirement(t);
                    assert mem != null : "unexpected null for analytics memory requirement after recent stale check";
                    return mem;
                })
                .max()
                .orElse(0L)
        );
        largestJobOrModel = Math.max(
            largestJobOrModel,
            modelAllocations.values().stream().mapToLong(t -> t.getTaskParams().estimateMemoryUsageBytes()).max().orElse(0L)
        );

        // This is an exceptionally weird state
        // Our view of the memory is stale or we have tasks where the required job memory is 0, which should be impossible
        if (largestJobOrModel == 0L && (dataframeAnalyticsTasks.size() + anomalyDetectionTasks.size() + modelAllocations.size() > 0)) {
            logger.warn(
                "The calculated minimum required node size was unexpectedly [0] as there are "
                    + "[{}] anomaly job tasks, [{}] data frame analytics tasks and [{}] model allocations",
                anomalyDetectionTasks.size(),
                dataframeAnalyticsTasks.size(),
                modelAllocations.size()
            );
            return noScaleResultOrRefresh(
                reasonBuilder,
                true,
                new AutoscalingDeciderResult(
                    context.currentCapacity(),
                    reasonBuilder.setSimpleReason(
                        "Passing currently perceived capacity as there are running analytics and anomaly jobs, "
                            + "but their memory usage estimates are inaccurate."
                    ).build()
                )
            );
        }

        final Optional<AutoscalingDeciderResult> maybeScaleDown = checkForScaleDown(
            nodeLoads,
            largestJobOrModel,
            currentScale,
            reasonBuilder
        )
            // Due to weird rounding errors, it may be that a scale down result COULD cause a scale up
            // Ensuring the scaleDown here forces the scale down result to always be lower than the current capacity.
            // This is safe as we know that ALL jobs are assigned at the current capacity
            .map(result -> {
                AutoscalingCapacity capacity = ensureScaleDown(result.requiredCapacity(), context.currentCapacity());
                if (capacity == null) {
                    return null;
                }
                return new AutoscalingDeciderResult(capacity, result.reason());
            });

        if (maybeScaleDown.isPresent()) {
            final AutoscalingDeciderResult scaleDownDecisionResult = maybeScaleDown.get();

            context.currentCapacity();
            // Given maxOpenJobs, could we scale down to just one node?
            // We have no way of saying "we need X nodes"
            if (nodeLoads.size() > 1) {
                long totalAssignedJobs = nodeLoads.stream().mapToLong(NodeLoad::getNumAssignedJobs).sum();
                // one volatile read
                long maxOpenJobsCopy = this.maxOpenJobs;
                if (totalAssignedJobs > maxOpenJobsCopy) {
                    String msg = String.format(
                        Locale.ROOT,
                        "not scaling down as the total number of jobs [%d] exceeds the setting [%s (%d)]. "
                            + " To allow a scale down [%s] must be increased.",
                        totalAssignedJobs,
                        MAX_OPEN_JOBS_PER_NODE.getKey(),
                        maxOpenJobsCopy,
                        MAX_OPEN_JOBS_PER_NODE.getKey()
                    );
                    logger.info(
                        () -> new ParameterizedMessage(
                            "{} Calculated potential scaled down capacity [{}] ",
                            msg,
                            scaleDownDecisionResult.requiredCapacity()
                        )
                    );
                    return new AutoscalingDeciderResult(context.currentCapacity(), reasonBuilder.setSimpleReason(msg).build());
                }
            }

            long msLeftToScale = msLeftToDownScale(configuration);
            if (msLeftToScale <= 0) {
                return scaleDownDecisionResult;
            }
            TimeValue downScaleDelay = DOWN_SCALE_DELAY.get(configuration);
            logger.debug(
                () -> new ParameterizedMessage(
                    "not scaling down as the current scale down delay [{}] is not satisfied."
                        + " The last time scale down was detected [{}]. Calculated scaled down capacity [{}] ",
                    downScaleDelay.getStringRep(),
                    XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(scaleDownDetected)),
                    scaleDownDecisionResult.requiredCapacity()
                )
            );
            return new AutoscalingDeciderResult(
                context.currentCapacity(),
                reasonBuilder.setSimpleReason(
                    String.format(
                        Locale.ROOT,
                        "Passing currently perceived capacity as down scale delay has not been satisfied; configured delay [%s]"
                            + "last detected scale down event [%s]. Will request scale down in approximately [%s]",
                        downScaleDelay.getStringRep(),
                        XContentElasticsearchExtension.DEFAULT_FORMATTER.format(Instant.ofEpochMilli(scaleDownDetected)),
                        TimeValue.timeValueMillis(msLeftToScale).getStringRep()
                    )
                ).build()
            );
        }

        return noScaleResultOrRefresh(
            reasonBuilder,
            mlMemoryTracker.isRecentlyRefreshed() == false,
            new AutoscalingDeciderResult(
                context.currentCapacity(),
                reasonBuilder.setSimpleReason("Passing currently perceived capacity as no scaling changes were detected to be possible")
                    .build()
            )
        );
    }

    static AutoscalingCapacity ensureScaleDown(AutoscalingCapacity scaleDownResult, AutoscalingCapacity currentCapacity) {
        if (scaleDownResult == null || currentCapacity == null) {
            return null;
        }
        AutoscalingCapacity newCapacity = new AutoscalingCapacity(
            new AutoscalingCapacity.AutoscalingResources(
                currentCapacity.total().storage(),
                ByteSizeValue.ofBytes(Math.min(scaleDownResult.total().memory().getBytes(), currentCapacity.total().memory().getBytes()))
            ),
            new AutoscalingCapacity.AutoscalingResources(
                currentCapacity.node().storage(),
                ByteSizeValue.ofBytes(Math.min(scaleDownResult.node().memory().getBytes(), currentCapacity.node().memory().getBytes()))
            )
        );
        if (scaleDownResult.node().memory().getBytes() - newCapacity.node().memory().getBytes() > ACCEPTABLE_DIFFERENCE
            || scaleDownResult.total().memory().getBytes() - newCapacity.total().memory().getBytes() > ACCEPTABLE_DIFFERENCE) {
            logger.warn(
                "scale down accidentally requested a scale up, auto-corrected; initial scaling [{}], corrected [{}]",
                scaleDownResult,
                newCapacity
            );
        }
        return newCapacity;
    }

    AutoscalingDeciderResult noScaleResultOrRefresh(
        MlScalingReason.Builder reasonBuilder,
        boolean memoryTrackingStale,
        AutoscalingDeciderResult potentialResult
    ) {
        if (memoryTrackingStale) {
            logger.debug("current view of job memory is stale given. Returning a no scale event");
            return buildDecisionAndRequestRefresh(reasonBuilder);
        } else {
            return potentialResult;
        }
    }

    // This doesn't allow any jobs to wait in the queue, this is because in a "normal" scaling event, we also verify if a job
    // can eventually start, and given the current cluster, no job can eventually start.
    AutoscalingDeciderResult scaleUpFromZero(
        List<String> waitingAnomalyJobs,
        List<String> waitingSnapshotUpgrades,
        List<String> waitingAnalyticsJobs,
        List<String> waitingAllocatedModels,
        MlScalingReason.Builder reasonBuilder
    ) {
        final Optional<NativeMemoryCapacity> analyticsCapacity = requiredCapacityForUnassignedJobs(
            waitingAnalyticsJobs,
            this::getAnalyticsMemoryRequirement,
            0
        );
        final Optional<NativeMemoryCapacity> anomalyCapacity = requiredCapacityForUnassignedJobs(
            waitingAnomalyJobs,
            this::getAnomalyMemoryRequirement,
            0
        );
        final Optional<NativeMemoryCapacity> snapshotUpgradeCapacity = requiredCapacityForUnassignedJobs(
            waitingSnapshotUpgrades,
            this::getAnomalyMemoryRequirement,
            0
        );
        final Optional<NativeMemoryCapacity> allocatedModelCapacity = requiredCapacityForUnassignedJobs(
            waitingAllocatedModels,
            this::getAllocatedModelRequirement,
            0
        );
        NativeMemoryCapacity updatedCapacity = NativeMemoryCapacity.ZERO.merge(anomalyCapacity.orElse(NativeMemoryCapacity.ZERO))
            .merge(snapshotUpgradeCapacity.orElse(NativeMemoryCapacity.ZERO))
            .merge(analyticsCapacity.orElse(NativeMemoryCapacity.ZERO))
            .merge(allocatedModelCapacity.orElse(NativeMemoryCapacity.ZERO));
        // If we still have calculated zero, this means the ml memory tracker does not have the required info.
        // So, request a scale for the default. This is only for the 0 -> N scaling case.
        if (updatedCapacity.getNodeMlNativeMemoryRequirement() == 0L) {
            updatedCapacity = updatedCapacity.merge(
                new NativeMemoryCapacity(
                    ByteSizeValue.ofMb(AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB).getBytes(),
                    ByteSizeValue.ofMb(AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB).getBytes()
                )
            );
        }
        updatedCapacity = updatedCapacity.merge(
            new NativeMemoryCapacity(
                MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(),
                MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes()
            )
        );
        AutoscalingCapacity requiredCapacity = updatedCapacity.autoscalingCapacity(maxMachineMemoryPercent, useAuto);
        return new AutoscalingDeciderResult(
            requiredCapacity,
            reasonBuilder.setRequiredCapacity(requiredCapacity)
                .setSimpleReason(
                    "requesting scale up as number of jobs in queues exceeded configured limit and there are no machine learning nodes"
                )
                .build()
        );
    }

    Optional<AutoscalingDeciderResult> checkForScaleUp(
        int numAnomalyJobsInQueue,
        int numAnalyticsJobsInQueue,
        List<NodeLoad> nodeLoads,
        List<String> waitingAnomalyJobs,
        List<String> waitingSnapshotUpgrades,
        List<String> waitingAnalyticsJobs,
        List<String> waitingAllocatedModels,
        @Nullable NativeMemoryCapacity futureFreedCapacity,
        NativeMemoryCapacity currentScale,
        MlScalingReason.Builder reasonBuilder
    ) {

        // Are we in breach of maximum waiting jobs?
        if (waitingAnalyticsJobs.size() > numAnalyticsJobsInQueue
            || waitingAnomalyJobs.size() + waitingSnapshotUpgrades.size() > numAnomalyJobsInQueue
            || waitingAllocatedModels.size() > 0) {

            Tuple<NativeMemoryCapacity, List<NodeLoad>> anomalyCapacityAndNewLoad = determineUnassignableJobs(
                Stream.concat(waitingAnomalyJobs.stream(), waitingSnapshotUpgrades.stream()).collect(Collectors.toList()),
                this::getAnomalyMemoryRequirement,
                numAnomalyJobsInQueue,
                nodeLoads
            ).orElse(Tuple.tuple(NativeMemoryCapacity.ZERO, nodeLoads));

            Tuple<NativeMemoryCapacity, List<NodeLoad>> analyticsCapacityAndNewLoad = determineUnassignableJobs(
                waitingAnalyticsJobs,
                this::getAnalyticsMemoryRequirement,
                numAnalyticsJobsInQueue,
                anomalyCapacityAndNewLoad.v2()
            ).orElse(Tuple.tuple(NativeMemoryCapacity.ZERO, anomalyCapacityAndNewLoad.v2()));

            Tuple<NativeMemoryCapacity, List<NodeLoad>> modelCapacityAndNewLoad = determineUnassignableJobs(
                waitingAllocatedModels,
                this::getAllocatedModelRequirement,
                0,
                analyticsCapacityAndNewLoad.v2()
            ).orElse(Tuple.tuple(NativeMemoryCapacity.ZERO, analyticsCapacityAndNewLoad.v2()));

            if (analyticsCapacityAndNewLoad.v1().equals(NativeMemoryCapacity.ZERO)
                && anomalyCapacityAndNewLoad.v1().equals(NativeMemoryCapacity.ZERO)
                && modelCapacityAndNewLoad.v1().equals(NativeMemoryCapacity.ZERO)) {
                logger.debug("no_scale event as current capacity, even though there are waiting jobs, is adequate to run the queued jobs");
                return Optional.empty();
            }

            NativeMemoryCapacity updatedCapacity = NativeMemoryCapacity.from(currentScale)
                .merge(analyticsCapacityAndNewLoad.v1())
                .merge(anomalyCapacityAndNewLoad.v1())
                .merge(modelCapacityAndNewLoad.v1())
                // Since we require new capacity, it COULD be we require a brand new node
                // We should account for overhead in the tier capacity just in case.
                .merge(new NativeMemoryCapacity(MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes(), 0));
            AutoscalingCapacity requiredCapacity = updatedCapacity.autoscalingCapacity(maxMachineMemoryPercent, useAuto);
            return Optional.of(
                new AutoscalingDeciderResult(
                    requiredCapacity,
                    reasonBuilder.setRequiredCapacity(requiredCapacity)
                        .setSimpleReason(
                            "requesting scale up as number of jobs in queues exceeded configured limit "
                                + "or there is at least one trained model waiting for allocation "
                                + "and current capacity is not large enough for waiting jobs or models"
                        )
                        .build()
                )
            );
        }

        // Could the currently waiting jobs ever be assigned?
        // NOTE: the previous predicate catches if an allocated model isn't assigned
        if (waitingAnalyticsJobs.isEmpty() == false
            || waitingSnapshotUpgrades.isEmpty() == false
            || waitingAnomalyJobs.isEmpty() == false) {
            // we are unable to determine new tier size, but maybe we can see if our nodes are big enough.
            if (futureFreedCapacity == null) {
                Optional<Long> maxSize = Stream.concat(
                    waitingAnalyticsJobs.stream().map(mlMemoryTracker::getDataFrameAnalyticsJobMemoryRequirement),
                    Stream.concat(
                        waitingAnomalyJobs.stream().map(mlMemoryTracker::getAnomalyDetectorJobMemoryRequirement),
                        waitingSnapshotUpgrades.stream().map(mlMemoryTracker::getAnomalyDetectorJobMemoryRequirement)
                    )
                ).filter(Objects::nonNull).max(Long::compareTo);
                if (maxSize.isPresent() && maxSize.get() > currentScale.getNodeMlNativeMemoryRequirement()) {
                    AutoscalingCapacity requiredCapacity = new NativeMemoryCapacity(
                        Math.max(currentScale.getTierMlNativeMemoryRequirement(), maxSize.get()),
                        maxSize.get()
                    ).autoscalingCapacity(maxMachineMemoryPercent, useAuto);
                    return Optional.of(
                        new AutoscalingDeciderResult(
                            requiredCapacity,
                            reasonBuilder.setSimpleReason("requesting scale up as there is no node large enough to handle queued jobs")
                                .setRequiredCapacity(requiredCapacity)
                                .build()
                        )
                    );
                }
                // we have no info, allow the caller to make the appropriate action, probably returning a no_scale
                return Optional.empty();
            }
            long newTierNeeded = 0L;
            // could any of the nodes actually run the job?
            long newNodeMax = currentScale.getNodeMlNativeMemoryRequirement();
            for (String analyticsJob : waitingAnalyticsJobs) {
                Long requiredMemory = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(analyticsJob);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                // Is there "future capacity" on a node that could run this job? If not, we need that much more in the tier.
                if (futureFreedCapacity.getNodeMlNativeMemoryRequirement() < requiredMemory) {
                    newTierNeeded = Math.max(requiredMemory, newTierNeeded);
                }
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            for (String anomalyJob : waitingAnomalyJobs) {
                Long requiredMemory = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(anomalyJob);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                // Is there "future capacity" on a node that could run this job? If not, we need that much more in the tier.
                if (futureFreedCapacity.getNodeMlNativeMemoryRequirement() < requiredMemory) {
                    newTierNeeded = Math.max(requiredMemory, newTierNeeded);
                }
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            for (String snapshotUpgrade : waitingSnapshotUpgrades) {
                Long requiredMemory = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(snapshotUpgrade);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                // Is there "future capacity" on a node that could run this job? If not, we need that much more in the tier.
                if (futureFreedCapacity.getNodeMlNativeMemoryRequirement() < requiredMemory) {
                    newTierNeeded = Math.max(requiredMemory, newTierNeeded);
                }
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            if (newNodeMax > currentScale.getNodeMlNativeMemoryRequirement() || newTierNeeded > 0L) {
                NativeMemoryCapacity newCapacity = new NativeMemoryCapacity(newTierNeeded, newNodeMax);
                AutoscalingCapacity requiredCapacity = NativeMemoryCapacity.from(currentScale)
                    .merge(newCapacity)
                    .autoscalingCapacity(maxMachineMemoryPercent, useAuto);
                return Optional.of(
                    new AutoscalingDeciderResult(
                        // We need more memory in the tier, or our individual node size requirements has increased
                        requiredCapacity,
                        reasonBuilder.setSimpleReason(
                            "scaling up as adequate space would not automatically become available when running jobs finish"
                        ).setRequiredCapacity(requiredCapacity).build()
                    )
                );
            }
        }

        return Optional.empty();
    }

    // This calculates the the following the potential future free capacity
    // Since jobs with lookback only datafeeds, and data frame analytics jobs all have some potential future end date
    // we can assume (without user intervention) that these will eventually stop and free their currently occupied resources.
    //
    // The capacity is as follows:
    // tier: The sum total of the resources that will be eventually be available
    // node: The largest block of memory that will be free on a given node.
    // - If > 1 "batch" ml tasks are running on the same node, we sum their resources.
    Optional<NativeMemoryCapacity> calculateFutureAvailableCapacity(
        PersistentTasksCustomMetadata tasks,
        Collection<DiscoveryNode> mlNodes,
        ClusterState clusterState
    ) {
        if (mlMemoryTracker.isRecentlyRefreshed() == false) {
            return Optional.empty();
        }
        final List<PersistentTask<DatafeedParams>> jobsWithLookbackDatafeeds = datafeedTasks(tasks).stream()
            .filter(t -> t.getParams().getEndTime() != null && t.getExecutorNode() != null)
            .collect(Collectors.toList());
        final List<PersistentTask<?>> assignedAnalyticsJobs = dataframeAnalyticsTasks(tasks).stream()
            .filter(t -> t.getExecutorNode() != null)
            .collect(Collectors.toList());

        // what is the future freed capacity, knowing the current capacity and what could be freed up in the future
        Map<String, Long> freeMemoryByNodeId = new HashMap<>();
        for (DiscoveryNode node : mlNodes) {
            NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(clusterState, node, maxOpenJobs, maxMachineMemoryPercent, useAuto);
            if (nodeLoad.getError() != null || nodeLoad.isUseMemory() == false) {
                return Optional.empty();
            }
            freeMemoryByNodeId.put(node.getId(), nodeLoad.getFreeMemory());
        }
        for (PersistentTask<DatafeedParams> lookbackOnlyDf : jobsWithLookbackDatafeeds) {
            Long jobSize = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(lookbackOnlyDf.getParams().getJobId());
            if (jobSize == null) {
                return Optional.empty();
            }
            freeMemoryByNodeId.compute(lookbackOnlyDf.getExecutorNode(), (_k, v) -> v == null ? jobSize : jobSize + v);
        }
        for (PersistentTask<?> task : assignedAnalyticsJobs) {
            Long jobSize = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(MlTasks.dataFrameAnalyticsId(task.getId()));
            if (jobSize == null) {
                return Optional.empty();
            }
            freeMemoryByNodeId.compute(task.getExecutorNode(), (_k, v) -> v == null ? jobSize : jobSize + v);
        }
        return Optional.of(
            new NativeMemoryCapacity(
                freeMemoryByNodeId.values().stream().mapToLong(Long::longValue).sum(),
                freeMemoryByNodeId.values().stream().mapToLong(Long::longValue).max().orElse(0L)
            )
        );
    }

    private AutoscalingDeciderResult buildDecisionAndRequestRefresh(MlScalingReason.Builder reasonBuilder) {
        mlMemoryTracker.asyncRefresh();
        return new AutoscalingDeciderResult(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build());
    }

    private Long getAnalyticsMemoryRequirement(String analyticsId) {
        return mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(analyticsId);
    }

    private Long getAllocatedModelRequirement(String modelId) {
        return mlMemoryTracker.getTrainedModelAllocationMemoryRequirement(modelId);
    }

    private Long getAnalyticsMemoryRequirement(PersistentTask<?> task) {
        return getAnalyticsMemoryRequirement(MlTasks.dataFrameAnalyticsId(task.getId()));
    }

    private Long getAnomalyMemoryRequirement(String anomalyId) {
        return mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(anomalyId);
    }

    private Long getAnomalyMemoryRequirement(PersistentTask<?> task) {
        return getAnomalyMemoryRequirement(MlTasks.jobId(task.getId()));
    }

    Optional<AutoscalingDeciderResult> checkForScaleDown(
        List<NodeLoad> nodeLoads,
        long largestJob,
        NativeMemoryCapacity currentCapacity,
        MlScalingReason.Builder reasonBuilder
    ) {
        long currentlyNecessaryTier = nodeLoads.stream().mapToLong(NodeLoad::getAssignedJobMemory).sum();
        // The required NATIVE node memory is the largest job and our static overhead.
        long currentlyNecessaryNode = largestJob == 0 ? 0 : largestJob + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
        // If we are using `auto` && have at least one job, that means our native node size should be at least native capacity provided
        // via our `MINIMUM_AUTOMATIC_NODE_SIZE`. Otherwise, if we have to auto-calculate the JVM size, it could be much smaller than
        // what will truly be used.
        if (currentlyNecessaryNode > 0 && useAuto) {
            currentlyNecessaryNode = Math.max(
                currentlyNecessaryNode,
                NativeMemoryCalculator.allowedBytesForMl(
                    NativeMemoryCalculator.MINIMUM_AUTOMATIC_NODE_SIZE,
                    maxMachineMemoryPercent,
                    useAuto
                )
            );
        }
        // We consider a scale down if we are not fully utilizing the tier
        // Or our largest job could be on a smaller node (meaning the same size tier but smaller nodes are possible).
        if (currentlyNecessaryTier < currentCapacity.getTierMlNativeMemoryRequirement()
            || currentlyNecessaryNode < currentCapacity.getNodeMlNativeMemoryRequirement()) {
            NativeMemoryCapacity nativeMemoryCapacity = new NativeMemoryCapacity(
                // Since we are in the `scaleDown` branch, we know jobs are running and we could be smaller
                // If we have some weird rounding errors, it may be that the `currentlyNecessary` values are larger than
                // current capacity. We never want to accidentally say "scale up" via a scale down.
                Math.min(currentlyNecessaryTier, currentCapacity.getTierMlNativeMemoryRequirement()),
                Math.min(currentlyNecessaryNode, currentCapacity.getNodeMlNativeMemoryRequirement()),
                // If our newly suggested native capacity is the same, we can use the previously stored jvm size
                currentlyNecessaryNode == currentCapacity.getNodeMlNativeMemoryRequirement() ? currentCapacity.getJvmSize() : null
            );
            AutoscalingCapacity requiredCapacity = nativeMemoryCapacity.autoscalingCapacity(maxMachineMemoryPercent, useAuto);
            return Optional.of(
                new AutoscalingDeciderResult(
                    requiredCapacity,
                    reasonBuilder.setRequiredCapacity(requiredCapacity)
                        .setSimpleReason("Requesting scale down as tier and/or node size could be smaller")
                        .build()
                )
            );
        }

        return Optional.empty();
    }

    private long msLeftToDownScale(Settings configuration) {
        final long now = timeSupplier.getAsLong();
        if (newScaleDownCheck()) {
            scaleDownDetected = now;
        }
        TimeValue downScaleDelay = DOWN_SCALE_DELAY.get(configuration);
        return downScaleDelay.millis() - (now - scaleDownDetected);
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public List<Setting<?>> deciderSettings() {
        return List.of(NUM_ANALYTICS_JOBS_IN_QUEUE, NUM_ANOMALY_JOBS_IN_QUEUE, DOWN_SCALE_DELAY);
    }

    @Override
    public List<DiscoveryNodeRole> roles() {
        return List.of(DiscoveryNodeRole.ML_ROLE);
    }

    private static boolean taskStateFilter(JobState jobState) {
        return jobState == null || jobState.isNoneOf(JobState.CLOSED, JobState.FAILED);
    }

    private static boolean taskStateFilter(SnapshotUpgradeState snapshotUpgradeState) {
        return snapshotUpgradeState == null || snapshotUpgradeState.isNoneOf(SnapshotUpgradeState.STOPPED, SnapshotUpgradeState.FAILED);
    }

    private static boolean taskStateFilter(DataFrameAnalyticsState dataFrameAnalyticsState) {
        // Don't count stopped and failed df-analytics tasks as they don't consume native memory
        return dataFrameAnalyticsState == null
            || dataFrameAnalyticsState.isNoneOf(DataFrameAnalyticsState.STOPPED, DataFrameAnalyticsState.FAILED);
    }
}
