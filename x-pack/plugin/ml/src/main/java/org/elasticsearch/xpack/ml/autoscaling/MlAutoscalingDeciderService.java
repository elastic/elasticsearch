/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingCapacity;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingCapacity.AutoscalingResources;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction.DatafeedParams;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.config.AnalysisLimits;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.xpack.core.ml.MlTasks.getDataFrameAnalyticsState;
import static org.elasticsearch.xpack.core.ml.MlTasks.getJobStateModifiedForReassignments;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

public class MlAutoscalingDeciderService implements
    AutoscalingDeciderService<MlAutoscalingDeciderConfiguration>,
    LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(MlAutoscalingDeciderService.class);
    private static final Duration DEFAULT_MEMORY_REFRESH_RATE = Duration.ofMinutes(15);
    private static final String MEMORY_STALE = "unable to make scaling decision as job memory requirements are stale";

    private final NodeLoadDetector nodeLoadDetector;
    private final MlMemoryTracker mlMemoryTracker;
    private final Supplier<Long> timeSupplier;

    private volatile boolean isMaster;
    private volatile boolean running;
    private volatile int maxMachineMemoryPercent;
    private volatile int maxOpenJobs;
    private volatile long lastTimeToScale;

    public MlAutoscalingDeciderService(MlMemoryTracker memoryTracker, Settings settings, ClusterService clusterService) {
        this(new NodeLoadDetector(memoryTracker), settings, clusterService, System::currentTimeMillis);
    }

    MlAutoscalingDeciderService(NodeLoadDetector nodeLoadDetector,
                                Settings settings,
                                ClusterService clusterService,
                                Supplier<Long> timeSupplier) {
        this.nodeLoadDetector = nodeLoadDetector;
        this.mlMemoryTracker = nodeLoadDetector.getMlMemoryTracker();
        this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxOpenJobs = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.timeSupplier = timeSupplier;
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_MACHINE_MEMORY_PERCENT,
            this::setMaxMachineMemoryPercent);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(MachineLearning.MAX_OPEN_JOBS_PER_NODE, this::setMaxOpenJobs);
        clusterService.addLocalNodeMasterListener(this);
        clusterService.addLifecycleListener(new LifecycleListener() {
            @Override
            public void afterStart() {
                running = true;
                if (isMaster) {
                    mlMemoryTracker.asyncRefresh();
                }
            }

            @Override
            public void beforeStop() {
                running = false;
            }
        });
    }

    void setMaxMachineMemoryPercent(int maxMachineMemoryPercent) {
        this.maxMachineMemoryPercent = maxMachineMemoryPercent;
    }

    void setMaxOpenJobs(int maxOpenJobs) {
        this.maxOpenJobs = maxOpenJobs;
    }

    @Override
    public void onMaster() {
        isMaster = true;
        if (running) {
            mlMemoryTracker.asyncRefresh();
        }
    }

    static AutoscalingCapacity currentScale(final List<DiscoveryNode> nodes) {
        return capacityFromBytes(
            nodes.stream().map(MlAutoscalingDeciderService::getNodeMemory)
                .mapToLong(l -> l.orElse(0L))
                .sum(),
            nodes.stream()
                .map(MlAutoscalingDeciderService::getNodeMemory)
                .mapToLong(l -> l.orElse(-1L))
                .max()
                .orElse(0L));
    }

    static OptionalLong getNodeMemory(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        OptionalLong machineMemory = OptionalLong.empty();
        String machineMemoryStr = nodeAttributes.get(MachineLearning.MACHINE_MEMORY_NODE_ATTR);
        try {
            machineMemory = OptionalLong.of(Long.parseLong(machineMemoryStr));
        } catch (NumberFormatException e) {
            logger.debug(() -> new ParameterizedMessage(
                "could not parse stored machine memory string value [{}] in node attribute [{}]",
                machineMemoryStr,
                MachineLearning.MACHINE_MEMORY_NODE_ATTR));
        }
        return machineMemory;
    }

    static List<DiscoveryNode> getNodes(final ClusterState clusterState) {
        return clusterState.nodes()
            .mastersFirstStream()
            .filter(MachineLearning::isMlNode)
            .collect(Collectors.toList());
    }

    @Override
    public void offMaster() {
        isMaster = false;
    }

    @Override
    public AutoscalingDecision scale(MlAutoscalingDeciderConfiguration decider, AutoscalingDeciderContext context) {
        if (isMaster == false) {
            throw new IllegalArgumentException("request for scaling information is only allowed on the master node");
        }
        final Duration memoryTrackingStale;
        long previousTimeStamp = this.lastTimeToScale;
        this.lastTimeToScale = this.timeSupplier.get();
        if (previousTimeStamp == 0L) {
            memoryTrackingStale = DEFAULT_MEMORY_REFRESH_RATE;
        } else {
            memoryTrackingStale = Duration.ofMillis(TimeValue.timeValueMinutes(1).millis() + this.lastTimeToScale - previousTimeStamp);
        }

        final ClusterState clusterState = context.state();

        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        Collection<PersistentTask<?>> anomalyDetectionTasks = anomalyDetectionTasks(tasks);
        Collection<PersistentTask<?>> dataframeAnalyticsTasks = dataframeAnalyticsTasks(tasks);
        final List<DiscoveryNode> nodes = getNodes(clusterState);
        Optional<MemoryCapacity> futureFreedCapacity = calculateFutureFreedCapacity(tasks, memoryTrackingStale);

        final List<String> waitingAnomalyJobs = anomalyDetectionTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.jobId(t.getId()))
            .collect(Collectors.toList());
        final List<String> waitingAnalyticsJobs = dataframeAnalyticsTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.dataFrameAnalyticsId(t.getId()))
            .collect(Collectors.toList());

        final AutoscalingCapacity currentScale = currentScale(nodes);
        final MlScalingReason.Builder reasonBuilder = MlScalingReason.builder()
                .setWaitingAnomalyJobs(waitingAnomalyJobs)
                .setWaitingAnalyticsJobs(waitingAnalyticsJobs)
                .setCurrentMlCapacity(currentScale)
                .setPassedConfiguration(decider);

        final Optional<AutoscalingDecision> scaleUpDecision = checkForScaleUp(decider,
            waitingAnomalyJobs,
            waitingAnalyticsJobs,
            futureFreedCapacity.orElse(null),
            currentScale,
            reasonBuilder);

        if (scaleUpDecision.isPresent()) {
            return scaleUpDecision.get();
        }
        if (waitingAnalyticsJobs.isEmpty() == false || waitingAnomalyJobs.isEmpty() == false) {
            return new AutoscalingDecision(
                currentScale,
                reasonBuilder
                    .setSimpleReason("Passing currently perceived capacity as there are analytics and anomaly jobs in the queue, " +
                        "but the number in the queue is less than the configured maximum allowed.")
                    .build());
        }
        if (mlMemoryTracker.isRecentlyRefreshed(memoryTrackingStale) == false) {
            return buildDecisionAndRequestRefresh(reasonBuilder);
        }

        return checkForScaleDown(nodes, clusterState, reasonBuilder);
    }

    Optional<AutoscalingDecision> checkForScaleUp(MlAutoscalingDeciderConfiguration decider,
                                                  List<String> waitingAnomalyJobs,
                                                  List<String> waitingAnalyticsJobs,
                                                  @Nullable MemoryCapacity futureFreedCapacity,
                                                  AutoscalingCapacity currentScale,
                                                  MlScalingReason.Builder reasonBuilder) {

        // Are we in breach of maximum waiting jobs?
        if (waitingAnalyticsJobs.size() > decider.getNumAnalyticsJobsInQueue()
            || waitingAnomalyJobs.size() > decider.getNumAnomalyJobsInQueue()) {
            MemoryCapacity updatedCapacity = MemoryCapacity.from(currentScale);
            Optional<MemoryCapacity> analyticsCapacity = requiredCapacityForUnassignedJobs(waitingAnalyticsJobs,
                this::getAnalyticsMemoryRequirement,
                // TODO Better default???
                AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB,
                decider.getNumAnalyticsJobsInQueue());
            Optional<MemoryCapacity> anomalyCapacity = requiredCapacityForUnassignedJobs(waitingAnomalyJobs,
                this::getAnomalyMemoryRequirement,
                AnalysisLimits.DEFAULT_MODEL_MEMORY_LIMIT_MB,
                decider.getNumAnomalyJobsInQueue());

            updatedCapacity.merge(anomalyCapacity.orElse(MemoryCapacity.ZERO)).merge(analyticsCapacity.orElse(MemoryCapacity.ZERO));
            return Optional.of(new AutoscalingDecision(
                updatedCapacity.autoscalingCapacity(),
                reasonBuilder.setSimpleReason("requesting scale up as number of jobs in queues exceeded configured limit").build()));
        }

        // Could the currently waiting jobs ever be assigned?
        if (waitingAnalyticsJobs.isEmpty() == false || waitingAnomalyJobs.isEmpty() == false) {
            // we are unable to determine new tier size, but maybe we can see if our nodes are big enough.
            if (futureFreedCapacity == null) {
                Optional<Long> maxSize = Stream.concat(
                    waitingAnalyticsJobs.stream().map(mlMemoryTracker::getDataFrameAnalyticsJobMemoryRequirement),
                    waitingAnomalyJobs.stream().map(mlMemoryTracker::getAnomalyDetectorJobMemoryRequirement))
                    .filter(Objects::nonNull)
                    .max(Long::compareTo);
                if (maxSize.isPresent() && maxSize.get() > currentScale.node().memory().getBytes()) {
                    mlMemoryTracker.asyncRefresh();
                    return Optional.of(new AutoscalingDecision(
                        capacityFromBytes(Math.max(currentScale.tier().memory().getBytes(), maxSize.get()), maxSize.get()),
                        reasonBuilder.setSimpleReason("requesting scale up as there is no node large enough to handle queued jobs")
                            .build()));
                }
                // we have no memory tracking, need to refresh and return a no_scale
                return Optional.of(buildDecisionAndRequestRefresh(reasonBuilder));
            }
            long newTierNeeded = 0L;
            // could any of the nodes actually run the job?
            long newNodeMax = currentScale.node().memory().getBytes();
            for (String analyticsJob : waitingAnalyticsJobs) {
                Long requiredMemory = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(analyticsJob);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                if (futureFreedCapacity.node < requiredMemory) {
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
                if (futureFreedCapacity.node < requiredMemory) {
                    newTierNeeded = Math.max(requiredMemory, newTierNeeded);
                }
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            if (newNodeMax > currentScale.node().memory().getBytes() || newTierNeeded > 0L) {
                return Optional.of(new AutoscalingDecision(
                    capacityFromBytes(currentScale.tier().memory().getBytes() + newTierNeeded,
                        Math.max(currentScale.node().memory().getBytes(), newNodeMax)),
                    reasonBuilder
                        .setSimpleReason("scaling up as adequate space would not automatically become available when running jobs finish")
                        .build()
                ));
            }
        }

        return Optional.empty();
    }

    // This calculates the the following the potentially automatically free capacity of sometime in the future
    // Since jobs with lookback only datafeeds, and data frame analytics jobs all have some potential future end date
    // we can assume (without user intervention) that these will eventually stop and free their currently occupied resources.
    //
    // The capacity is as follows:
    //  tier: The sum total of the resources that will be removed
    //  node: The largest block of memory that will be freed on a given node.
    //      - If > 1 "batch" ml tasks are running on the same node, we sum their resources.
    Optional<MemoryCapacity> calculateFutureFreedCapacity(PersistentTasksCustomMetadata tasks, Duration jobMemoryExpiry) {
        final List<PersistentTask<DatafeedParams>> jobsWithLookbackDatafeeds = datafeedTasks(tasks).stream()
            .filter(t -> t.getParams().getEndTime() != null && t.getExecutorNode() != null)
            .collect(Collectors.toList());
        final List<PersistentTask<?>> assignedAnalyticsJobs = dataframeAnalyticsTasks(tasks).stream()
            .filter(t -> t.getExecutorNode() != null)
            .collect(Collectors.toList());

        if (jobsWithLookbackDatafeeds.isEmpty() && assignedAnalyticsJobs.isEmpty()) {
            return Optional.of(MemoryCapacity.ZERO);
        }
        if (mlMemoryTracker.isRecentlyRefreshed(jobMemoryExpiry) == false) {
            return Optional.empty();
        }
        // What is the largest chunk of memory that could be freed on a node in the future
        Map<String, Long> maxNodeBytes = new HashMap<>();
        for (PersistentTask<DatafeedParams> lookbackOnlyDf : jobsWithLookbackDatafeeds) {
            Long jobSize = mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(lookbackOnlyDf.getParams().getJobId());
            if (jobSize == null) {
                return Optional.empty();
            }
            maxNodeBytes.compute(lookbackOnlyDf.getExecutorNode(), (_k, v) -> v == null ? jobSize : jobSize + v);
        }
        for (PersistentTask<?> task : assignedAnalyticsJobs) {
            Long jobSize = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(MlTasks.dataFrameAnalyticsId(task.getId()));
            if (jobSize == null) {
                return Optional.empty();
            }
            maxNodeBytes.compute(task.getExecutorNode(), (_k, v) -> v == null ? jobSize : jobSize + v);
        }
        return Optional.of(new MemoryCapacity(
            maxNodeBytes.values().stream().mapToLong(Long::longValue).sum(),
            maxNodeBytes.values().stream().mapToLong(Long::longValue).max().orElse(0L)));
    }

    private AutoscalingDecision buildDecisionAndRequestRefresh(MlScalingReason.Builder reasonBuilder) {
        mlMemoryTracker.asyncRefresh();
        return new AutoscalingDecision(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build());
    }

    private static AutoscalingCapacity capacityFromBytes(long tierBytes, long nodeBytes) {
        return new AutoscalingCapacity(
            new AutoscalingResources(null, new ByteSizeValue(tierBytes)),
            new AutoscalingResources(null, new ByteSizeValue(nodeBytes)));
    }

    /**
     * @param unassignedJobs The list of unassigned jobs
     * @param sizeFunction Function providing the memory required for a job
     * @param defaultSize The default memory size (if the sizeFunction returns null)
     * @param maxNumInQueue The number of unassigned jobs allowed.
     * @return The capacity needed to reduce the length of `unassignedJobs` to `maxNumInQueue`
     */
    static Optional<MemoryCapacity> requiredCapacityForUnassignedJobs(List<String> unassignedJobs,
                                                                      Function<String, Long> sizeFunction,
                                                                      long defaultSize,
                                                                      int maxNumInQueue) {
        List<Long> jobSizes = unassignedJobs
            .stream()
            // TODO do we want to verify memory requirements aren't stale? Or just consider `null` a fastpath?
            .map(sizeFunction)
            .map(l -> l == null ? defaultSize : l)
            .collect(Collectors.toList());
        // Only possible if unassignedJobs was empty.
        if (jobSizes.isEmpty()) {
            return Optional.empty();
        }
        jobSizes.sort(Comparator.comparingLong(Long::longValue).reversed());
        long tierMemory = 0L;
        long nodeMemory = jobSizes.get(0);
        Iterator<Long> iter = jobSizes.iterator();
        while (jobSizes.size() > maxNumInQueue && iter.hasNext()) {
            tierMemory += iter.next();
            iter.remove();
        }
        return Optional.of(new MemoryCapacity(tierMemory, nodeMemory));
    }

    private Long getAnalyticsMemoryRequirement(String analyticsId) {
        return mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(analyticsId);
    }

    private Long getAnomalyMemoryRequirement(String anomalyId) {
        return mlMemoryTracker.getAnomalyDetectorJobMemoryRequirement(anomalyId);
    }

    // TODO, actually calculate scale down,
    //  but only return it as a scale option IF cool down period has passed (AFTER it was previously calculated)
    AutoscalingDecision checkForScaleDown(List<DiscoveryNode> nodes, ClusterState clusterState, MlScalingReason.Builder reasonBuilder) {
        List<NodeLoadDetector.NodeLoad> nodeLoads = new ArrayList<>();
        boolean isMemoryAccurateFlag = true;
        for (DiscoveryNode node : nodes) {
            NodeLoadDetector.NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(clusterState,
                true,
                node,
                maxOpenJobs,
                maxMachineMemoryPercent,
                true);
            if (nodeLoad.getError() != null) {
                logger.warn("[{}] failed to gather node load limits, failure [{}]", node.getId(), nodeLoad.getError());
                continue;
            }
            nodeLoads.add(nodeLoad);
            isMemoryAccurateFlag = isMemoryAccurateFlag && nodeLoad.isUseMemory();
        }
        // Even if we verify that memory usage is up today before checking node capacity, we could still run into stale information.
        // We should not make a decision if the memory usage is stale/inaccurate.
        if (isMemoryAccurateFlag == false) {
            return buildDecisionAndRequestRefresh(reasonBuilder);
        }
        // TODO check for scale down
        return new AutoscalingDecision(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build());
    }

    private static Collection<PersistentTask<?>> anomalyDetectionTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.JOB_TASK_NAME,
            t -> getJobStateModifiedForReassignments(t).isAnyOf(JobState.OPENED, JobState.OPENING));
    }

    private static Collection<PersistentTask<?>> dataframeAnalyticsTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME,
            t -> getDataFrameAnalyticsState(t).isAnyOf(DataFrameAnalyticsState.STARTED, DataFrameAnalyticsState.STARTING));
    }

    @SuppressWarnings("unchecked")
    private static Collection<PersistentTask<DatafeedParams>> datafeedTasks(PersistentTasksCustomMetadata tasksCustomMetadata) {
        if (tasksCustomMetadata == null) {
            return Collections.emptyList();
        }

        return tasksCustomMetadata.findTasks(MlTasks.DATAFEED_TASK_NAME, t -> true)
            .stream()
            .map(p -> (PersistentTask<DatafeedParams>)p)
            .collect(Collectors.toList());
    }

    @Override
    public String name() {
        return "ml";
    }

    static class MemoryCapacity {

        static final MemoryCapacity ZERO = new MemoryCapacity(0L, 0L);

        long tier;
        long node;

        static MemoryCapacity from(AutoscalingCapacity autoscalingCapacity) {
            return new MemoryCapacity(autoscalingCapacity.tier().memory().getBytes(), autoscalingCapacity.node().memory().getBytes());
        }

        MemoryCapacity(long tier, long node) {
            this.tier = tier;
            this.node = node;
        }

        MemoryCapacity merge(MemoryCapacity memoryCapacity) {
            this.tier += memoryCapacity.tier;
            this.node = Math.max(memoryCapacity.node, this.node);
            return this;
        }

        AutoscalingCapacity autoscalingCapacity() {
            return new AutoscalingCapacity(
                new AutoscalingResources(null, new ByteSizeValue(tier)),
                new AutoscalingResources(null, new ByteSizeValue(node)));
        }

        public long getTier() {
            return tier;
        }

        public long getNode() {
            return node;
        }
    }

}

