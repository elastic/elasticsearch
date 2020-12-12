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
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentElasticsearchExtension;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderResult;
import org.elasticsearch.xpack.autoscaling.capacity.AutoscalingDeciderService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction.DatafeedParams;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoad;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;
import org.elasticsearch.xpack.ml.utils.NativeMemoryCalculator;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
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

public class MlAutoscalingDeciderService implements AutoscalingDeciderService,
    LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(MlAutoscalingDeciderService.class);
    private static final Duration DEFAULT_MEMORY_REFRESH_RATE = Duration.ofMinutes(15);
    private static final String MEMORY_STALE = "unable to make scaling decision as job memory requirements are stale";
    private static final long NO_SCALE_DOWN_POSSIBLE = -1L;

    public static final String NAME = "ml";
    public static final Setting<Integer> NUM_ANOMALY_JOBS_IN_QUEUE = Setting.intSetting("num_anomaly_jobs_in_queue", 0, 0);
    public static final Setting<Integer> NUM_ANALYTICS_JOBS_IN_QUEUE = Setting.intSetting("num_analytics_jobs_in_queue", 0, 0);
    public static final Setting<TimeValue> DOWN_SCALE_DELAY = Setting.timeSetting("down_scale_delay", TimeValue.timeValueHours(1));

    private final NodeLoadDetector nodeLoadDetector;
    private final MlMemoryTracker mlMemoryTracker;
    private final Supplier<Long> timeSupplier;
    private final boolean useAuto;

    private volatile boolean isMaster;
    private volatile boolean running;
    private volatile int maxMachineMemoryPercent;
    private volatile int maxOpenJobs;
    private volatile long lastTimeToScale;
    private volatile long scaleDownDetected;

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
        this.useAuto = MachineLearning.USE_AUTO_MACHINE_MEMORY_PERCENT.get(settings);
        this.timeSupplier = timeSupplier;
        this.scaleDownDetected = NO_SCALE_DOWN_POSSIBLE;
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

    static OptionalLong getNodeJvmSize(DiscoveryNode node) {
        Map<String, String> nodeAttributes = node.getAttributes();
        OptionalLong value = OptionalLong.empty();
        String valueStr = nodeAttributes.get(MachineLearning.MAX_JVM_SIZE_NODE_ATTR);
        try {
            value = OptionalLong.of(Long.parseLong(valueStr));
        } catch (NumberFormatException e) {
            logger.debug(() -> new ParameterizedMessage(
                "could not parse stored string value [{}] in node attribute [{}]",
                valueStr,
                MachineLearning.MAX_JVM_SIZE_NODE_ATTR));
        }
        return value;
    }

    static List<DiscoveryNode> getNodes(final ClusterState clusterState) {
        return clusterState.nodes()
            .mastersFirstStream()
            .filter(MachineLearning::isMlNode)
            .collect(Collectors.toList());
    }

    /**
     * @param unassignedJobs The list of unassigned jobs
     * @param sizeFunction   Function providing the memory required for a job
     * @param maxNumInQueue  The number of unassigned jobs allowed.
     * @return The capacity needed to reduce the length of `unassignedJobs` to `maxNumInQueue`
     */
    static Optional<NativeMemoryCapacity> requiredCapacityForUnassignedJobs(List<String> unassignedJobs,
                                                                            Function<String, Long> sizeFunction,
                                                                            int maxNumInQueue) {
        List<Long> jobSizes = unassignedJobs
            .stream()
            .map(sizeFunction)
            .map(l -> l == null ? 0L : l)
            .collect(Collectors.toList());
        // Only possible if unassignedJobs was empty.
        if (jobSizes.isEmpty()) {
            return Optional.empty();
        }
        jobSizes.sort(Comparator.comparingLong(Long::longValue).reversed());
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
            .map(p -> (PersistentTask<DatafeedParams>) p)
            .collect(Collectors.toList());
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

    private void resetScaleDownCoolDown() {
        this.scaleDownDetected = NO_SCALE_DOWN_POSSIBLE;
    }

    private boolean canScaleDown(TimeValue coolDown) {
        if (this.scaleDownDetected == NO_SCALE_DOWN_POSSIBLE) {
            return false;
        }
        return timeSupplier.get() - scaleDownDetected >= coolDown.millis();
    }

    private boolean newScaleDownCheck() {
        return scaleDownDetected == NO_SCALE_DOWN_POSSIBLE;
    }

    public static NativeMemoryCapacity currentScale(final List<DiscoveryNode> machineLearningNodes,
                                                    int maxMachineMemoryPercent,
                                                    boolean useAuto) {
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
        Optional<NativeMemoryCapacity> futureFreedCapacity = calculateFutureFreedCapacity(tasks, memoryTrackingStale);

        final List<String> waitingAnomalyJobs = anomalyDetectionTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.jobId(t.getId()))
            .collect(Collectors.toList());
        final List<String> waitingAnalyticsJobs = dataframeAnalyticsTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.dataFrameAnalyticsId(t.getId()))
            .collect(Collectors.toList());

        final NativeMemoryCapacity currentScale = currentScale(nodes);
        final MlScalingReason.Builder reasonBuilder = MlScalingReason.builder()
            .setWaitingAnomalyJobs(waitingAnomalyJobs)
            .setWaitingAnalyticsJobs(waitingAnalyticsJobs)
            .setCurrentMlCapacity(currentScale.autoscalingCapacity(maxMachineMemoryPercent, useAuto))
            .setPassedConfiguration(configuration);

        final Optional<AutoscalingDeciderResult> scaleUpDecision = checkForScaleUp(
            NUM_ANOMALY_JOBS_IN_QUEUE.get(configuration),
            NUM_ANALYTICS_JOBS_IN_QUEUE.get(configuration),
            waitingAnomalyJobs,
            waitingAnalyticsJobs,
            futureFreedCapacity.orElse(null),
            currentScale,
            reasonBuilder);

        if (scaleUpDecision.isPresent()) {
            resetScaleDownCoolDown();
            return scaleUpDecision.get();
        }
        if (waitingAnalyticsJobs.isEmpty() == false || waitingAnomalyJobs.isEmpty() == false) {
            // We don't want to continue to consider a scale down if there are now waiting jobs
            resetScaleDownCoolDown();
            noScaleResultOrRefresh(reasonBuilder, memoryTrackingStale, new AutoscalingDeciderResult(
                context.currentCapacity(),
                reasonBuilder
                    .setSimpleReason("Passing currently perceived capacity as there are analytics and anomaly jobs in the queue, " +
                        "but the number in the queue is less than the configured maximum allowed.")
                    .build()));
        }
        if (mlMemoryTracker.isRecentlyRefreshed(memoryTrackingStale) == false) {
            logger.debug(() -> new ParameterizedMessage(
                "view of job memory is stale given duration [{}]. Not attempting to scale down",
                memoryTrackingStale));
            return buildDecisionAndRequestRefresh(reasonBuilder);
        }

        long largestJob = Math.max(
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
                .orElse(0L));

        final Optional<AutoscalingDeciderResult> scaleDownDecision =
            checkForScaleDown(nodes, clusterState, largestJob, currentScale, reasonBuilder);

        if (scaleDownDecision.isPresent()) {
            if (newScaleDownCheck()) {
                scaleDownDetected = timeSupplier.get();
            }
            TimeValue downScaleDelay = DOWN_SCALE_DELAY.get(configuration);
            if (canScaleDown(downScaleDelay)) {
                return scaleDownDecision.get();
            }
            logger.debug(() -> new ParameterizedMessage(
                "not scaling down as the current scale down delay [{}] is not satisfied. The last time scale down was detected [{}]",
                downScaleDelay.getStringRep(),
                XContentElasticsearchExtension.DEFAULT_DATE_PRINTER.print(scaleDownDetected)));
            return new AutoscalingDeciderResult(
                context.currentCapacity(),
                reasonBuilder
                    .setSimpleReason(
                        "Passing currently perceived capacity as configured down scale delay has not be satisfied; configured delay ["
                            + downScaleDelay.millis()
                            + "] last detected scale down event ["
                            + scaleDownDetected
                            + "]")
                    .build());
        }

        return noScaleResultOrRefresh(reasonBuilder, memoryTrackingStale, new AutoscalingDeciderResult(context.currentCapacity(),
            reasonBuilder
                .setSimpleReason("Passing currently perceived capacity as no scaling changes were detected to be possible")
                .build()));
    }

    AutoscalingDeciderResult noScaleResultOrRefresh(MlScalingReason.Builder reasonBuilder,
                                                    Duration memoryTrackingStale,
                                                    AutoscalingDeciderResult potentialResult) {
        if (mlMemoryTracker.isRecentlyRefreshed(memoryTrackingStale) == false) {
            logger.debug(() -> new ParameterizedMessage(
                "current view of job memory is stale given the duration [{}]. Returning a no scale event",
                memoryTrackingStale.toString()));
            return buildDecisionAndRequestRefresh(reasonBuilder);
        } else {
            return potentialResult;
        }
    }

    Optional<AutoscalingDeciderResult> checkForScaleUp(int numAnomalyJobsInQueue,
                                                       int numAnalyticsJobsInQueue,
                                                       List<String> waitingAnomalyJobs,
                                                       List<String> waitingAnalyticsJobs,
                                                       @Nullable NativeMemoryCapacity futureFreedCapacity,
                                                       NativeMemoryCapacity currentScale,
                                                       MlScalingReason.Builder reasonBuilder) {

        // Are we in breach of maximum waiting jobs?
        if (waitingAnalyticsJobs.size() > numAnalyticsJobsInQueue
            || waitingAnomalyJobs.size() > numAnomalyJobsInQueue) {
            NativeMemoryCapacity updatedCapacity = NativeMemoryCapacity.from(currentScale);
            Optional<NativeMemoryCapacity> analyticsCapacity = requiredCapacityForUnassignedJobs(waitingAnalyticsJobs,
                this::getAnalyticsMemoryRequirement,
                numAnalyticsJobsInQueue);
            Optional<NativeMemoryCapacity> anomalyCapacity = requiredCapacityForUnassignedJobs(waitingAnomalyJobs,
                this::getAnomalyMemoryRequirement,
                numAnomalyJobsInQueue);

            updatedCapacity.merge(anomalyCapacity.orElse(NativeMemoryCapacity.ZERO))
                .merge(analyticsCapacity.orElse(NativeMemoryCapacity.ZERO));
            return Optional.of(new AutoscalingDeciderResult(
                updatedCapacity.autoscalingCapacity(maxMachineMemoryPercent, useAuto),
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
                if (maxSize.isPresent() && maxSize.get() > currentScale.getNode()) {
                    return Optional.of(new AutoscalingDeciderResult(
                        new NativeMemoryCapacity(Math.max(currentScale.getTier(), maxSize.get()), maxSize.get())
                            .autoscalingCapacity(maxMachineMemoryPercent, useAuto),
                        reasonBuilder.setSimpleReason("requesting scale up as there is no node large enough to handle queued jobs")
                            .build()));
                }
                // we have no info, allow the caller to make the appropriate action, probably returning a no_scale
                return Optional.empty();
            }
            long newTierNeeded = 0L;
            // could any of the nodes actually run the job?
            long newNodeMax = currentScale.getNode();
            for (String analyticsJob : waitingAnalyticsJobs) {
                Long requiredMemory = mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(analyticsJob);
                // it is OK to continue here as we have not breached our queuing limit
                if (requiredMemory == null) {
                    continue;
                }
                // Is there "future capacity" on a node that could run this job? If not, we need that much more in the tier.
                if (futureFreedCapacity.getNode() < requiredMemory) {
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
                if (futureFreedCapacity.getNode() < requiredMemory) {
                    newTierNeeded = Math.max(requiredMemory, newTierNeeded);
                }
                newNodeMax = Math.max(newNodeMax, requiredMemory);
            }
            if (newNodeMax > currentScale.getNode() || newTierNeeded > 0L) {
                NativeMemoryCapacity newCapacity = new NativeMemoryCapacity(newTierNeeded, newNodeMax);
                return Optional.of(new AutoscalingDeciderResult(
                    // We need more memory in the tier, or our individual node size requirements has increased
                    NativeMemoryCapacity.from(currentScale).merge(newCapacity).autoscalingCapacity(maxMachineMemoryPercent, useAuto),
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
    Optional<NativeMemoryCapacity> calculateFutureFreedCapacity(PersistentTasksCustomMetadata tasks, Duration jobMemoryExpiry) {
        final List<PersistentTask<DatafeedParams>> jobsWithLookbackDatafeeds = datafeedTasks(tasks).stream()
            .filter(t -> t.getParams().getEndTime() != null && t.getExecutorNode() != null)
            .collect(Collectors.toList());
        final List<PersistentTask<?>> assignedAnalyticsJobs = dataframeAnalyticsTasks(tasks).stream()
            .filter(t -> t.getExecutorNode() != null)
            .collect(Collectors.toList());

        if (jobsWithLookbackDatafeeds.isEmpty() && assignedAnalyticsJobs.isEmpty()) {
            return Optional.of(NativeMemoryCapacity.ZERO);
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
        return Optional.of(new NativeMemoryCapacity(
            maxNodeBytes.values().stream().mapToLong(Long::longValue).sum(),
            maxNodeBytes.values().stream().mapToLong(Long::longValue).max().orElse(0L)));
    }

    private AutoscalingDeciderResult buildDecisionAndRequestRefresh(MlScalingReason.Builder reasonBuilder) {
        mlMemoryTracker.asyncRefresh();
        return new AutoscalingDeciderResult(null, reasonBuilder.setSimpleReason(MEMORY_STALE).build());
    }

    private Long getAnalyticsMemoryRequirement(String analyticsId) {
        return mlMemoryTracker.getDataFrameAnalyticsJobMemoryRequirement(analyticsId);
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

    Optional<AutoscalingDeciderResult> checkForScaleDown(List<DiscoveryNode> nodes,
                                                         ClusterState clusterState,
                                                         long largestJob,
                                                         NativeMemoryCapacity currentCapacity,
                                                         MlScalingReason.Builder reasonBuilder) {
        List<NodeLoad> nodeLoads = new ArrayList<>();
        boolean isMemoryAccurateFlag = true;
        for (DiscoveryNode node : nodes) {
            NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(clusterState,
                true,
                node,
                maxOpenJobs,
                maxMachineMemoryPercent,
                true,
                useAuto);
            if (nodeLoad.getError() != null) {
                logger.warn("[{}] failed to gather node load limits, failure [{}]. Returning no scale",
                    node.getId(),
                    nodeLoad.getError());
                return Optional.empty();
            }
            nodeLoads.add(nodeLoad);
            isMemoryAccurateFlag = isMemoryAccurateFlag && nodeLoad.isUseMemory();
        }
        // Even if we verify that memory usage is up to date before checking node capacity, we could still run into stale information.
        // We should not make a decision if the memory usage is stale/inaccurate.
        if (isMemoryAccurateFlag == false) {
            assert isMemoryAccurateFlag : "view of memory is inaccurate after recent check";
            return Optional.empty();
        }
        long currentlyNecessaryTier = nodeLoads.stream().mapToLong(NodeLoad::getAssignedJobMemory).sum();
        // The required NATIVE node memory is the largest job and our static overhead.
        long currentlyNecessaryNode = largestJob == 0 ? 0 : largestJob + MachineLearning.NATIVE_EXECUTABLE_CODE_OVERHEAD.getBytes();
        // We consider a scale down if we are not fully utilizing the tier
        // Or our largest job could be on a smaller node (meaning the same size tier but smaller nodes are possible).
        if (currentlyNecessaryTier < currentCapacity.getTier() || currentlyNecessaryNode < currentCapacity.getNode()) {
            NativeMemoryCapacity nativeMemoryCapacity = new NativeMemoryCapacity(
                currentlyNecessaryTier,
                currentlyNecessaryNode,
                // If our newly suggested native capacity is the same, we can use the previously stored jvm size
                currentlyNecessaryNode == currentCapacity.getNode() ? currentCapacity.getJvmSize() : null);
            return Optional.of(
                new AutoscalingDeciderResult(
                    nativeMemoryCapacity.autoscalingCapacity(maxMachineMemoryPercent, useAuto),
                    reasonBuilder.setSimpleReason("Requesting scale down as tier and/or node size could be smaller").build()
                )
            );
        }

        return Optional.empty();
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
        return List.of(MachineLearning.ML_ROLE);
    }
}

