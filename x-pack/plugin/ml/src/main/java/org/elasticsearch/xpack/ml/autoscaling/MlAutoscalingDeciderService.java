/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.autoscaling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.LifecycleListener;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderContext;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDeciderService;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecision;
import org.elasticsearch.xpack.autoscaling.decision.AutoscalingDecisionType;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.NodeLoadDetector;
import org.elasticsearch.xpack.ml.process.MlMemoryTracker;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.core.ml.MlTasks.getDataFrameAnalyticsState;
import static org.elasticsearch.xpack.core.ml.MlTasks.getJobStateModifiedForReassignments;
import static org.elasticsearch.xpack.ml.job.JobNodeSelector.AWAITING_LAZY_ASSIGNMENT;

public class MlAutoscalingDeciderService implements
    AutoscalingDeciderService<MlAutoscalingDeciderConfiguration>,
    LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(MlAutoscalingDeciderService.class);

    private final NodeLoadDetector nodeLoadDetector;
    private final Map<String, Long> anomalyJobsTimeInQueue;
    private final Map<String, Long> analyticsJobsTimeInQueue;
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
        this.maxMachineMemoryPercent = MachineLearning.MAX_MACHINE_MEMORY_PERCENT.get(settings);
        this.maxOpenJobs = MachineLearning.MAX_OPEN_JOBS_PER_NODE.get(settings);
        this.analyticsJobsTimeInQueue = new ConcurrentHashMap<>();
        this.anomalyJobsTimeInQueue = new ConcurrentHashMap<>();
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
                    nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
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
            nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
        }
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
        long previousTimeStamp = this.lastTimeToScale;
        this.lastTimeToScale = this.timeSupplier.get();
        if (previousTimeStamp == 0L) {
            previousTimeStamp = lastTimeToScale;
        }
        final long timeDiff = Math.max(0L, this.lastTimeToScale - previousTimeStamp);

        final ClusterState clusterState = context.state();

        PersistentTasksCustomMetadata tasks = clusterState.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        Collection<PersistentTask<?>> anomalyDetectionTasks = anomalyDetectionTasks(tasks);
        Collection<PersistentTask<?>> dataframeAnalyticsTasks = dataframeAnalyticsTasks(tasks);
        List<DiscoveryNode> nodes = clusterState.nodes()
            .mastersFirstStream()
            .filter(MachineLearning::isMlNode)
            .collect(Collectors.toList());

        final AutoscalingDecision scaleUpDecision = checkForScaleUp(decider,
            nodes,
            anomalyDetectionTasks,
            dataframeAnalyticsTasks,
            timeDiff);
        if (AutoscalingDecisionType.SCALE_UP.equals(scaleUpDecision.type())) {
            return scaleUpDecision;
        }

        final AutoscalingDecision scaleDownDecision = checkForScaleDown(decider, nodes, clusterState);
        if (AutoscalingDecisionType.SCALE_DOWN.equals(scaleDownDecision.type())) {
            return scaleDownDecision;
        }

        return new AutoscalingDecision(name(),
            AutoscalingDecisionType.NO_SCALE,
            scaleUpDecision.reason() + "|" + scaleDownDecision.reason());
    }

    AutoscalingDecision checkForScaleUp(MlAutoscalingDeciderConfiguration decider,
                                        List<DiscoveryNode> nodes,
                                        Collection<PersistentTask<?>> anomalyDetectionTasks,
                                        Collection<PersistentTask<?>> dataframeAnalyticsTasks,
                                        long timeSinceLastCheckMs) {
        Set<String> waitingAnomalyJobs = anomalyDetectionTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.jobId(t.getId()))
            .collect(Collectors.toSet());
        Set<String> waitingAnalysisJobs = dataframeAnalyticsTasks.stream()
            .filter(t -> AWAITING_LAZY_ASSIGNMENT.equals(t.getAssignment()))
            .map(t -> MlTasks.dataFrameAnalyticsId(t.getId()))
            .collect(Collectors.toSet());
        anomalyJobsTimeInQueue.keySet().retainAll(waitingAnomalyJobs);
        analyticsJobsTimeInQueue.keySet().retainAll(waitingAnalysisJobs);

        if (waitingAnomalyJobs.isEmpty() == false || waitingAnalysisJobs.isEmpty() == false || nodes.size() < decider.getMinNumNodes()) {
            if (nodes.size() < decider.getMinNumNodes() || nodes.isEmpty()) {
                return new AutoscalingDecision(name(),
                    AutoscalingDecisionType.SCALE_UP,
                    "number of machine learning nodes ["
                        + nodes.size()
                        + "] is below the configured minimum number of ["
                        + decider.getMinNumNodes()
                        + "] or is zero");
            }
            Set<String> timedUpJobs = new HashSet<>();
            for (String jobId : waitingAnomalyJobs) {
                long time = anomalyJobsTimeInQueue.compute(jobId, (k, v) -> v == null ? 0L : v + timeSinceLastCheckMs);
                if (time >= decider.getAnomalyJobTimeInQueue().getMillis()) {
                    timedUpJobs.add(jobId);
                }
            }
            for (String jobId : waitingAnalysisJobs) {
                long time = analyticsJobsTimeInQueue.compute(jobId, (k, v) -> v == null ? 0L : v + timeSinceLastCheckMs);
                if (time >= decider.getAnalysisJobTimeInQueue().getMillis()) {
                    timedUpJobs.add(jobId);
                }
            }
            if (timedUpJobs.isEmpty() == false) {
                return new AutoscalingDecision(name(),
                    AutoscalingDecisionType.SCALE_UP,
                    "jobs " + timedUpJobs + " have been waiting for assignment too long");
            }
        }
        return new AutoscalingDecision(name(), AutoscalingDecisionType.NO_SCALE, "no jobs have waited long enough for assignment");
    }

    AutoscalingDecision checkForScaleDown(MlAutoscalingDeciderConfiguration decider,
                                          List<DiscoveryNode> nodes,
                                          ClusterState clusterState) {
        if (nodes.size() == decider.getMinNumNodes()) {
            return new AutoscalingDecision(name(),
                AutoscalingDecisionType.NO_SCALE,
                "| already at configured minimum node count ["
                    + decider.getMinNumNodes()
                    +"]");
        }
        // kick of a refresh if our data around memory usage is stale.
        if (nodeLoadDetector.getMlMemoryTracker().isRecentlyRefreshed() == false) {
            logger.debug("job memory tracker is stale. Request refresh before making scaling decision.");
            nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
            return new AutoscalingDecision(
                name(),
                AutoscalingDecisionType.NO_SCALE,
                "job memory tracker is stale. Unable to make accurate scale down recommendation");
        }
        // TODO: remove in 8.0.0
        boolean allNodesHaveDynamicMaxWorkers = clusterState.getNodes().getMinNodeVersion().onOrAfter(Version.V_7_2_0);
        List<NodeLoadDetector.NodeLoad> nodeLoads = new ArrayList<>();
        boolean isMemoryAccurateFlag = true;
        for (DiscoveryNode node : nodes) {
            NodeLoadDetector.NodeLoad nodeLoad = nodeLoadDetector.detectNodeLoad(clusterState,
                allNodesHaveDynamicMaxWorkers,
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
            logger.info("nodes' view of memory usage is stale. Request refresh before making scaling decision.");
            nodeLoadDetector.getMlMemoryTracker().asyncRefresh();
            return new AutoscalingDecision(name(),
                AutoscalingDecisionType.NO_SCALE,
                "job memory tracker is stale. Unable to make accurate scale down recommendation");
        }
        // TODO check for scale down
        return new AutoscalingDecision(name(),
            AutoscalingDecisionType.NO_SCALE,
            "TODO");
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

    @Override
    public String name() {
        return "ml";
    }

}

