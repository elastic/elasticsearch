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
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.decider.EnableAssignmentDecider;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;
import org.elasticsearch.xpack.core.ml.utils.MlTaskState;
import org.elasticsearch.xpack.ml.datafeed.DatafeedNodeSelector;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;
import org.elasticsearch.xpack.ml.utils.MlInvariantLogger;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MlAssignmentNotifier implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MlAssignmentNotifier.class);

    static final Duration MIN_CHECK_UNASSIGNED_INTERVAL = Duration.ofSeconds(30);
    static final Duration LONG_TIME_UNASSIGNED_INTERVAL = Duration.ofMinutes(15);
    static final Duration MIN_REPORT_INTERVAL = Duration.ofHours(6);

    private final AnomalyDetectionAuditor anomalyDetectionAuditor;
    private final DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor;
    private final ThreadPool threadPool;
    private final Clock clock;
    private final IndexNameExpressionResolver resolver;
    private final ClusterService clusterService;
    private final MlInvariantLogger invariantLogger = new MlInvariantLogger();
    private Map<TaskNameAndId, UnassignedTimeAndReportTime> unassignedInfoByTask = Map.of();
    private volatile Map<String, UnassignedTimeAndReportTime> datafeedWithoutTaskInfoById = Map.of();
    private volatile Instant lastLogCheck;

    MlAssignmentNotifier(
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndexNameExpressionResolver resolver
    ) {
        this(anomalyDetectionAuditor, dataFrameAnalyticsAuditor, threadPool, clusterService, resolver, Clock.systemUTC());
    }

    MlAssignmentNotifier(
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor,
        ThreadPool threadPool,
        ClusterService clusterService,
        IndexNameExpressionResolver resolver,
        Clock clock
    ) {
        this.anomalyDetectionAuditor = anomalyDetectionAuditor;
        this.dataFrameAnalyticsAuditor = dataFrameAnalyticsAuditor;
        this.threadPool = threadPool;
        this.resolver = resolver;
        this.clusterService = clusterService;
        this.clock = clock;
        this.lastLogCheck = clock.instant();
        clusterService.addListener(this);
    }

    private static String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (event.localNodeMaster() == false) {
            unassignedInfoByTask = Map.of();
            datafeedWithoutTaskInfoById = Map.of();
            return;
        }

        Instant now = clock.instant();
        if (lastLogCheck.plus(MIN_CHECK_UNASSIGNED_INTERVAL).isBefore(now)) {
            lastLogCheck = now;
            threadPool.executor(executorName()).execute(() -> logLongTimeUnassigned(now, event.state()));
        }

        if (event.metadataChanged() == false) {
            return;
        }

        threadPool.executor(executorName()).execute(() -> auditChangesToMlTasks(event));
    }

    private void auditChangesToMlTasks(ClusterChangedEvent event) {

        for (ProjectMetadata project : event.state().getMetadata().projects().values()) {
            final Metadata previousMetadata = event.previousState().getMetadata();
            if (previousMetadata.hasProject(project.id()) == false) {
                continue;
            }
            final ProjectMetadata previousProject = previousMetadata.getProject(project.id());
            PersistentTasksCustomMetadata previousTasks = previousProject.custom(PersistentTasksCustomMetadata.TYPE);
            PersistentTasksCustomMetadata currentTasks = project.custom(PersistentTasksCustomMetadata.TYPE);

            if (Objects.equals(previousTasks, currentTasks)) {
                continue;
            }
            auditMlTasks(project.id(), event.previousState().nodes(), event.state().nodes(), previousTasks, currentTasks, false);
        }
    }

    /**
     * Creates an audit warning for all currently unassigned ML
     * tasks, even if a previous audit warning has been created.
     * Care must be taken not to call this method frequently.
     */
    public void auditUnassignedMlTasks(ProjectId projectId, DiscoveryNodes nodes, PersistentTasksCustomMetadata tasks) {
        auditMlTasks(projectId, nodes, nodes, tasks, tasks, true);
    }

    private void auditMlTasks(
        ProjectId projectId,
        DiscoveryNodes previousNodes,
        DiscoveryNodes currentNodes,
        PersistentTasksCustomMetadata previousTasks,
        PersistentTasksCustomMetadata currentTasks,
        boolean alwaysAuditUnassigned
    ) {
        if (currentTasks == null) {
            return;
        }

        if (Metadata.DEFAULT_PROJECT_ID.equals(projectId) == false) {
            @FixForMultiProject
            final Metadata.MultiProjectPendingException exception = new Metadata.MultiProjectPendingException(
                "Can only audit ML changes on the default project"
            );
            throw exception;
        }

        for (PersistentTask<?> currentTask : currentTasks.tasks()) {
            Assignment currentAssignment = currentTask.getAssignment();
            PersistentTask<?> previousTask = previousTasks != null ? previousTasks.getTask(currentTask.getId()) : null;
            Assignment previousAssignment = previousTask != null ? previousTask.getAssignment() : null;

            boolean isTaskAssigned = (currentAssignment.getExecutorNode() != null);
            if (Objects.equals(currentAssignment, previousAssignment) && (isTaskAssigned || alwaysAuditUnassigned == false)) {
                continue;
            }
            boolean wasTaskAssigned = (previousAssignment != null) && (previousAssignment.getExecutorNode() != null);

            if (MlTasks.JOB_TASK_NAME.equals(currentTask.getTaskName())) {
                String jobId = ((OpenJobAction.JobParams) currentTask.getParams()).getJobId();
                if (isTaskAssigned) {
                    String msg = "Opening job";
                    if (anomalyDetectionAuditor.includeNodeInfo()) {
                        String nodeName = nodeName(currentNodes, currentAssignment.getExecutorNode());
                        msg += " on node [" + nodeName + "]";
                    }
                    anomalyDetectionAuditor.info(jobId, msg);
                } else if (alwaysAuditUnassigned) {
                    if (anomalyDetectionAuditor.includeNodeInfo()) {
                        anomalyDetectionAuditor.warning(
                            jobId,
                            "No node found to open job. Reasons [" + currentAssignment.getExplanation() + "]"
                        );
                    } else {
                        anomalyDetectionAuditor.warning(jobId, "Awaiting capacity to open job.");
                    }
                } else if (wasTaskAssigned) {
                    if (anomalyDetectionAuditor.includeNodeInfo()) {
                        String nodeName = nodeName(previousNodes, previousAssignment.getExecutorNode());
                        anomalyDetectionAuditor.info(jobId, "Job unassigned from node [" + nodeName + "]");
                    } else {
                        anomalyDetectionAuditor.info(jobId, "Job relocating.");
                    }
                }
            } else if (MlTasks.DATAFEED_TASK_NAME.equals(currentTask.getTaskName())) {
                StartDatafeedAction.DatafeedParams datafeedParams = (StartDatafeedAction.DatafeedParams) currentTask.getParams();
                String jobId = datafeedParams.getJobId();
                if (jobId != null) {
                    if (isTaskAssigned) {
                        String msg = "Starting datafeed [" + datafeedParams.getDatafeedId() + "]";
                        if (anomalyDetectionAuditor.includeNodeInfo()) {
                            String nodeName = nodeName(currentNodes, currentAssignment.getExecutorNode());
                            msg += "] on node [" + nodeName + "]";
                        }
                        anomalyDetectionAuditor.info(jobId, msg);
                    } else {
                        if (alwaysAuditUnassigned) {
                            if (anomalyDetectionAuditor.includeNodeInfo()) {
                                anomalyDetectionAuditor.warning(
                                    jobId,
                                    "No node found to start datafeed ["
                                        + datafeedParams.getDatafeedId()
                                        + "]. Reasons ["
                                        + currentAssignment.getExplanation()
                                        + "]"
                                );
                            } else {
                                anomalyDetectionAuditor.warning(
                                    jobId,
                                    "Awaiting capacity to start datafeed [" + datafeedParams.getDatafeedId() + "]."
                                );
                            }
                        } else if (wasTaskAssigned) {
                            if (anomalyDetectionAuditor.includeNodeInfo()) {
                                String nodeName = nodeName(previousNodes, previousAssignment.getExecutorNode());
                                anomalyDetectionAuditor.info(
                                    jobId,
                                    "Datafeed [" + datafeedParams.getDatafeedId() + "] unassigned from node [" + nodeName + "]"
                                );
                            } else {
                                anomalyDetectionAuditor.info(jobId, "Datafeed [" + datafeedParams.getDatafeedId() + "] relocating.");
                            }
                        } else {
                            logger.warn(
                                "[{}] No node found to start datafeed [{}]. Reasons [{}]",
                                jobId,
                                datafeedParams.getDatafeedId(),
                                currentAssignment.getExplanation()
                            );
                        }
                    }
                }
            } else if (MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME.equals(currentTask.getTaskName())) {
                String id = ((StartDataFrameAnalyticsAction.TaskParams) currentTask.getParams()).getId();
                if (isTaskAssigned) {
                    if (anomalyDetectionAuditor.includeNodeInfo()) {
                        String nodeName = nodeName(currentNodes, currentAssignment.getExecutorNode());
                        dataFrameAnalyticsAuditor.info(id, "Starting analytics on node [" + nodeName + "]");
                    } else {
                        dataFrameAnalyticsAuditor.info(id, "Starting analytics.");
                    }
                } else if (alwaysAuditUnassigned) {
                    if (anomalyDetectionAuditor.includeNodeInfo()) {
                        dataFrameAnalyticsAuditor.warning(
                            id,
                            "No node found to start analytics. Reasons [" + currentAssignment.getExplanation() + "]"
                        );
                    } else {
                        dataFrameAnalyticsAuditor.warning(id, "Awaiting capacity to start analytics.");
                    }
                } else if (wasTaskAssigned) {
                    if (anomalyDetectionAuditor.includeNodeInfo()) {
                        String nodeName = nodeName(previousNodes, previousAssignment.getExecutorNode());
                        anomalyDetectionAuditor.info(id, "Analytics unassigned from node [" + nodeName + "]");
                    } else {
                        anomalyDetectionAuditor.info(id, "Analytics relocating.");
                    }
                }
            }
        }
    }

    static String nodeName(DiscoveryNodes nodes, String nodeId) {
        // It's possible that we're reporting on a node that left the
        // cluster in an earlier cluster state update, in which case
        // the cluster state we've got doesn't record its friendly
        // name. In this case we have no choice but to use the ID. (We
        // also use the ID in tests that don't bother to name nodes.)
        DiscoveryNode node = nodes.get(nodeId);
        if (node != null && Strings.hasLength(node.getName())) {
            return node.getName();
        }
        return nodeId;
    }

    private void logLongTimeUnassigned(Instant now, ClusterState state) {
        logStartedDatafeedsWithoutTask(now, state);
        state.forEachProject(project -> {
            PersistentTasksCustomMetadata tasks = project.metadata().custom(PersistentTasksCustomMetadata.TYPE);
            if (tasks == null) {
                return;
            }

            List<String> itemsToReport = findLongTimeUnassignedTasks(now, tasks);
            if (itemsToReport.isEmpty() == false) {
                logger.warn(
                    "In project [{}] ML persistent tasks unassigned for a long time [{}]",
                    project.projectId(),
                    String.join("|", itemsToReport)
                );
            }
        });
    }

    private void logStartedDatafeedsWithoutTask(Instant now, ClusterState state) {
        try {
            for (StuckDatafeed stuck : findStartedDatafeedsWithoutTask(now, state)) {
                invariantLogger.violated(
                    logger,
                    MlInvariantLogger.Kind.STARTED_DATAFEED_WITHOUT_TASK,
                    stuck.datafeedId(),
                    "Datafeed ["
                        + stuck.datafeedId()
                        + "] has been started without a running task for ["
                        + stuck.minutesWithoutTask()
                        + "] minutes although job ["
                        + stuck.jobId()
                        + "] is open"
                );
            }
        } catch (RuntimeException e) {
            // The watchdog must never keep the unassigned job and analytics reporting that follows from running.
            logger.debug("Failed to check for started datafeeds without a task", e);
        }
    }

    /**
     * Creates a list of items to be logged to report ML job tasks that:
     * 1. Have been unassigned for a long time
     * 2. Have not been logged recently (to avoid log spam)
     * <p>
     * Only report on jobs, not datafeeds, on the assumption that jobs and their corresponding
     * datafeeds get assigned together. This may miss some obscure edge cases, but will avoid
     * the verbose and confusing messages that the duplication between jobs and datafeeds would
     * generally cause.
     * <p>
     * The time intervals used in this reporting reset each time the master node changes, as
     * the data structure used to record the information is in memory on the current master node,
     * not in cluster state.
     */
    synchronized List<String> findLongTimeUnassignedTasks(Instant now, PersistentTasksCustomMetadata tasks) {

        assert tasks != null;

        final List<String> itemsToReport = new ArrayList<>();
        final Map<TaskNameAndId, UnassignedTimeAndReportTime> oldUnassignedInfoByTask = unassignedInfoByTask;
        final Map<TaskNameAndId, UnassignedTimeAndReportTime> newUnassignedInfoByTask = new HashMap<>();

        for (PersistentTask<?> task : tasks.tasks()) {
            if (task.getExecutorNode() == null) {
                final String taskName = task.getTaskName();
                if (MlTasks.JOB_TASK_NAME.equals(taskName) || MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME.equals(taskName)) {
                    // Ignore failed tasks - they don't need to be assigned to a node
                    if (task.getState() == null || ((MlTaskState) task.getState()).isFailed()) {
                        continue;
                    }
                    final String mlId = ((MlTaskParams) task.getParams()).getMlId();
                    final TaskNameAndId key = new TaskNameAndId(taskName, mlId);
                    final UnassignedTimeAndReportTime previousInfo = oldUnassignedInfoByTask.get(key);
                    final Instant firstUnassignedTime;
                    final Instant lastReportedTime;
                    if (previousInfo != null) {
                        firstUnassignedTime = previousInfo.unassignedTime();
                        if (firstUnassignedTime.plus(LONG_TIME_UNASSIGNED_INTERVAL).isBefore(now)
                            && (previousInfo.reportTime() == null || previousInfo.reportTime().plus(MIN_REPORT_INTERVAL).isBefore(now))) {
                            lastReportedTime = now;
                            itemsToReport.add(
                                Strings.format(
                                    "[%s]/[%s] unassigned for [%d] seconds",
                                    taskName,
                                    mlId,
                                    ChronoUnit.SECONDS.between(firstUnassignedTime, now)
                                )
                            );
                        } else {
                            lastReportedTime = previousInfo.reportTime();
                        }
                    } else {
                        firstUnassignedTime = now;
                        lastReportedTime = null;
                    }
                    newUnassignedInfoByTask.put(key, new UnassignedTimeAndReportTime(firstUnassignedTime, lastReportedTime));
                }
            }
        }

        unassignedInfoByTask = newUnassignedInfoByTask;

        return itemsToReport;
    }

    record StuckDatafeed(String datafeedId, String jobId, long minutesWithoutTask) {}

    /**
     * Returns the started datafeeds that have had no live task for over {@link #LONG_TIME_UNASSIGNED_INTERVAL} and have not been
     * reported within {@link #MIN_REPORT_INTERVAL}, although their job is open on a live node and the node selector would place them.
     */
    synchronized List<StuckDatafeed> findStartedDatafeedsWithoutTask(Instant now, ClusterState state) {
        // DatafeedNodeSelector reads the single project of the cluster state, so multi-project states are not checked.
        if (state.metadata().projects().size() != 1
            || state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
            || persistentTaskAssignmentEnabled(state) == false) {
            datafeedWithoutTaskInfoById = Map.of();
            return List.of();
        }
        final PersistentTasksCustomMetadata tasks = state.getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks == null) {
            datafeedWithoutTaskInfoById = Map.of();
            return List.of();
        }

        final Set<String> shuttingDownNodes = state.metadata().nodeShutdowns().getAllNodeIds();
        final List<StuckDatafeed> stuckDatafeeds = new ArrayList<>();
        final Map<String, UnassignedTimeAndReportTime> oldInfoById = datafeedWithoutTaskInfoById;
        final Map<String, UnassignedTimeAndReportTime> newInfoById = new HashMap<>();

        for (PersistentTask<?> task : tasks.tasks()) {
            if (MlTasks.DATAFEED_TASK_NAME.equals(task.getTaskName()) == false) {
                continue;
            }
            final StartDatafeedAction.DatafeedParams params = (StartDatafeedAction.DatafeedParams) task.getParams();
            if (isStartedWithoutLiveTask(task, state.nodes()) == false) {
                continue;
            }
            final DiscoveryNode jobNode = openJobNode(params.getJobId(), tasks, state.nodes(), shuttingDownNodes);
            if (jobNode == null || nodeSelectorWouldPlaceDatafeed(params, jobNode, state) == false) {
                continue;
            }

            final String datafeedId = params.getDatafeedId();
            final UnassignedTimeAndReportTime previousInfo = oldInfoById.get(datafeedId);
            final Instant firstSeenTime = previousInfo != null ? previousInfo.unassignedTime() : now;
            Instant lastReportedTime = previousInfo != null ? previousInfo.reportTime() : null;
            if (firstSeenTime.plus(LONG_TIME_UNASSIGNED_INTERVAL).isBefore(now)
                && (lastReportedTime == null || lastReportedTime.plus(MIN_REPORT_INTERVAL).isBefore(now))) {
                lastReportedTime = now;
                stuckDatafeeds.add(new StuckDatafeed(datafeedId, params.getJobId(), ChronoUnit.MINUTES.between(firstSeenTime, now)));
            }
            newInfoById.put(datafeedId, new UnassignedTimeAndReportTime(firstSeenTime, lastReportedTime));
        }

        datafeedWithoutTaskInfoById = newInfoById;
        return stuckDatafeeds;
    }

    /** Returns the live, non-shutting-down node on which the job is opened, or null if the job is not opened on one. */
    private static DiscoveryNode openJobNode(
        String jobId,
        PersistentTasksCustomMetadata tasks,
        DiscoveryNodes nodes,
        Set<String> shuttingDownNodes
    ) {
        if (jobId == null) {
            return null;
        }
        final PersistentTask<?> jobTask = MlTasks.getJobTask(jobId, tasks);
        if (jobTask == null || MlTasks.getJobStateModifiedForReassignments(jobTask) != JobState.OPENED) {
            return null;
        }
        final String jobNodeId = jobTask.getExecutorNode();
        if (jobNodeId == null || shuttingDownNodes.contains(jobNodeId)) {
            return null;
        }
        return nodes.get(jobNodeId);
    }

    private static boolean isStartedWithoutLiveTask(PersistentTask<?> datafeedTask, DiscoveryNodes nodes) {
        final DatafeedState datafeedState = MlTasks.getDatafeedState(datafeedTask);
        if (datafeedState != DatafeedState.STARTED && datafeedState != DatafeedState.STARTING) {
            return false;
        }
        return datafeedTask.getExecutorNode() == null || nodes.nodeExists(datafeedTask.getExecutorNode()) == false;
    }

    /** Mirrors {@code EnableAssignmentDecider}: cluster settings in the state override node settings. */
    private boolean persistentTaskAssignmentEnabled(ClusterState state) {
        final Settings effective = Settings.builder().put(clusterService.getSettings()).put(state.metadata().settings()).build();
        return EnableAssignmentDecider.CLUSTER_TASKS_ALLOCATION_ENABLE_SETTING.get(effective) != EnableAssignmentDecider.Allocation.NONE;
    }

    /** Index availability, upgrade and reset mode legitimately keep a datafeed unassigned; only an assignable datafeed is suspect. */
    private boolean nodeSelectorWouldPlaceDatafeed(StartDatafeedAction.DatafeedParams params, DiscoveryNode jobNode, ClusterState state) {
        return new DatafeedNodeSelector(
            state,
            resolver,
            params.getDatafeedId(),
            params.getJobId(),
            params.getDatafeedIndices(),
            params.getIndicesOptions()
        ).selectNode(List.of(jobNode)).getExecutorNode() != null;
    }

    private record TaskNameAndId(String taskName, String mlId) {};

    private record UnassignedTimeAndReportTime(Instant unassignedTime, Instant reportTime) {};
}
