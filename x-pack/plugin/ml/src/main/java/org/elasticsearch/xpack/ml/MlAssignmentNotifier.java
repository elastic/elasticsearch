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
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.FixForMultiProject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.utils.MlTaskParams;
import org.elasticsearch.xpack.core.ml.utils.MlTaskState;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class MlAssignmentNotifier implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MlAssignmentNotifier.class);

    static final Duration MIN_CHECK_UNASSIGNED_INTERVAL = Duration.ofSeconds(30);
    static final Duration LONG_TIME_UNASSIGNED_INTERVAL = Duration.ofMinutes(15);
    static final Duration MIN_REPORT_INTERVAL = Duration.ofHours(6);

    private final AnomalyDetectionAuditor anomalyDetectionAuditor;
    private final DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor;
    private final ThreadPool threadPool;
    private final Clock clock;
    private Map<TaskNameAndId, UnassignedTimeAndReportTime> unassignedInfoByTask = Map.of();
    private volatile Instant lastLogCheck;

    MlAssignmentNotifier(
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this(anomalyDetectionAuditor, dataFrameAnalyticsAuditor, threadPool, clusterService, Clock.systemUTC());
    }

    MlAssignmentNotifier(
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor,
        ThreadPool threadPool,
        ClusterService clusterService,
        Clock clock
    ) {
        this.anomalyDetectionAuditor = anomalyDetectionAuditor;
        this.dataFrameAnalyticsAuditor = dataFrameAnalyticsAuditor;
        this.threadPool = threadPool;
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

    private record TaskNameAndId(String taskName, String mlId) {};

    private record UnassignedTimeAndReportTime(Instant unassignedTime, Instant reportTime) {};
}
