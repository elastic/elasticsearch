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
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDataFrameAnalyticsAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;
import org.elasticsearch.xpack.ml.notifications.DataFrameAnalyticsAuditor;

import java.util.Objects;

public class MlAssignmentNotifier implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MlAssignmentNotifier.class);

    private final AnomalyDetectionAuditor anomalyDetectionAuditor;
    private final DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor;
    private final ThreadPool threadPool;

    MlAssignmentNotifier(
        AnomalyDetectionAuditor anomalyDetectionAuditor,
        DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor,
        ThreadPool threadPool,
        ClusterService clusterService
    ) {
        this.anomalyDetectionAuditor = anomalyDetectionAuditor;
        this.dataFrameAnalyticsAuditor = dataFrameAnalyticsAuditor;
        this.threadPool = threadPool;
        clusterService.addListener(this);
    }

    private String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {

        if (event.localNodeMaster() == false) {
            return;
        }

        if (event.metadataChanged() == false) {
            return;
        }

        threadPool.executor(executorName()).execute(() -> auditChangesToMlTasks(event));
    }

    private void auditChangesToMlTasks(ClusterChangedEvent event) {

        PersistentTasksCustomMetadata previousTasks = event.previousState().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata currentTasks = event.state().getMetadata().custom(PersistentTasksCustomMetadata.TYPE);

        if (Objects.equals(previousTasks, currentTasks)) {
            return;
        }

        auditMlTasks(event.previousState().nodes(), event.state().nodes(), previousTasks, currentTasks, false);
    }

    /**
     * Creates an audit warning for all currently unassigned ML
     * tasks, even if a previous audit warning has been created.
     * Care must be taken not to call this method frequently.
     */
    public void auditUnassignedMlTasks(DiscoveryNodes nodes, PersistentTasksCustomMetadata tasks) {
        auditMlTasks(nodes, nodes, tasks, tasks, true);
    }

    private void auditMlTasks(
        DiscoveryNodes previousNodes,
        DiscoveryNodes currentNodes,
        PersistentTasksCustomMetadata previousTasks,
        PersistentTasksCustomMetadata currentTasks,
        boolean alwaysAuditUnassigned
    ) {
        if (currentTasks == null) {
            return;
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
                    String nodeName = nodeName(currentNodes, currentAssignment.getExecutorNode());
                    anomalyDetectionAuditor.info(jobId, "Opening job on node [" + nodeName + "]");
                } else if (alwaysAuditUnassigned) {
                    anomalyDetectionAuditor.warning(
                        jobId,
                        "No node found to open job. Reasons [" + currentAssignment.getExplanation() + "]"
                    );
                } else if (wasTaskAssigned) {
                    String nodeName = nodeName(previousNodes, previousAssignment.getExecutorNode());
                    anomalyDetectionAuditor.info(jobId, "Job unassigned from node [" + nodeName + "]");
                }
            } else if (MlTasks.DATAFEED_TASK_NAME.equals(currentTask.getTaskName())) {
                StartDatafeedAction.DatafeedParams datafeedParams = (StartDatafeedAction.DatafeedParams) currentTask.getParams();
                String jobId = datafeedParams.getJobId();
                if (jobId != null) {
                    if (isTaskAssigned) {
                        String nodeName = nodeName(currentNodes, currentAssignment.getExecutorNode());
                        anomalyDetectionAuditor.info(
                            jobId,
                            "Starting datafeed [" + datafeedParams.getDatafeedId() + "] on node [" + nodeName + "]"
                        );
                    } else if (alwaysAuditUnassigned) {
                        anomalyDetectionAuditor.warning(
                            jobId,
                            "No node found to start datafeed ["
                                + datafeedParams.getDatafeedId()
                                + "]. Reasons ["
                                + currentAssignment.getExplanation()
                                + "]"
                        );
                    } else if (wasTaskAssigned) {
                        String nodeName = nodeName(previousNodes, previousAssignment.getExecutorNode());
                        anomalyDetectionAuditor.info(
                            jobId,
                            "Datafeed [" + datafeedParams.getDatafeedId() + "] unassigned from node [" + nodeName + "]"
                        );
                    } else {
                        logger.warn(
                            "[{}] No node found to start datafeed [{}]. Reasons [{}]",
                            jobId,
                            datafeedParams.getDatafeedId(),
                            currentAssignment.getExplanation()
                        );
                    }
                }
            } else if (MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME.equals(currentTask.getTaskName())) {
                String id = ((StartDataFrameAnalyticsAction.TaskParams) currentTask.getParams()).getId();
                if (isTaskAssigned) {
                    String nodeName = nodeName(currentNodes, currentAssignment.getExecutorNode());
                    dataFrameAnalyticsAuditor.info(id, "Starting analytics on node [" + nodeName + "]");
                } else if (alwaysAuditUnassigned) {
                    dataFrameAnalyticsAuditor.warning(
                        id,
                        "No node found to start analytics. Reasons [" + currentAssignment.getExplanation() + "]"
                    );
                } else if (wasTaskAssigned) {
                    String nodeName = nodeName(previousNodes, previousAssignment.getExecutorNode());
                    anomalyDetectionAuditor.info(id, "Analytics unassigned from node [" + nodeName + "]");
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
}
