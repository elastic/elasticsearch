/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
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
    private final MlConfigMigrator mlConfigMigrator;
    private final ThreadPool threadPool;

    MlAssignmentNotifier(AnomalyDetectionAuditor anomalyDetectionAuditor, DataFrameAnalyticsAuditor dataFrameAnalyticsAuditor,
                         ThreadPool threadPool, MlConfigMigrator mlConfigMigrator, ClusterService clusterService) {
        this.anomalyDetectionAuditor = anomalyDetectionAuditor;
        this.dataFrameAnalyticsAuditor = dataFrameAnalyticsAuditor;
        this.mlConfigMigrator = mlConfigMigrator;
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

        mlConfigMigrator.migrateConfigs(event.state(), ActionListener.wrap(
                response -> threadPool.executor(executorName()).execute(() -> auditChangesToMlTasks(event)),
                e -> {
                    logger.error("error migrating ml configurations", e);
                    threadPool.executor(executorName()).execute(() -> auditChangesToMlTasks(event));
                }
        ));
    }

    private void auditChangesToMlTasks(ClusterChangedEvent event) {

        if (event.metaDataChanged() == false) {
            return;
        }

        PersistentTasksCustomMetaData previousTasks = event.previousState().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData currentTasks = event.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        if (Objects.equals(previousTasks, currentTasks)) {
            return;
        }

        auditMlTasks(event.state().nodes(), previousTasks, currentTasks, false);
    }

    /**
     * Creates an audit warning for all currently unassigned ML
     * tasks, even if a previous audit warning has been created.
     * Care must be taken not to call this method frequently.
     */
    public void auditUnassignedMlTasks(DiscoveryNodes nodes, PersistentTasksCustomMetaData tasks) {
        auditMlTasks(nodes, tasks, tasks, true);
    }

    private void auditMlTasks(DiscoveryNodes nodes, PersistentTasksCustomMetaData previousTasks, PersistentTasksCustomMetaData currentTasks,
                              boolean alwaysAuditUnassigned) {

        for (PersistentTask<?> currentTask : currentTasks.tasks()) {
            Assignment currentAssignment = currentTask.getAssignment();
            PersistentTask<?> previousTask = previousTasks != null ? previousTasks.getTask(currentTask.getId()) : null;
            Assignment previousAssignment = previousTask != null ? previousTask.getAssignment() : null;

            boolean isTaskAssigned = (currentAssignment.getExecutorNode() != null);
            if (Objects.equals(currentAssignment, previousAssignment) &&
                (isTaskAssigned || alwaysAuditUnassigned == false)) {
                continue;
            }

            if (MlTasks.JOB_TASK_NAME.equals(currentTask.getTaskName())) {
                String jobId = ((OpenJobAction.JobParams) currentTask.getParams()).getJobId();
                if (isTaskAssigned) {
                    DiscoveryNode node = nodes.get(currentAssignment.getExecutorNode());
                    anomalyDetectionAuditor.info(jobId, "Opening job on node [" + node.toString() + "]");
                } else {
                    anomalyDetectionAuditor.warning(jobId,
                        "No node found to open job. Reasons [" + currentAssignment.getExplanation() + "]");
                }
            } else if (MlTasks.DATAFEED_TASK_NAME.equals(currentTask.getTaskName())) {
                StartDatafeedAction.DatafeedParams datafeedParams = (StartDatafeedAction.DatafeedParams) currentTask.getParams();
                String jobId = datafeedParams.getJobId();
                if (isTaskAssigned) {
                    DiscoveryNode node = nodes.get(currentAssignment.getExecutorNode());
                    if (jobId != null) {
                        anomalyDetectionAuditor.info(jobId,
                            "Starting datafeed [" + datafeedParams.getDatafeedId() + "] on node [" + node + "]");
                    }
                } else {
                    String msg = "No node found to start datafeed [" + datafeedParams.getDatafeedId() +"]. Reasons [" +
                        currentAssignment.getExplanation() + "]";
                    if (alwaysAuditUnassigned == false) {
                        logger.warn("[{}] {}", jobId, msg);
                    }
                    if (jobId != null) {
                        anomalyDetectionAuditor.warning(jobId, msg);
                    }
                }
            } else if (MlTasks.DATA_FRAME_ANALYTICS_TASK_NAME.equals(currentTask.getTaskName())) {
                String id = ((StartDataFrameAnalyticsAction.TaskParams) currentTask.getParams()).getId();
                if (isTaskAssigned) {
                    DiscoveryNode node = nodes.get(currentAssignment.getExecutorNode());
                    dataFrameAnalyticsAuditor.info(id, "Starting analytics on node [" + node.toString() + "]");
                } else {
                    dataFrameAnalyticsAuditor.warning(id,
                        "No node found to start analytics. Reasons [" + currentAssignment.getExplanation() + "]");
                }
            }
        }
    }
}
