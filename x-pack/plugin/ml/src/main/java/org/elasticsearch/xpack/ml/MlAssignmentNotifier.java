/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

import java.util.Objects;


public class MlAssignmentNotifier implements ClusterStateListener {
    private static final Logger logger = LogManager.getLogger(MlAssignmentNotifier.class);

    private final AnomalyDetectionAuditor auditor;
    private final MlConfigMigrator mlConfigMigrator;
    private final ThreadPool threadPool;

    MlAssignmentNotifier(Settings settings, AnomalyDetectionAuditor auditor, ThreadPool threadPool, Client client,
                         ClusterService clusterService) {
        this.auditor = auditor;
        this.mlConfigMigrator = new MlConfigMigrator(settings, client, clusterService);
        this.threadPool = threadPool;
        clusterService.addListener(this);
    }

    MlAssignmentNotifier(AnomalyDetectionAuditor auditor, ThreadPool threadPool, MlConfigMigrator mlConfigMigrator,
                         ClusterService clusterService) {
        this.auditor = auditor;
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

        PersistentTasksCustomMetaData previous = event.previousState().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData current = event.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);

        if (Objects.equals(previous, current)) {
            return;
        }

        for (PersistentTask<?> currentTask : current.tasks()) {
            Assignment currentAssignment = currentTask.getAssignment();
            PersistentTask<?> previousTask = previous != null ? previous.getTask(currentTask.getId()) : null;
            Assignment previousAssignment = previousTask != null ? previousTask.getAssignment() : null;
            if (Objects.equals(currentAssignment, previousAssignment)) {
                continue;
            }
            if (MlTasks.JOB_TASK_NAME.equals(currentTask.getTaskName())) {
                String jobId = ((OpenJobAction.JobParams) currentTask.getParams()).getJobId();
                if (currentAssignment.getExecutorNode() == null) {
                    auditor.warning(jobId, "No node found to open job. Reasons [" + currentAssignment.getExplanation() + "]");
                } else {
                    DiscoveryNode node = event.state().nodes().get(currentAssignment.getExecutorNode());
                    auditor.info(jobId, "Opening job on node [" + node.toString() + "]");
                }
            } else if (MlTasks.DATAFEED_TASK_NAME.equals(currentTask.getTaskName())) {
                StartDatafeedAction.DatafeedParams datafeedParams = (StartDatafeedAction.DatafeedParams) currentTask.getParams();
                String jobId = datafeedParams.getJobId();
                if (currentAssignment.getExecutorNode() == null) {
                    String msg = "No node found to start datafeed [" + datafeedParams.getDatafeedId() +"]. Reasons [" +
                            currentAssignment.getExplanation() + "]";
                    logger.warn("[{}] {}", jobId, msg);
                    if (jobId != null) {
                        auditor.warning(jobId, msg);
                    }
                } else {
                    DiscoveryNode node = event.state().nodes().get(currentAssignment.getExecutorNode());
                    if (jobId != null) {
                        auditor.info(jobId, "Starting datafeed [" + datafeedParams.getDatafeedId() + "] on node [" + node + "]");
                    }
                }
            }
        }
    }
}
