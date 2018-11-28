/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class MlAssignmentNotifier implements ClusterStateListener, LocalNodeMasterListener {
    private static final Logger logger = LogManager.getLogger(MlAssignmentNotifier.class);

    private final Auditor auditor;
    private final ClusterService clusterService;
    private final MlConfigMigrator mlConfigMigrator;
    private final ThreadPool threadPool;
    private final AtomicBoolean enabled = new AtomicBoolean(false);

    MlAssignmentNotifier(Auditor auditor, ThreadPool threadPool, Client client, ClusterService clusterService) {
        this.auditor = auditor;
        this.clusterService = clusterService;
        this.mlConfigMigrator = new MlConfigMigrator(client, clusterService);
        this.threadPool = threadPool;
        clusterService.addLocalNodeMasterListener(this);
    }

    MlAssignmentNotifier(Auditor auditor, ThreadPool threadPool, MlConfigMigrator mlConfigMigrator, ClusterService clusterService) {
        this.auditor = auditor;
        this.clusterService = clusterService;
        this.mlConfigMigrator = mlConfigMigrator;
        this.threadPool = threadPool;
        clusterService.addLocalNodeMasterListener(this);
    }

    @Override
    public void onMaster() {
        if (enabled.compareAndSet(false, true)) {
            clusterService.addListener(this);
        }
    }

    @Override
    public void offMaster() {
        if (enabled.compareAndSet(true, false)) {
            clusterService.removeListener(this);
        }
    }

    @Override
    public String executorName() {
        return ThreadPool.Names.GENERIC;
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (enabled.get() == false) {
            return;
        }
        if (event.metaDataChanged() == false) {
            return;
        }
        PersistentTasksCustomMetaData previous = event.previousState().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData current = event.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        if (Objects.equals(previous, current)) {
            return;
        }

        Version minNodeVersion = event.state().nodes().getMinNodeVersion();
        if (minNodeVersion.onOrAfter(Version.V_6_6_0)) {
            // ok to migrate
            mlConfigMigrator.migrateConfigsWithoutTasks(event.state(), ActionListener.wrap(
                    response -> threadPool.executor(executorName()).execute(() -> auditChangesToMlTasks(current, previous, event.state())),
                    e -> logger.error("error migrating ml configurations", e)
            ));
        } else {
            threadPool.executor(executorName()).execute(() -> auditChangesToMlTasks(current, previous, event.state()));
        }

    }

    private void auditChangesToMlTasks(PersistentTasksCustomMetaData current, PersistentTasksCustomMetaData previous,
                                       ClusterState state) {

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
                    DiscoveryNode node = state.nodes().get(currentAssignment.getExecutorNode());
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
                    DiscoveryNode node = state.nodes().get(currentAssignment.getExecutorNode());
                    if (jobId != null) {
                        auditor.info(jobId, "Starting datafeed [" + datafeedParams.getDatafeedId() + "] on node [" + node + "]");
                    }
                }
            }
        }
    }
}
