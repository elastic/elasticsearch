/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.LocalNodeMasterListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.Assignment;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData.PersistentTask;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public class MlAssignmentNotifier implements ClusterStateListener, LocalNodeMasterListener {

    private static final Logger logger = LogManager.getLogger(MlAssignmentNotifier.class);

    private final Auditor auditor;
    private final ClusterService clusterService;

    private final AtomicBoolean enabled = new AtomicBoolean(false);

    MlAssignmentNotifier(Auditor auditor, ClusterService clusterService) {
        this.auditor = auditor;
        this.clusterService = clusterService;
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

        for (PersistentTask<?> currentTask : current.tasks()) {
            Assignment currentAssignment = currentTask.getAssignment();
            PersistentTask<?> previousTask = previous != null ? previous.getTask(currentTask.getId()) : null;
            Assignment previousAssignment = previousTask != null ? previousTask.getAssignment() : null;
            if (Objects.equals(currentAssignment, previousAssignment)) {
                continue;
            }
            if (OpenJobAction.TASK_NAME.equals(currentTask.getTaskName())) {
                String jobId = ((OpenJobAction.JobParams) currentTask.getParams()).getJobId();
                if (currentAssignment.getExecutorNode() == null) {
                    auditor.warning(jobId, "No node found to open job. Reasons [" + currentAssignment.getExplanation() + "]");
                } else {
                    DiscoveryNode node = event.state().nodes().get(currentAssignment.getExecutorNode());
                    auditor.info(jobId, "Opening job on node [" + node.toString() + "]");
                }
            } else if (StartDatafeedAction.TASK_NAME.equals(currentTask.getTaskName())) {
                String datafeedId = ((StartDatafeedAction.DatafeedParams) currentTask.getParams()).getDatafeedId();
                DatafeedConfig datafeedConfig = MlMetadata.getMlMetadata(event.state()).getDatafeed(datafeedId);
                if (currentAssignment.getExecutorNode() == null) {
                    String msg = "No node found to start datafeed [" + datafeedId +"]. Reasons [" +
                            currentAssignment.getExplanation() + "]";
                    logger.warn("[{}] {}", datafeedConfig.getJobId(), msg);
                    auditor.warning(datafeedConfig.getJobId(), msg);
                } else {
                    DiscoveryNode node = event.state().nodes().get(currentAssignment.getExecutorNode());
                    auditor.info(datafeedConfig.getJobId(), "Starting datafeed [" + datafeedId + "] on node [" + node + "]");
                }
            }
        }
    }
}
