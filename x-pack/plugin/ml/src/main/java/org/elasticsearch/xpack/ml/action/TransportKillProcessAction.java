/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.AnomalyDetectionAuditor;

public class TransportKillProcessAction extends TransportJobTaskAction<KillProcessAction.Request, KillProcessAction.Response> {

    private static final Logger logger = LogManager.getLogger(TransportKillProcessAction.class);

    private final AnomalyDetectionAuditor auditor;

    @Inject
    public TransportKillProcessAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                      AutodetectProcessManager processManager, AnomalyDetectionAuditor auditor) {
        super(KillProcessAction.NAME, clusterService, transportService, actionFilters, KillProcessAction.Request::new,
            KillProcessAction.Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME, processManager);
        this.auditor = auditor;
    }

    @Override
    protected void taskOperation(KillProcessAction.Request request, TransportOpenJobAction.JobTask jobTask,
                                 ActionListener<KillProcessAction.Response> listener) {
        logger.info("[{}] Killing job", jobTask.getJobId());
        auditor.info(jobTask.getJobId(), Messages.JOB_AUDIT_KILLING);

        try {
            processManager.killProcess(jobTask, true, null);
            listener.onResponse(new KillProcessAction.Response(true));
        } catch (Exception e) {
            listener.onFailure(e);
        }
    }

    @Override
    protected void doExecute(Task task, KillProcessAction.Request request, ActionListener<KillProcessAction.Response> listener) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlTasks.getJobTask(request.getJobId(), tasks);
        if (jobTask == null || jobTask.getExecutorNode() == null) {
            logger.debug("[{}] Cannot kill the process because job is not open", request.getJobId());
            listener.onResponse(new KillProcessAction.Response(false));
            return;
        }

        DiscoveryNode executorNode = nodes.get(jobTask.getExecutorNode());
        if (executorNode == null) {
            listener.onFailure(ExceptionsHelper.conflictStatusException("Cannot kill process for job {} as" +
                    "executor node {} cannot be found", request.getJobId(), jobTask.getExecutorNode()));
            return;
        }

        super.doExecute(task, request, listener);
    }

}
