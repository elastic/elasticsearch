/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.KillProcessAction;
import org.elasticsearch.xpack.core.ml.job.messages.Messages;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.ml.MachineLearning;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.notifications.Auditor;

import java.io.IOException;

public class TransportKillProcessAction extends TransportJobTaskAction<KillProcessAction.Request, KillProcessAction.Response> {

    private final Auditor auditor;

    @Inject
    public TransportKillProcessAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                      ClusterService clusterService, ActionFilters actionFilters,
                                      IndexNameExpressionResolver indexNameExpressionResolver,
                                      AutodetectProcessManager processManager, Auditor auditor) {
        super(settings, KillProcessAction.NAME, threadPool, clusterService, transportService, actionFilters, indexNameExpressionResolver,
                KillProcessAction.Request::new, KillProcessAction.Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME, processManager);
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

        Version nodeVersion = executorNode.getVersion();
        if (nodeVersion.before(Version.V_5_5_0)) {
            listener.onFailure(new ElasticsearchException("Cannot kill the process on node with version " + nodeVersion));
            return;
        }

        super.doExecute(task, request, listener);
    }


    @Override
    protected KillProcessAction.Response readTaskResponse(StreamInput in) throws IOException {
        return new KillProcessAction.Response(in);
    }
}
