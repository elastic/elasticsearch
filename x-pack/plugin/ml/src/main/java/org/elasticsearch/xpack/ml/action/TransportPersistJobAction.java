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
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.job.process.autodetect.AutodetectProcessManager;

public class TransportPersistJobAction extends TransportJobTaskAction<PersistJobAction.Request, PersistJobAction.Response> {

    @Inject
    public TransportPersistJobAction(TransportService transportService, ClusterService clusterService, ActionFilters actionFilters,
                                     AutodetectProcessManager processManager) {
        super(PersistJobAction.NAME, clusterService, transportService, actionFilters,
            PersistJobAction.Request::new, PersistJobAction.Response::new, ThreadPool.Names.SAME, processManager);
        // ThreadPool.Names.SAME, because operations is executed by autodetect worker thread
    }

    @Override
    protected void taskOperation(PersistJobAction.Request request, TransportOpenJobAction.JobTask task,
                                 ActionListener<PersistJobAction.Response> listener) {

        processManager.persistJob(task, e -> {
            if (e == null) {
                listener.onResponse(new PersistJobAction.Response(true));
            } else {
                listener.onFailure(e);
            }
        });
    }

    @Override
    protected void doExecute(Task task, PersistJobAction.Request request, ActionListener<PersistJobAction.Response> listener) {
        // TODO Remove this overridden method in 7.0.0
        DiscoveryNodes nodes = clusterService.state().nodes();
        PersistentTasksCustomMetaData tasks = clusterService.state().getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> jobTask = MlTasks.getJobTask(request.getJobId(), tasks);
        if (jobTask == null || jobTask.getExecutorNode() == null) {
            logger.debug("[{}] Cannot persist the job because the job is not open", request.getJobId());
            listener.onResponse(new PersistJobAction.Response(false));
            return;
        }

        DiscoveryNode executorNode = nodes.get(jobTask.getExecutorNode());
        if (executorNode == null) {
            listener.onFailure(ExceptionsHelper.conflictStatusException("Cannot persist job [{}] as" +
                    "executor node [{}] cannot be found", request.getJobId(), jobTask.getExecutorNode()));
            return;
        }

        Version nodeVersion = executorNode.getVersion();
        if (nodeVersion.before(Version.V_6_3_0)) {
            listener.onFailure(
                    new ElasticsearchException("Cannot persist job [" + request.getJobId() + "] on node with version " + nodeVersion));
            return;
        }

        super.doExecute(task, request, listener);
    }
}
