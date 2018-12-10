/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.dataframe.action.DeleteDataFrameJobAction.Request;
import org.elasticsearch.xpack.dataframe.action.DeleteDataFrameJobAction.Response;
import org.elasticsearch.xpack.dataframe.job.DataFrameJobTask;

import java.util.List;

public class TransportDeleteDataFrameJobAction extends TransportTasksAction<DataFrameJobTask, Request, Response, Response> {

    @Inject
    public TransportDeleteDataFrameJobAction(TransportService transportService, ThreadPool threadPool, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, PersistentTasksService persistentTasksService,
            ClusterService clusterService) {
        super(DeleteDataFrameJobAction.NAME, clusterService, transportService, actionFilters, Request::new, Response::new, Response::new,
                ThreadPool.Names.SAME);
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        assert tasks.size() + taskOperationFailures.size() == 1;
        boolean cancelled = tasks.size() > 0 && tasks.stream().allMatch(Response::isDeleted);

        return new Response(cancelled, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameJobTask task, ActionListener<Response> listener) {
        assert task.getConfig().getId().equals(request.getId());
        IndexerState state = task.getState().getJobState();
        if (state.equals(IndexerState.STOPPED)) {
            task.onCancelled();
            listener.onResponse(new Response(true));
        } else {
            listener.onFailure(new IllegalStateException("Could not delete job [" + request.getId() + "] because " + "indexer state is ["
                    + state + "].  Job must be [" + IndexerState.STOPPED + "] before deletion."));
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();
        if (nodes.isLocalNodeElectedMaster()) {
            PersistentTasksCustomMetaData pTasksMeta = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
            if (pTasksMeta != null && pTasksMeta.getTask(request.getId()) != null) {
                super.doExecute(task, request, listener);
            } else {
                // If we couldn't find the job in the persistent task CS, it means it was deleted prior to this call,
                // no need to go looking for the allocated task
                listener.onFailure(new ResourceNotFoundException("the task with id [" + request.getId() + "] doesn't exist"));
            }
        } else {
            // Delegates DeleteJob to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows jobs which are cancelled
            // on the master, which makes testing difficult.
            if (nodes.getMasterNode() == null) {
                listener.onFailure(new MasterNotDiscoveredException("no known master nodes"));
            } else {
                transportService.sendRequest(nodes.getMasterNode(), actionName, request,
                        new ActionListenerResponseHandler<>(listener, Response::new));
            }
        }
    }
}
