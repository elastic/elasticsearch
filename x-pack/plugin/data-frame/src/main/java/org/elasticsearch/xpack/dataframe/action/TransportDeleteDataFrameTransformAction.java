/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.dataframe.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction.Request;
import org.elasticsearch.xpack.core.dataframe.action.DeleteDataFrameTransformAction.Response;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.List;

public class TransportDeleteDataFrameTransformAction extends TransportTasksAction<DataFrameTransformTask, Request, Response, Response> {

    private final DataFrameTransformsConfigManager transformsConfigManager;

    @Inject
    public TransportDeleteDataFrameTransformAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService, DataFrameTransformsConfigManager transformsConfigManager) {
        super(DeleteDataFrameTransformAction.NAME, clusterService, transportService, actionFilters, Request::new, Response::new,
                Response::new, ThreadPool.Names.SAME);
        this.transformsConfigManager = transformsConfigManager;
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        assert tasks.size() + taskOperationFailures.size() == 1;
        boolean cancelled = tasks.size() > 0 && tasks.stream().allMatch(Response::isDeleted);

        return new Response(cancelled, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask task, ActionListener<Response> listener) {
        assert task.getTransformId().equals(request.getId());
        IndexerState state = task.getState().getIndexerState();
        if (state.equals(IndexerState.STOPPED)) {
            task.onCancelled();
            transformsConfigManager.deleteTransform(request.getId(), ActionListener.wrap(r -> {
                listener.onResponse(new Response(true));
            }, listener::onFailure));
        } else {
            listener.onFailure(new IllegalStateException("Could not delete transform [" + request.getId() + "] because "
                    + "indexer state is [" + state + "].  Transform must be [" + IndexerState.STOPPED + "] before deletion."));
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
                // we couldn't find the transform in the persistent task CS, but maybe the transform exists in the configuration index,
                // if so delete the orphaned document and do not throw (for the normal case we want to stop the task first,
                // than delete the configuration document if and only if the data frame transform is in stopped state)
                transformsConfigManager.deleteTransform(request.getId(), ActionListener.wrap(r -> {
                    listener.onResponse(new Response(true));
                    return;
                }, listener::onFailure));
            }
        } else {
            // Delegates DeleteTransform to elected master node, so it becomes the coordinating node.
            // Non-master nodes may have a stale cluster state that shows transforms which are cancelled
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
