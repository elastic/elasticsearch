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
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.discovery.MasterNotDiscoveredException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.dataframe.action.GetDataFrameTransformsAction.Request;
import org.elasticsearch.xpack.dataframe.action.GetDataFrameTransformsAction.Response;
import org.elasticsearch.xpack.dataframe.persistence.DataFramePersistentTaskUtils;
import org.elasticsearch.xpack.dataframe.persistence.DataFrameTransformsConfigManager;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformConfig;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TransportGetDataFrameTransformsAction extends
        TransportTasksAction<DataFrameTransformTask,
        GetDataFrameTransformsAction.Request,
        GetDataFrameTransformsAction.Response,
        GetDataFrameTransformsAction.Response> {

    private final DataFrameTransformsConfigManager transformsConfigManager;

    @Inject
    public TransportGetDataFrameTransformsAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService, DataFrameTransformsConfigManager transformsConfigManager) {
        super(GetDataFrameTransformsAction.NAME, clusterService, transportService, actionFilters, GetDataFrameTransformsAction.Request::new,
                GetDataFrameTransformsAction.Response::new, GetDataFrameTransformsAction.Response::new, ThreadPool.Names.SAME);
        this.transformsConfigManager = transformsConfigManager;
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        List<DataFrameTransformConfig> configs = tasks.stream().map(GetDataFrameTransformsAction.Response::getTransformConfigurations)
                .flatMap(Collection::stream).collect(Collectors.toList());
        return new Response(configs, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask task, ActionListener<Response> listener) {
        assert task.getTransformId().equals(request.getId()) || request.getId().equals(MetaData.ALL);
        // Little extra insurance, make sure we only return transforms that aren't cancelled
        if (task.isCancelled() == false) {
            transformsConfigManager.getTransformConfiguration(task.getTransformId(), ActionListener.wrap(config -> {
                listener.onResponse(new Response(Collections.singletonList(config)));
            }, e -> {
                listener.onFailure(new RuntimeException("failed to retrieve...", e));
            }));
        } else {
            listener.onResponse(new Response(Collections.emptyList()));
        }
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        final ClusterState state = clusterService.state();
        final DiscoveryNodes nodes = state.nodes();

        if (nodes.isLocalNodeElectedMaster()) {
            if (DataFramePersistentTaskUtils.stateHasDataFrameTransforms(request.getId(), state)) {
                super.doExecute(task, request, listener);
            } else {
                // If we couldn't find the transform in the persistent task CS, it means it was deleted prior to this GET
                // and we can just send an empty response, no need to go looking for the allocated task
                listener.onResponse(new Response(Collections.emptyList()));
            }

        } else {
            // Delegates GetTransforms to elected master node, so it becomes the coordinating node.
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
