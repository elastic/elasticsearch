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
import org.elasticsearch.xpack.dataframe.action.GetDataFrameTransformsStatsAction.Request;
import org.elasticsearch.xpack.dataframe.action.GetDataFrameTransformsStatsAction.Response;
import org.elasticsearch.xpack.dataframe.persistence.DataFramePersistentTaskUtils;
import org.elasticsearch.xpack.dataframe.transforms.DataFrameTransformTask;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class TransportGetDataFrameTransformsStatsAction extends
        TransportTasksAction<DataFrameTransformTask,
        GetDataFrameTransformsStatsAction.Request,
        GetDataFrameTransformsStatsAction.Response,
        GetDataFrameTransformsStatsAction.Response> {

    @Inject
    public TransportGetDataFrameTransformsStatsAction(TransportService transportService, ActionFilters actionFilters,
            ClusterService clusterService) {
        super(GetDataFrameTransformsStatsAction.NAME, clusterService, transportService, actionFilters, Request::new, Response::new,
                Response::new, ThreadPool.Names.SAME);
    }

    @Override
    protected Response newResponse(Request request, List<Response> tasks, List<TaskOperationFailure> taskOperationFailures,
            List<FailedNodeException> failedNodeExceptions) {
        List<DataFrameTransformStateAndStats> responses = tasks.stream()
                .map(GetDataFrameTransformsStatsAction.Response::getTransformsStateAndStats).flatMap(Collection::stream)
                .collect(Collectors.toList());
        return new Response(responses, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected void taskOperation(Request request, DataFrameTransformTask task, ActionListener<Response> listener) {
        List<DataFrameTransformStateAndStats> transformsStateAndStats = Collections.emptyList();

        assert task.getTransformId().equals(request.getId()) || request.getId().equals(MetaData.ALL);

        // Little extra insurance, make sure we only return transforms that aren't cancelled
        if (task.isCancelled() == false) {
            DataFrameTransformStateAndStats transformStateAndStats = new DataFrameTransformStateAndStats(task.getTransformId(),
                    task.getState(), task.getStats());
            transformsStateAndStats = Collections.singletonList(transformStateAndStats);
        }

        listener.onResponse(new Response(transformsStateAndStats));
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