/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction.Request;
import org.elasticsearch.xpack.core.ml.action.GetDatafeedRunningStateAction.Response;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;

public class TransportGetDatafeedRunningStateAction extends TransportTasksAction<
    TransportStartDatafeedAction.DatafeedTask,
    Request,
    Response,
    Response> {

    private static final Logger logger = LogManager.getLogger(TransportGetDatafeedRunningStateAction.class);

    @Inject
    public TransportGetDatafeedRunningStateAction(
        TransportService transportService,
        ClusterService clusterService,
        ActionFilters actionFilters
    ) {
        super(
            GetDatafeedRunningStateAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            Request::new,
            Response::new,
            transportService.getThreadPool().executor(ThreadPool.Names.MANAGEMENT)
        );
    }

    @Override
    protected Response newResponse(
        Request request,
        List<Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        org.elasticsearch.ExceptionsHelper.rethrowAndSuppress(
            taskOperationFailures.stream()
                .map(t -> org.elasticsearch.ExceptionsHelper.convertToElastic(t.getCause()))
                .collect(Collectors.toList())
        );
        org.elasticsearch.ExceptionsHelper.rethrowAndSuppress(failedNodeExceptions);
        return Response.fromResponses(tasks);
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        Request request,
        TransportStartDatafeedAction.DatafeedTask datafeedTask,
        ActionListener<Response> listener
    ) {
        listener.onResponse(Response.fromTaskAndState(datafeedTask.getDatafeedId(), datafeedTask.getRunningState()));
    }

    @Override
    protected void doExecute(Task task, Request request, ActionListener<Response> listener) {
        DiscoveryNodes nodes = clusterService.state().nodes();
        PersistentTasksCustomMetadata tasks = clusterService.state().getMetadata().getProject().custom(PersistentTasksCustomMetadata.TYPE);
        if (tasks == null) {
            listener.onResponse(new Response(Collections.emptyMap()));
            return;
        }
        final List<PersistentTasksCustomMetadata.PersistentTask<?>> datafeedTasks = request.getDatafeedTaskIds()
            .stream()
            .map(tasks::getTask)
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
        if (datafeedTasks.isEmpty()) {
            listener.onResponse(new Response(Collections.emptyMap()));
            return;
        }

        // Do this to catch datafeed tasks that have been created but are currently not assigned to a node.
        ActionListener<Response> taskResponseListener = ActionListener.wrap(actionResponses -> {
            Map<String, Response.RunningState> runningStateMap = actionResponses.getDatafeedRunningState();
            if (runningStateMap.size() == datafeedTasks.size()) {
                listener.onResponse(actionResponses);
                return;
            }
            List<Response> missingResponses = new ArrayList<>();
            missingResponses.add(actionResponses);
            missingResponses.add(
                new Response(
                    datafeedTasks.stream()
                        .map(t -> (StartDatafeedAction.DatafeedParams) t.getParams())
                        .filter(datafeedParams -> runningStateMap.containsKey(datafeedParams.getDatafeedId()) == false)
                        .collect(
                            Collectors.toMap(
                                StartDatafeedAction.DatafeedParams::getDatafeedId,
                                // If it isn't assigned to a node, assume that look back hasn't completed yet
                                params -> new Response.RunningState(params.getEndTime() == null, false, null)
                            )
                        )
                )
            );
            listener.onResponse(Response.fromResponses(missingResponses));
        }, listener::onFailure);

        String[] nodesOfConcern = datafeedTasks.stream()
            .map(PersistentTasksCustomMetadata.PersistentTask::getExecutorNode)
            .filter(Objects::nonNull)
            .filter(nodes::nodeExists)
            .toArray(String[]::new);

        if (nodesOfConcern.length == 0) {
            logger.debug(() -> format("Unable to find executor nodes for datafeed tasks %s", request.getDatafeedTaskIds()));

            taskResponseListener.onResponse(new Response(Collections.emptyMap()));
            return;
        }
        request.setNodes(nodesOfConcern);
        super.doExecute(task, request, taskResponseListener);
    }

}
