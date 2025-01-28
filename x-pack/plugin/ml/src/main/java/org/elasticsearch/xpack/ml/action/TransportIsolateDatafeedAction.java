/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.injection.guice.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.core.ml.utils.ExceptionsHelper;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.List;

public class TransportIsolateDatafeedAction extends TransportTasksAction<
    TransportStartDatafeedAction.DatafeedTask,
    IsolateDatafeedAction.Request,
    IsolateDatafeedAction.Response,
    IsolateDatafeedAction.Response> {

    @Inject
    public TransportIsolateDatafeedAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(
            IsolateDatafeedAction.NAME,
            clusterService,
            transportService,
            actionFilters,
            IsolateDatafeedAction.Request::new,
            IsolateDatafeedAction.Response::new,
            transportService.getThreadPool().executor(MachineLearning.UTILITY_THREAD_POOL_NAME)
        );
    }

    @Override
    protected void doExecute(Task task, IsolateDatafeedAction.Request request, ActionListener<IsolateDatafeedAction.Response> listener) {
        final ClusterState state = clusterService.state();
        PersistentTasksCustomMetadata tasks = state.getMetadata().custom(PersistentTasksCustomMetadata.TYPE);
        PersistentTasksCustomMetadata.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(request.getDatafeedId(), tasks);

        if (datafeedTask == null || datafeedTask.getExecutorNode() == null) {
            // No running datafeed task to isolate
            listener.onResponse(new IsolateDatafeedAction.Response(false));
            return;
        }

        request.setNodes(datafeedTask.getExecutorNode());
        super.doExecute(task, request, listener);
    }

    @Override
    protected IsolateDatafeedAction.Response newResponse(
        IsolateDatafeedAction.Request request,
        List<IsolateDatafeedAction.Response> tasks,
        List<TaskOperationFailure> taskOperationFailures,
        List<FailedNodeException> failedNodeExceptions
    ) {
        // We only let people isolate one datafeed at a time, so each list will be empty or contain one item
        assert tasks.size() <= 1 : "more than 1 item in tasks: " + tasks.size();
        assert taskOperationFailures.size() <= 1 : "more than 1 item in taskOperationFailures: " + taskOperationFailures.size();
        assert failedNodeExceptions.size() <= 1 : "more than 1 item in failedNodeExceptions: " + failedNodeExceptions.size();
        if (taskOperationFailures.isEmpty() == false) {
            throw ExceptionsHelper.taskOperationFailureToStatusException(taskOperationFailures.get(0));
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw failedNodeExceptions.get(0);
        } else if (tasks.isEmpty() == false) {
            return tasks.get(0);
        }
        return new IsolateDatafeedAction.Response(false);
    }

    @Override
    protected void taskOperation(
        CancellableTask actionTask,
        IsolateDatafeedAction.Request request,
        TransportStartDatafeedAction.DatafeedTask datafeedTask,
        ActionListener<IsolateDatafeedAction.Response> listener
    ) {
        datafeedTask.isolate();
        listener.onResponse(new IsolateDatafeedAction.Response(true));
    }
}
