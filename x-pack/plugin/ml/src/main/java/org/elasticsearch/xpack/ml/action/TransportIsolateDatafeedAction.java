/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.util.List;

public class TransportIsolateDatafeedAction extends TransportTasksAction<TransportStartDatafeedAction.DatafeedTask,
        IsolateDatafeedAction.Request, IsolateDatafeedAction.Response, IsolateDatafeedAction.Response> {

    @Inject
    public TransportIsolateDatafeedAction(TransportService transportService, ActionFilters actionFilters, ClusterService clusterService) {
        super(IsolateDatafeedAction.NAME, clusterService, transportService, actionFilters, IsolateDatafeedAction.Request::new,
            IsolateDatafeedAction.Response::new, IsolateDatafeedAction.Response::new, MachineLearning.UTILITY_THREAD_POOL_NAME);
    }

    @Override
    protected void doExecute(Task task, IsolateDatafeedAction.Request request, ActionListener<IsolateDatafeedAction.Response> listener) {
        final ClusterState state = clusterService.state();
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(request.getDatafeedId(), tasks);

        if (datafeedTask == null || datafeedTask.getExecutorNode() == null) {
            // No running datafeed task to isolate
            listener.onResponse(new IsolateDatafeedAction.Response(false));
            return;
        }

        request.setNodes(datafeedTask.getExecutorNode());
        super.doExecute(task, request, listener);
    }

    @Override
    protected IsolateDatafeedAction.Response newResponse(IsolateDatafeedAction.Request request, List<IsolateDatafeedAction.Response> tasks,
                                                         List<TaskOperationFailure> taskOperationFailures,
                                                         List<FailedNodeException> failedNodeExceptions) {
        if (taskOperationFailures.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper
                    .convertToElastic(taskOperationFailures.get(0).getCause());
        } else if (failedNodeExceptions.isEmpty() == false) {
            throw org.elasticsearch.ExceptionsHelper
                    .convertToElastic(failedNodeExceptions.get(0));
        } else {
            return new IsolateDatafeedAction.Response(false);
        }
    }

    @Override
    protected void taskOperation(IsolateDatafeedAction.Request request, TransportStartDatafeedAction.DatafeedTask datafeedTask,
                                 ActionListener<IsolateDatafeedAction.Response> listener) {
        datafeedTask.isolate();
        listener.onResponse(new IsolateDatafeedAction.Response(false));
    }

}
