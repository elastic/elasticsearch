/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.ml.MlTasks;
import org.elasticsearch.xpack.core.ml.action.IsolateDatafeedAction;
import org.elasticsearch.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.ml.MachineLearning;

import java.io.IOException;
import java.util.List;

public class TransportIsolateDatafeedAction extends TransportTasksAction<TransportStartDatafeedAction.DatafeedTask,
        IsolateDatafeedAction.Request, IsolateDatafeedAction.Response, IsolateDatafeedAction.Response> {

    @Inject
    public TransportIsolateDatafeedAction(Settings settings, TransportService transportService, ThreadPool threadPool,
                                          ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                          ClusterService clusterService) {
        super(settings, IsolateDatafeedAction.NAME, threadPool, clusterService, transportService, actionFilters,
                indexNameExpressionResolver, IsolateDatafeedAction.Request::new, IsolateDatafeedAction.Response::new,
                MachineLearning.UTILITY_THREAD_POOL_NAME);
    }

    @Override
    protected void doExecute(Task task, IsolateDatafeedAction.Request request, ActionListener<IsolateDatafeedAction.Response> listener) {
        final ClusterState state = clusterService.state();
        PersistentTasksCustomMetaData tasks = state.getMetaData().custom(PersistentTasksCustomMetaData.TYPE);
        PersistentTasksCustomMetaData.PersistentTask<?> datafeedTask = MlTasks.getDatafeedTask(request.getDatafeedId(), tasks);

        if (datafeedTask == null || datafeedTask.getExecutorNode() == null) {
            // No running datafeed task to isolate
            listener.onResponse(new IsolateDatafeedAction.Response());
            return;
        }

        String executorNode = datafeedTask.getExecutorNode();
        DiscoveryNodes nodes = state.nodes();
        if (nodes.resolveNode(executorNode).getVersion().before(Version.V_5_5_0)) {
            listener.onFailure(new ElasticsearchException("Force delete datafeed is not supported because the datafeed task " +
                    "is running on a node [" + executorNode + "] with a version prior to " + Version.V_5_5_0));
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
            return new IsolateDatafeedAction.Response();
        }
    }

    @Override
    protected void taskOperation(IsolateDatafeedAction.Request request, TransportStartDatafeedAction.DatafeedTask datafeedTask,
                                 ActionListener<IsolateDatafeedAction.Response> listener) {
        datafeedTask.isolate();
        listener.onResponse(new IsolateDatafeedAction.Response());
    }

    @Override
    protected IsolateDatafeedAction.Response readTaskResponse(StreamInput in) throws IOException {
        return new IsolateDatafeedAction.Response(in);
    }
}
