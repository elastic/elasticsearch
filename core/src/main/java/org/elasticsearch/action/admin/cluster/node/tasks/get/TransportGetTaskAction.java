/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.get;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.HandledTransportAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.tasks.PersistedTaskInfo;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskPersistenceService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.BaseTransportResponseHandler;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;

import static org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction.waitForCompletionTimeout;

/**
 * Action to get a single task. If the task isn't running then it'll try to request the status from request index.
 *
 * The general flow is:
 * <ul>
 * <li>If this isn't being executed on the node to which the requested TaskId belongs then move to that node.
 * <li>Look up the task and return it if it exists
 * <li>If it doesn't then look up the task from the results index
 * </ul>
 */
public class TransportGetTaskAction extends HandledTransportAction<GetTaskRequest, GetTaskResponse> {
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final Client client;

    @Inject
    public TransportGetTaskAction(Settings settings, ThreadPool threadPool, TransportService transportService, ActionFilters actionFilters,
            IndexNameExpressionResolver indexNameExpressionResolver, ClusterService clusterService, Client client) {
        super(settings, GetTaskAction.NAME, threadPool, transportService, actionFilters, indexNameExpressionResolver, GetTaskRequest::new);
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.client = client;
    }

    @Override
    protected void doExecute(GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        throw new UnsupportedOperationException("Task is required");
    }

    @Override
    protected void doExecute(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        if (clusterService.localNode().getId().equals(request.getTaskId().getNodeId())) {
            getRunningTaskFromNode(thisTask, request, listener);
        } else {
            runOnNodeWithTaskIfPossible(thisTask, request, listener);
        }
    }

    /**
     * Executed on the coordinating node to forward execution of the remaining work to the node that matches that requested
     * {@link TaskId#getNodeId()}. If the node isn't in the cluster then this will just proceed to
     * {@link #getFinishedTaskFromIndex(Task, GetTaskRequest, ActionListener)} on this node.
     */
    private void runOnNodeWithTaskIfPossible(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        TransportRequestOptions.Builder builder = TransportRequestOptions.builder();
        if (request.getTimeout() != null) {
            builder.withTimeout(request.getTimeout());
        }
        builder.withCompress(false);
        DiscoveryNode node = clusterService.state().nodes().get(request.getTaskId().getNodeId());
        if (node == null) {
            // Node is no longer part of the cluster! Try and look the task up from the results index.
            getFinishedTaskFromIndex(thisTask, request, listener);
            return;
        }
        GetTaskRequest nodeRequest = request.nodeRequest(clusterService.localNode().getId(), thisTask.getId());
        taskManager.registerChildTask(thisTask, node.getId());
        transportService.sendRequest(node, GetTaskAction.NAME, nodeRequest, builder.build(),
                new BaseTransportResponseHandler<GetTaskResponse>() {
                    @Override
                    public GetTaskResponse newInstance() {
                        return new GetTaskResponse();
                    }

                    @Override
                    public void handleResponse(GetTaskResponse response) {
                        listener.onResponse(response);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        listener.onFailure(exp);
                    }

                    @Override
                    public String executor() {
                        return ThreadPool.Names.SAME;
                    }
                });
    }

    /**
     * Executed on the node that should be running the task to find and return the running task. Falls back to
     * {@link #getFinishedTaskFromIndex(Task, GetTaskRequest, ActionListener)} if the task isn't still running.
     */
    void getRunningTaskFromNode(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        Task runningTask = taskManager.getTask(request.getTaskId().getId());
        if (runningTask == null) {
            getFinishedTaskFromIndex(thisTask, request, listener);
        } else {
            if (request.getWaitForCompletion()) {
                // Shift to the generic thread pool and let it wait for the task to complete so we don't block any important threads.
                threadPool.generic().execute(new AbstractRunnable() {
                    @Override
                    protected void doRun() throws Exception {
                        taskManager.waitForTaskCompletion(runningTask, waitForCompletionTimeout(request.getTimeout()));
                        // TODO look up the task's result from the .tasks index now that it is done
                        listener.onResponse(
                                new GetTaskResponse(new PersistedTaskInfo(runningTask.taskInfo(clusterService.localNode(), true))));
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        listener.onFailure(t);
                    }
                });
            } else {
                listener.onResponse(new GetTaskResponse(new PersistedTaskInfo(runningTask.taskInfo(clusterService.localNode(), true))));
            }
        }
    }

    /**
     * Send a {@link GetRequest} to the results index looking for the results of the task. It'll only be found only if the task's result was
     * persisted. Called on the node that once had the task if that node is part of the cluster or on the coordinating node if the node
     * wasn't part of the cluster.
     */
    void getFinishedTaskFromIndex(Task thisTask, GetTaskRequest request, ActionListener<GetTaskResponse> listener) {
        GetRequest get = new GetRequest(TaskPersistenceService.TASK_INDEX, TaskPersistenceService.TASK_TYPE,
                request.getTaskId().toString());
        get.setParentTask(clusterService.localNode().getId(), thisTask.getId());
        client.get(get, new ActionListener<GetResponse>() {
            @Override
            public void onResponse(GetResponse getResponse) {
                try {
                    onGetFinishedTaskFromIndex(getResponse, listener);
                } catch (Throwable e) {
                    listener.onFailure(e);
                }
            }

            @Override
            public void onFailure(Throwable e) {
                if (ExceptionsHelper.unwrap(e, IndexNotFoundException.class) != null) {
                    // We haven't yet created the index for the task results so it can't be found.
                    listener.onFailure(new ResourceNotFoundException("task [{}] isn't running or persisted", e, request.getTaskId()));
                } else {
                    listener.onFailure(e);
                }
            }
        });
    }

    /**
     * Called with the {@linkplain GetResponse} from loading the task from the results index. Called on the node that once had the task if
     * that node is part of the cluster or on the coordinating node if the node wasn't part of the cluster.
     */
    void onGetFinishedTaskFromIndex(GetResponse response, ActionListener<GetTaskResponse> listener) throws IOException {
        if (false == response.isExists()) {
            listener.onFailure(new ResourceNotFoundException("task [{}] isn't running or persisted", response.getId()));
        }
        if (response.isSourceEmpty()) {
            listener.onFailure(new ElasticsearchException("Stored task status for [{}] didn't contain any source!", response.getId()));
            return;
        }
        try (XContentParser parser = XContentHelper.createParser(response.getSourceAsBytesRef())) {
            PersistedTaskInfo result = PersistedTaskInfo.PARSER.apply(parser, () -> ParseFieldMatcher.STRICT);
            listener.onResponse(new GetTaskResponse(result));
        }
    }
}
