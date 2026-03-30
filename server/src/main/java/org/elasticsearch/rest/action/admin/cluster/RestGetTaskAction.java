/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.ReindexTaskManagementFeatures;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.RestUtils.getTimeout;
import static org.elasticsearch.rest.Scope.PUBLIC;

@ServerlessScope(PUBLIC)
public class RestGetTaskAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestGetTaskAction.class);

    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestGetTaskAction(Predicate<NodeFeature> clusterSupportsFeature) {
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_tasks/{task_id}"));
    }

    @Override
    public String getName() {
        return "get_task_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        TaskId taskId = new TaskId(request.param("task_id"));
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        TimeValue timeout = getTimeout(request);

        GetTaskRequest getTaskRequest = new GetTaskRequest();
        getTaskRequest.setTaskId(taskId);
        getTaskRequest.setWaitForCompletion(waitForCompletion);
        getTaskRequest.setTimeout(timeout);
        return channel -> {
            ActionListener<GetTaskResponse> delegate = new RestToXContentListener<>(channel);
            client.admin().cluster().getTask(getTaskRequest, new ActionListener<>() {
                @Override
                public void onResponse(GetTaskResponse response) {
                    softDeprecateReindexingTasks(response);
                    delegate.onResponse(response);
                }

                @Override
                public void onFailure(Exception e) {
                    delegate.onFailure(e);
                }
            });
        };
    }

    private void softDeprecateReindexingTasks(GetTaskResponse response) {
        if (clusterSupportsFeature.test(ReindexTaskManagementFeatures.REINDEX_MANAGEMENT_ENDPOINTS) == false) {
            return;
        }
        TaskInfo task = response.getTask().getTask();
        if (isParentReindexTask(task) == false) {
            return;
        }
        deprecationLogger.warn(
            DeprecationCategory.API,
            "get-api-deprecated-for-reindexing-tasks",
            "Using the task management APIs to get reindexing tasks is deprecated because they do not account for "
                + "task relocations to other nodes. Use the dedicated reindex API instead: `GET /_reindex/{task_id}`"
        );
    }

    static boolean isParentReindexTask(TaskInfo task) {
        return task != null && ReindexAction.NAME.equals(task.action()) && task.parentTaskId().isSet() == false;
    }
}
