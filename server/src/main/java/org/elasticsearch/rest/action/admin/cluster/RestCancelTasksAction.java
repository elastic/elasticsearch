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
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.reindex.ReindexTaskManagementFeatures;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.io.IOException;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.Scope.INTERNAL;
import static org.elasticsearch.rest.action.admin.cluster.RestListTasksAction.listTasksResponseListener;

@ServerlessScope(INTERNAL)
public class RestCancelTasksAction extends BaseRestHandler {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestCancelTasksAction.class);

    private final Supplier<DiscoveryNodes> nodesInCluster;
    private final Predicate<NodeFeature> clusterSupportsFeature;

    public RestCancelTasksAction(Supplier<DiscoveryNodes> nodesInCluster, Predicate<NodeFeature> clusterSupportsFeature) {
        this.nodesInCluster = nodesInCluster;
        this.clusterSupportsFeature = clusterSupportsFeature;
    }

    @Override
    public String getName() {
        return "cancel_tasks_action";
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_tasks/_cancel"), new Route(POST, "/_tasks/{task_id}/_cancel"));
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodes"));
        final TaskId taskId = new TaskId(request.param("task_id"));
        final String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        final TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        final String groupBy = request.param("group_by", "nodes");

        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTargetTaskId(taskId);
        cancelTasksRequest.setNodes(nodesIds);
        cancelTasksRequest.setActions(actions);
        cancelTasksRequest.setTargetParentTaskId(parentTaskId);
        cancelTasksRequest.setWaitForCompletion(request.paramAsBoolean("wait_for_completion", cancelTasksRequest.waitForCompletion()));
        return channel -> {
            ActionListener<ListTasksResponse> listener = listTasksResponseListener(nodesInCluster, groupBy, channel);
            client.admin().cluster().cancelTasks(cancelTasksRequest, listener.delegateFailureAndWrap((l, resp) -> {
                softDeprecateReindexingTasks(resp);
                l.onResponse(resp);
            }));
        };
    }

    private void softDeprecateReindexingTasks(ListTasksResponse response) {
        if (clusterSupportsFeature.test(ReindexTaskManagementFeatures.REINDEX_MANAGEMENT_ENDPOINTS) == false) {
            return;
        }
        for (TaskInfo task : response.getTasks()) {
            if (RestGetTaskAction.isParentReindexTask(task)) {
                deprecationLogger.warn(
                    DeprecationCategory.API,
                    "cancel-api-deprecated-for-reindexing-tasks",
                    "Using the task management APIs to cancel reindex tasks is deprecated because they do not account for "
                        + "task relocations to other nodes. Use the dedicated reindex API instead: `POST /_reindex/{task_id}/_cancel`."
                );
                return;
            }
        }
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
