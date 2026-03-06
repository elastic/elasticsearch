/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.Scope;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;

import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.admin.cluster.RestListTasksAction.listTasksResponseListener;

@ServerlessScope(Scope.INTERNAL)
public class RestReindexRethrottleAction extends BaseRestHandler {

    static final String REDACTED_NODE_ID_IN_STATELESS = "stateless";

    private final Supplier<DiscoveryNodes> nodesInCluster;
    private final boolean isStateless;

    public RestReindexRethrottleAction(Supplier<DiscoveryNodes> nodesInCluster, Settings settings) {
        this.nodesInCluster = nodesInCluster;
        this.isStateless = DiscoveryNode.isStateless(settings);
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(POST, "/_reindex/{task_id}/_rethrottle"));
    }

    @Override
    public String getName() {
        return "reindex_rethrottle_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) {
        RethrottleRequest internalRequest = new RethrottleRequest();
        internalRequest.setTargetTaskId(new TaskId(request.param("task_id")));
        Float requestsPerSecond = AbstractBaseReindexRestHandler.parseRequestsPerSecond(request);
        if (requestsPerSecond == null) {
            throw new IllegalArgumentException("requests_per_second is a required parameter");
        }
        internalRequest.setRequestsPerSecond(requestsPerSecond);
        // This ListTasksResponse will only ever contain a single task, so grouping them is not very useful.
        // In stateful, we allow the group_by parameter and default to "nodes", for historical reasons.
        // In stateless, we don't allow group_by, we never group, and we redact the node IDs: this minimizes the visibility of node IDs.
        final String groupBy = isStateless ? "none" : request.param("group_by", "nodes");
        return channel -> {
            ActionListener<ListTasksResponse> responseListener = listTasksResponseListener(nodesInCluster, groupBy, channel);
            client.execute(
                ReindexPlugin.RETHROTTLE_ACTION,
                internalRequest,
                isStateless ? responseListener.map(RestReindexRethrottleAction::redactNodeIdsInListTasksResponse) : responseListener
            );
        };
    }

    private static ListTasksResponse redactNodeIdsInListTasksResponse(ListTasksResponse originalResponse) {
        return new ListTasksResponse(
            originalResponse.getTasks().stream().map(RestReindexRethrottleAction::redactNodeIdInTaskInfo).toList(),
            originalResponse.getTaskFailures().stream().map(RestReindexRethrottleAction::redactNodeIdInTaskOperationFailure).toList(),
            originalResponse.getNodeFailures()
        );
    }

    private static TaskInfo redactNodeIdInTaskInfo(TaskInfo originalTaskInfo) {
        return new TaskInfo(
            originalTaskInfo.taskId(),
            originalTaskInfo.type(),
            REDACTED_NODE_ID_IN_STATELESS,
            originalTaskInfo.action(),
            originalTaskInfo.description(),
            originalTaskInfo.status(),
            originalTaskInfo.startTime(),
            originalTaskInfo.runningTimeNanos(),
            originalTaskInfo.cancellable(),
            originalTaskInfo.cancelled(),
            originalTaskInfo.parentTaskId(),
            originalTaskInfo.headers()
        );
    }

    private static TaskOperationFailure redactNodeIdInTaskOperationFailure(TaskOperationFailure taskOperationFailure) {
        return new TaskOperationFailure(REDACTED_NODE_ID_IN_STATELESS, taskOperationFailure.getTaskId(), taskOperationFailure.getCause());
    }
}
