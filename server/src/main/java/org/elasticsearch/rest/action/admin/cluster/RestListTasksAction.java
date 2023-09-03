/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.ServerlessScope;
import org.elasticsearch.rest.action.RestCancellableNodeClient;
import org.elasticsearch.rest.action.RestChunkedToXContentListener;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.rest.RestRequest.Method.GET;
import static org.elasticsearch.rest.Scope.INTERNAL;

@ServerlessScope(INTERNAL)
public class RestListTasksAction extends BaseRestHandler {

    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestListTasksAction(Supplier<DiscoveryNodes> nodesInCluster) {
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public List<Route> routes() {
        return List.of(new Route(GET, "/_tasks"));
    }

    @Override
    public String getName() {
        return "list_tasks_action";
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final ListTasksRequest listTasksRequest = generateListTasksRequest(request);
        final String groupBy = request.param("group_by", "nodes");
        return channel -> new RestCancellableNodeClient(client, request.getHttpChannel()).admin()
            .cluster()
            .listTasks(listTasksRequest, listTasksResponseListener(nodesInCluster, groupBy, channel));
    }

    public static ListTasksRequest generateListTasksRequest(RestRequest request) {
        boolean detailed = request.paramAsBoolean("detailed", false);
        String[] nodes = Strings.splitStringByCommaToArray(request.param("nodes"));
        String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        TimeValue timeout = request.paramAsTime("timeout", null);

        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setNodes(nodes);
        listTasksRequest.setDetailed(detailed);
        listTasksRequest.setActions(actions);
        listTasksRequest.setTargetParentTaskId(parentTaskId);
        listTasksRequest.setWaitForCompletion(waitForCompletion);
        listTasksRequest.setTimeout(timeout);
        return listTasksRequest;
    }

    /**
     * Standard listener for extensions of {@link ListTasksResponse} that supports {@code group_by=nodes}.
     */
    public static <T extends ListTasksResponse> ActionListener<T> listTasksResponseListener(
        Supplier<DiscoveryNodes> nodesInCluster,
        String groupBy,
        RestChannel channel
    ) {
        final var listener = new RestChunkedToXContentListener<>(channel);
        return switch (groupBy) {
            case "nodes" -> listener.map(response -> response.groupedByNode(nodesInCluster));
            case "parents" -> listener.map(response -> response.groupedByParent());
            case "none" -> listener.map(response -> response.groupedByNone());
            default -> throw new IllegalArgumentException(
                "[group_by] must be one of [nodes], [parents] or [none] but was [" + groupBy + "]"
            );
        };
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
