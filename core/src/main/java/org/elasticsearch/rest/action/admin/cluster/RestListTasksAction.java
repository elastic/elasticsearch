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

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.BytesRestResponse;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestResponse;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestBuilderListener;
import org.elasticsearch.rest.action.RestToXContentListener;
import org.elasticsearch.tasks.TaskId;

import static org.elasticsearch.rest.RestRequest.Method.GET;


public class RestListTasksAction extends BaseRestHandler {
    private final ClusterService clusterService;

    @Inject
    public RestListTasksAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        controller.registerHandler(GET, "/_tasks", this);
    }

    public static ListTasksRequest generateListTasksRequest(RestRequest request) {
        boolean detailed = request.paramAsBoolean("detailed", false);
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("node_id"));
        String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        TimeValue timeout = request.paramAsTime("timeout", null);

        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setNodesIds(nodesIds);
        listTasksRequest.setDetailed(detailed);
        listTasksRequest.setActions(actions);
        listTasksRequest.setParentTaskId(parentTaskId);
        listTasksRequest.setWaitForCompletion(waitForCompletion);
        listTasksRequest.setTimeout(timeout);
        return listTasksRequest;
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final NodeClient client) {
        client.admin().cluster().listTasks(generateListTasksRequest(request), listTasksResponseListener(clusterService, channel));
    }

    /**
     * Standard listener for extensions of {@link ListTasksResponse} that supports {@code group_by=nodes}.
     */
    public static <T extends ListTasksResponse> ActionListener<T> listTasksResponseListener(ClusterService clusterService,
            RestChannel channel) {
        String groupBy = channel.request().param("group_by", "nodes");
        if ("nodes".equals(groupBy)) {
            return new RestBuilderListener<T>(channel) {
                @Override
                public RestResponse buildResponse(T response, XContentBuilder builder) throws Exception {
                    builder.startObject();
                    response.toXContentGroupedByNode(builder, channel.request(), clusterService.state().nodes());
                    builder.endObject();
                    return new BytesRestResponse(RestStatus.OK, builder);
                }
            };
        } else if ("parents".equals(groupBy)) {
            return new RestToXContentListener<>(channel);
        } else {
            throw new IllegalArgumentException("[group_by] must be one of [nodes] or [parents] but was [" + groupBy + "]");
        }
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }
}
