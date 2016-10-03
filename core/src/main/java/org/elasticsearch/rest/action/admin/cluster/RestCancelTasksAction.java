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

import org.elasticsearch.action.admin.cluster.node.tasks.cancel.CancelTasksRequest;
import org.elasticsearch.client.node.NodeClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;

import static org.elasticsearch.rest.RestRequest.Method.POST;
import static org.elasticsearch.rest.action.admin.cluster.RestListTasksAction.listTasksResponseListener;


public class RestCancelTasksAction extends BaseRestHandler {
    private final ClusterService clusterService;

    @Inject
    public RestCancelTasksAction(Settings settings, RestController controller, ClusterService clusterService) {
        super(settings);
        this.clusterService = clusterService;
        controller.registerHandler(POST, "/_tasks/_cancel", this);
        controller.registerHandler(POST, "/_tasks/{task_id}/_cancel", this);
    }

    @Override
    public RestChannelConsumer prepareRequest(final RestRequest request, final NodeClient client) throws IOException {
        final String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodes"));
        final TaskId taskId = new TaskId(request.param("task_id"));
        final String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        final TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        final String groupBy = request.param("group_by", "nodes");

        CancelTasksRequest cancelTasksRequest = new CancelTasksRequest();
        cancelTasksRequest.setTaskId(taskId);
        cancelTasksRequest.setNodes(nodesIds);
        cancelTasksRequest.setActions(actions);
        cancelTasksRequest.setParentTaskId(parentTaskId);
        return channel ->
            client.admin().cluster().cancelTasks(cancelTasksRequest, listTasksResponseListener(clusterService, groupBy, channel));
    }

    @Override
    public boolean canTripCircuitBreaker() {
        return false;
    }

}
