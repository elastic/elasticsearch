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

package org.elasticsearch.rest.action.admin.cluster.node.tasks;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestToXContentListener;
import org.elasticsearch.tasks.TaskId;

import static org.elasticsearch.rest.RestRequest.Method.GET;


public class RestListTasksAction extends BaseRestHandler {

    @Inject
    public RestListTasksAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_tasks", this);
        controller.registerHandler(GET, "/_tasks/{taskId}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        boolean detailed = request.paramAsBoolean("detailed", false);
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("node_id"));
        TaskId taskId = new TaskId(request.param("taskId"));
        String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        TaskId parentTaskId = new TaskId(request.param("parent_task_id"));
        boolean waitForCompletion = request.paramAsBoolean("wait_for_completion", false);
        TimeValue timeout = request.paramAsTime("timeout", null);

        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setTaskId(taskId);
        listTasksRequest.setNodesIds(nodesIds);
        listTasksRequest.setDetailed(detailed);
        listTasksRequest.setActions(actions);
        listTasksRequest.setParentTaskId(parentTaskId);
        listTasksRequest.setWaitForCompletion(waitForCompletion);
        listTasksRequest.setTimeout(timeout);
        client.admin().cluster().listTasks(listTasksRequest, new RestToXContentListener<ListTasksResponse>(channel));
    }
}
