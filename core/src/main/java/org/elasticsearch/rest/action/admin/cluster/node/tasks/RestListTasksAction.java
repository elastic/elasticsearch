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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.action.support.RestToXContentListener;

import static org.elasticsearch.rest.RestRequest.Method.GET;


public class RestListTasksAction extends BaseRestHandler {

    @Inject
    public RestListTasksAction(Settings settings, RestController controller, Client client) {
        super(settings, controller, client);
        controller.registerHandler(GET, "/_tasks", this);
        controller.registerHandler(GET, "/_tasks/{nodeId}", this);
        controller.registerHandler(GET, "/_tasks/{nodeId}/{actions}", this);
    }

    @Override
    public void handleRequest(final RestRequest request, final RestChannel channel, final Client client) {
        boolean detailed = request.paramAsBoolean("detailed", false);
        String[] nodesIds = Strings.splitStringByCommaToArray(request.param("nodeId"));
        String[] actions = Strings.splitStringByCommaToArray(request.param("actions"));
        String parentNode = request.param("parent_node");
        long parentTaskId = request.paramAsLong("parent_task", ListTasksRequest.ALL_TASKS);

        ListTasksRequest listTasksRequest = new ListTasksRequest(nodesIds);
        listTasksRequest.detailed(detailed);
        listTasksRequest.actions(actions);
        listTasksRequest.parentNode(parentNode);
        listTasksRequest.parentTaskId(parentTaskId);
        client.admin().cluster().listTasks(listTasksRequest, new RestToXContentListener<>(channel));
    }
}
