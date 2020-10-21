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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.client.RequestConverters.EndpointBuilder;
import org.elasticsearch.client.tasks.CancelTasksRequest;
import org.elasticsearch.client.tasks.GetTaskRequest;

final class TasksRequestConverters {

    private TasksRequestConverters() {}

    static Request cancelTasks(CancelTasksRequest req) {
        Request request = new Request(HttpPost.METHOD_NAME, "/_tasks/_cancel");
        RequestConverters.Params params = new RequestConverters.Params();
        req.getTimeout().ifPresent(params::withTimeout);
        req.getTaskId().ifPresent(params::withTaskId);
        req.getParentTaskId().ifPresent(params::withParentTaskId);
        params
            .withNodes(req.getNodes())
            .withActions(req.getActions());
        if (req.getWaitForCompletion() != null) {
            params.withWaitForCompletion(req.getWaitForCompletion());
        }
        request.addParameters(params.asMap());
        return request;
    }

    static Request listTasks(ListTasksRequest listTaskRequest) {
        if (listTaskRequest.getTaskId() != null && listTaskRequest.getTaskId().isSet()) {
            throw new IllegalArgumentException("TaskId cannot be used for list tasks request");
        }
        Request request  = new Request(HttpGet.METHOD_NAME, "/_tasks");
        RequestConverters.Params params = new RequestConverters.Params();
        params.withTimeout(listTaskRequest.getTimeout())
            .withDetailed(listTaskRequest.getDetailed())
            .withWaitForCompletion(listTaskRequest.getWaitForCompletion())
            .withParentTaskId(listTaskRequest.getParentTaskId())
            .withNodes(listTaskRequest.getNodes())
            .withActions(listTaskRequest.getActions())
            .putParam("group_by", "none");
        request.addParameters(params.asMap());
        return request;
    }

    static Request getTask(GetTaskRequest getTaskRequest) {
        String endpoint = new EndpointBuilder().addPathPartAsIs("_tasks")
                .addPathPartAsIs(getTaskRequest.getNodeId() + ":" + Long.toString(getTaskRequest.getTaskId()))
                .build();
        Request request = new Request(HttpGet.METHOD_NAME, endpoint);
        RequestConverters.Params params = new RequestConverters.Params();
        params.withTimeout(getTaskRequest.getTimeout())
            .withWaitForCompletion(getTaskRequest.getWaitForCompletion());
        request.addParameters(params.asMap());
        return request;
    }

}
