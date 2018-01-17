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
package org.elasticsearch.action.support.tasks;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;

/**
 * Builder for task-based requests
 */
public class TasksRequestBuilder<
            Request extends BaseTasksRequest<Request>,
            Response extends BaseTasksResponse,
            RequestBuilder extends TasksRequestBuilder<Request, Response, RequestBuilder>
        > extends ActionRequestBuilder<Request, Response, RequestBuilder> {

    protected TasksRequestBuilder(ElasticsearchClient client, Action<Request, Response, RequestBuilder> action, Request request) {
        super(client, action, request);
    }

    /**
     * Set the task to lookup.
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setTaskId(TaskId taskId) {
        request.setTaskId(taskId);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setNodesIds(String... nodesIds) {
        request.setNodes(nodesIds);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setActions(String... actions) {
        request.setActions(actions);
        return (RequestBuilder) this;
    }

    @SuppressWarnings("unchecked")
    public final RequestBuilder setTimeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return (RequestBuilder) this;
    }

    /**
     * Match all children of the provided task. 
     */
    @SuppressWarnings("unchecked")
    public final RequestBuilder setParentTaskId(TaskId taskId) {
        request.setParentTaskId(taskId);
        return (RequestBuilder) this;
    }
}

