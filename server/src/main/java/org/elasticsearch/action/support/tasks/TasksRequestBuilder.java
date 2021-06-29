/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.support.tasks;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;

/**
 * Builder for task-based requests
 */
public class TasksRequestBuilder<
            Request extends BaseTasksRequest<Request>,
            Response extends BaseTasksResponse,
            RequestBuilder extends TasksRequestBuilder<Request, Response, RequestBuilder>
        > extends ActionRequestBuilder<Request, Response> {

    protected TasksRequestBuilder(ElasticsearchClient client, ActionType<Response> action, Request request) {
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

