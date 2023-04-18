/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.get;

import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.TaskId;

/**
 * Builder for the request to retrieve the list of tasks running on the specified nodes
 */
public class GetTaskRequestBuilder extends ActionRequestBuilder<GetTaskRequest, GetTaskResponse> {
    public GetTaskRequestBuilder(ElasticsearchClient client, GetTaskAction action) {
        super(client, action, new GetTaskRequest());
    }

    /**
     * Set the TaskId to look up. Required.
     */
    public final GetTaskRequestBuilder setTaskId(TaskId taskId) {
        request.setTaskId(taskId);
        return this;
    }

    /**
     * Should this request wait for all found tasks to complete?
     */
    public final GetTaskRequestBuilder setWaitForCompletion(boolean waitForCompletion) {
        request.setWaitForCompletion(waitForCompletion);
        return this;
    }

    /**
     * Timeout to wait for any async actions this request must take. It must take anywhere from 0 to 2.
     */
    public final GetTaskRequestBuilder setTimeout(TimeValue timeout) {
        request.setTimeout(timeout);
        return this;
    }
}
