/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.reindex;

import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.tasks.TasksRequestBuilder;
import org.elasticsearch.client.internal.ElasticsearchClient;

/**
 * Java API support for changing the throttle on reindex tasks while they are running.
 */
public class RethrottleRequestBuilder extends TasksRequestBuilder<RethrottleRequest, ListTasksResponse, RethrottleRequestBuilder> {
    public RethrottleRequestBuilder(ElasticsearchClient client, ActionType<ListTasksResponse> action) {
        super(client, action, new RethrottleRequest());
    }

    /**
     * Set the throttle to apply to all matching requests in sub-requests per second. {@link Float#POSITIVE_INFINITY} means set no throttle.
     * Throttling is done between batches, as we start the next scroll requests. That way we can increase the scroll's timeout to make sure
     * that it contains any time that we might wait.
     */
    public RethrottleRequestBuilder setRequestsPerSecond(float requestsPerSecond) {
        request.setRequestsPerSecond(requestsPerSecond);
        return this;
    }
}
