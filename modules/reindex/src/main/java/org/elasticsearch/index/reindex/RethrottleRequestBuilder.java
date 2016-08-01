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

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.Action;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.support.tasks.TasksRequestBuilder;
import org.elasticsearch.client.ElasticsearchClient;

/**
 * Java API support for changing the throttle on reindex tasks while they are running.
 */
public class RethrottleRequestBuilder extends TasksRequestBuilder<RethrottleRequest, ListTasksResponse, RethrottleRequestBuilder> {
    public RethrottleRequestBuilder(ElasticsearchClient client,
            Action<RethrottleRequest, ListTasksResponse, RethrottleRequestBuilder> action) {
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
