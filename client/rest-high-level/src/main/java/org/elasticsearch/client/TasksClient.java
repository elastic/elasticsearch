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

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;

import java.io.IOException;

import static java.util.Collections.emptySet;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Tasks API.
 * <p>
 * See <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html">Task Management API on elastic.co</a>
 */
public final class TasksClient {
    private final RestHighLevelClient restHighLevelClient;

    TasksClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Get current tasks using the Task Management API.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ListTasksResponse list(ListTasksRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(request, RequestConverters::listTasks, options,
                ListTasksResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get current tasks using the Task Management API.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     */
    public void listAsync(ListTasksRequest request, RequestOptions options, ActionListener<ListTasksResponse> listener) {
        restHighLevelClient.performRequestAsyncAndParseEntity(request, RequestConverters::listTasks, options,
                ListTasksResponse::fromXContent, listener, emptySet());
    }
}
