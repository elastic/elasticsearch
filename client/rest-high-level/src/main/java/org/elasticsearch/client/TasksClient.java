/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.client.tasks.CancelTasksRequest;
import org.elasticsearch.client.tasks.CancelTasksResponse;
import org.elasticsearch.client.tasks.GetTaskRequest;
import org.elasticsearch.client.tasks.GetTaskResponse;

import java.io.IOException;
import java.util.Optional;

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
        return restHighLevelClient.performRequestAndParseEntity(request, TasksRequestConverters::listTasks, options,
                ListTasksResponse::fromXContent, emptySet());
    }

    /**
     * Asynchronously get current tasks using the Task Management API.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable listAsync(ListTasksRequest request, RequestOptions options, ActionListener<ListTasksResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(request, TasksRequestConverters::listTasks, options,
                ListTasksResponse::fromXContent, listener, emptySet());
    }

    /**
     * Get a task using the Task Management API.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public Optional<GetTaskResponse> get(GetTaskRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseOptionalEntity(request, TasksRequestConverters::getTask, options,
                GetTaskResponse::fromXContent);
    }

    /**
     * Get a task using the Task Management API.
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener an actionlistener that takes an optional response (404s are returned as an empty Optional)
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getAsync(GetTaskRequest request, RequestOptions options,
                                ActionListener<Optional<GetTaskResponse>> listener) {

        return restHighLevelClient.performRequestAsyncAndParseOptionalEntity(request, TasksRequestConverters::getTask, options,
                GetTaskResponse::fromXContent, listener);
    }

    /**
     * Cancel one or more cluster tasks using the Task Management API.
     *
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     * @param cancelTasksRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     *
     */
    public CancelTasksResponse cancel(CancelTasksRequest cancelTasksRequest, RequestOptions options ) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            cancelTasksRequest,
            TasksRequestConverters::cancelTasks,
            options,
            CancelTasksResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously cancel one or more cluster tasks using the Task Management API.
     *
     * See
     * <a href="https://www.elastic.co/guide/en/elasticsearch/reference/current/tasks.html"> Task Management API on elastic.co</a>
     * @param cancelTasksRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable cancelAsync(CancelTasksRequest cancelTasksRequest, RequestOptions options,
                                   ActionListener<CancelTasksResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            cancelTasksRequest,
            TasksRequestConverters::cancelTasks,
            options,
            CancelTasksResponse::fromXContent,
            listener,
            emptySet()
        );
    }
}
