/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.prometheus.rest;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.util.Map;
import java.util.Objects;

/**
 * The request of a Prometheus read endpoint to {@link PrometheusQueryTransportAction}.
 * <p>
 * Like an ES|QL request, it is authorized by its action name only, as a {@link CompositeIndicesRequest}: the indices are
 * authorized when the ES|QL query it runs resolves them. Its task is cancellable, and cancelling it cancels that query.
 * It is only executed on the node that received the HTTP request, so it is never serialized.
 */
public class PrometheusQueryRequest extends ActionRequest implements CompositeIndicesRequest {

    private final PrometheusQuery query;
    private final String description;
    private final Object[] loggedParams;

    /**
     * @param description describes the query in the task list and in the ES|QL query log: the PromQL query, or the endpoint
     * @param loggedParams the request parameters the ES|QL query log records, as name and value pairs, see {@link PromqlQueryRequest}
     */
    PrometheusQueryRequest(PrometheusQuery query, String description, Object... loggedParams) {
        this.query = Objects.requireNonNull(query);
        this.description = Objects.requireNonNull(description);
        this.loggedParams = loggedParams;
    }

    PrometheusQuery query() {
        return query;
    }

    String description() {
        return description;
    }

    Object[] loggedParams() {
        return loggedParams;
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public void writeTo(StreamOutput out) {
        TransportAction.localOnly();
    }
}
