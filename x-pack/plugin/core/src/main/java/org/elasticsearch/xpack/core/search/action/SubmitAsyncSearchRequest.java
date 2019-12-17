/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to track asynchronously the progress of a search against one or more indices.
 *
 * @see AsyncSearchResponse
 */
public class SubmitAsyncSearchRequest extends ActionRequest implements CompositeIndicesRequest {
    private TimeValue waitForCompletion = TimeValue.timeValueSeconds(1);
    private boolean cleanOnCompletion = true;

    private final SearchRequest request;

    /**
     * Create a new request
     */
    public SubmitAsyncSearchRequest(String... indices) {
        this(new SearchSourceBuilder(), indices);
    }

    /**
     * Create a new request
     */
    public SubmitAsyncSearchRequest(SearchSourceBuilder source, String... indices) {
        this.request = new SearchRequest(indices, source);
        request.setCcsMinimizeRoundtrips(false);
        request.setPreFilterShardSize(1);
        request.setBatchedReduceSize(5);
        request.requestCache(true);
    }

    public SubmitAsyncSearchRequest(StreamInput in) throws IOException {
        this.request = new SearchRequest(in);
        this.waitForCompletion = in.readTimeValue();
        this.cleanOnCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        request.writeTo(out);
        out.writeTimeValue(waitForCompletion);
        out.writeBoolean(cleanOnCompletion);
    }

    public SearchRequest getSearchRequest() {
        return request;
    }

    /**
     * Set the minimum time that the request should wait before returning a partial result.
     */
    public void setWaitForCompletion(TimeValue waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    /**
     * Return the minimum time that the request should wait before returning a partial result.
     */
    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setBatchedReduceSize(int size) {
        request.setBatchedReduceSize(size);
    }

    public SearchSourceBuilder source() {
        return request.source();
    }

    /**
     * Should the resource be removed on completion or failure.
     */
    public boolean isCleanOnCompletion() {
        return cleanOnCompletion;
    }

    public void setCleanOnCompletion(boolean value) {
        this.cleanOnCompletion = value;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = request.validate();
        if (request.scroll() != null) {
            addValidationError("scroll queries are not supported", validationException);
        }
        if (request.isSuggestOnly()) {
            validationException = addValidationError("suggest-only queries are not supported", validationException);
        }

        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new Task(id, type, action, getDescription(), parentTaskId, headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubmitAsyncSearchRequest request1 = (SubmitAsyncSearchRequest) o;
        return cleanOnCompletion == request1.cleanOnCompletion &&
            Objects.equals(waitForCompletion, request1.waitForCompletion) &&
            request.equals(request1.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(waitForCompletion, cleanOnCompletion, request);
    }
}
