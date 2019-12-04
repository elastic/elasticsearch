/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

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
public class SubmitAsyncSearchRequest extends SearchRequest implements CompositeIndicesRequest {
    private TimeValue waitForCompletion = TimeValue.timeValueSeconds(1);

    /**
     * Create a new request
     * @param indices The indices the search will be executed on.
     * @param source The source of the search request.
     */
    public SubmitAsyncSearchRequest(String[] indices, SearchSourceBuilder source) {
        super(indices, source);
        setCcsMinimizeRoundtrips(false);
        setPreFilterShardSize(1);
        setBatchedReduceSize(5);
    }

    /**
     * Create a new request
     * @param indices The indices the search will be executed on.
     */
    public SubmitAsyncSearchRequest(String... indices) {
        this(indices, new SearchSourceBuilder());
    }

    public SubmitAsyncSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.waitForCompletion = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(waitForCompletion);
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

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (scroll() != null) {
            addValidationError("scroll queries are not supported", validationException);
        }
        if (isSuggestOnly()) {
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
        if (!super.equals(o)) return false;
        SubmitAsyncSearchRequest request = (SubmitAsyncSearchRequest) o;
        return waitForCompletion.equals(request.waitForCompletion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), waitForCompletion);
    }
}
