/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.CancellableTask;
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
public class SubmitAsyncSearchRequest extends ActionRequest {
    public static long MIN_KEEP_ALIVE = TimeValue.timeValueHours(1).millis();

    private TimeValue waitForCompletion = TimeValue.timeValueSeconds(1);
    private boolean cleanOnCompletion = true;
    private TimeValue keepAlive = TimeValue.timeValueDays(5);

    private final SearchRequest request;

    /**
     * Creates a new request
     */
    public SubmitAsyncSearchRequest(String... indices) {
        this(new SearchSourceBuilder(), indices);
    }

    /**
     * Creates a new request
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
        this.keepAlive = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        request.writeTo(out);
        out.writeTimeValue(waitForCompletion);
        out.writeBoolean(cleanOnCompletion);
        out.writeTimeValue(keepAlive);
    }

    public SearchRequest getSearchRequest() {
        return request;
    }

    /**
     * Sets the minimum time that the request should wait before returning a partial result.
     */
    public void setWaitForCompletion(TimeValue waitForCompletion) {
        this.waitForCompletion = waitForCompletion;
    }

    /**
     * Returns the minimum time that the request should wait before returning a partial result.
     */
    public TimeValue getWaitForCompletion() {
        return waitForCompletion;
    }

    public void setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
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

    public void setBatchedReduceSize(int size) {
        request.setBatchedReduceSize(size);
    }

    public SearchSourceBuilder source() {
        return request.source();
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
        if (keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException =
                addValidationError("keep_alive must be greater than 1 minute, got:" + keepAlive.toString(), validationException);
        }

        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, "", parentTaskId, headers) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return true;
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubmitAsyncSearchRequest request1 = (SubmitAsyncSearchRequest) o;
        return cleanOnCompletion == request1.cleanOnCompletion &&
            waitForCompletion.equals(request1.waitForCompletion) &&
            keepAlive.equals(request1.keepAlive) &&
            request.equals(request1.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(waitForCompletion, cleanOnCompletion, keepAlive, request);
    }
}
