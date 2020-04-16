/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.eql.action;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to track asynchronously the progress of an eql search against one or more indices.
 *
 * @see AsyncEqlSearchResponse
 */
public class SubmitAsyncEqlSearchRequest extends ActionRequest {
    public static long MIN_KEEP_ALIVE = TimeValue.timeValueMinutes(1).millis();

    private TimeValue waitForCompletionTimeout = TimeValue.timeValueSeconds(1);
    private boolean keepOnCompletion = false;
    private TimeValue keepAlive = TimeValue.timeValueDays(5);

    private final EqlSearchRequest request;

    /**
     * Creates a new request
     */
    public SubmitAsyncEqlSearchRequest(String... indices) {
        this(new EqlSearchRequest().indices(indices));
    }

    /**
     * Creates a new request
     */
    public SubmitAsyncEqlSearchRequest(EqlSearchRequest innerRequest) {
        this.request = innerRequest;
    }

    public SubmitAsyncEqlSearchRequest(StreamInput in) throws IOException {
        this.request = new EqlSearchRequest(in);
        this.waitForCompletionTimeout = in.readTimeValue();
        this.keepAlive = in.readTimeValue();
        this.keepOnCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        request.writeTo(out);
        out.writeTimeValue(waitForCompletionTimeout);
        out.writeTimeValue(keepAlive);
        out.writeBoolean(keepOnCompletion);
    }


    /**
     * Sets the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public SubmitAsyncEqlSearchRequest setWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
        return this;
    }

    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    /**
     * Sets the amount of time after which the result will expire (defaults to 5 days).
     */
    public SubmitAsyncEqlSearchRequest setKeepAlive(TimeValue keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    /**
     * Returns the underlying {@link EqlSearchRequest}.
     */
    public EqlSearchRequest getEqlSearchRequest() {
        return request;
    }

    /**
     * Should the resource be kept on completion or failure (defaults to false).
     */
    public SubmitAsyncEqlSearchRequest setKeepOnCompletion(boolean value) {
        this.keepOnCompletion = value;
        return this;
    }

    public boolean isKeepOnCompletion() {
        return keepOnCompletion;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = request.validate();
        if (keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException =
                addValidationError("[keep_alive] must be greater than 1 minute, got:" + keepAlive.toString(), validationException);
        }
        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, toString(), parentTaskId, headers) {
            @Override
            public boolean shouldCancelChildrenOnCancellation() {
                return true;
            }
        };
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SubmitAsyncEqlSearchRequest request1 = (SubmitAsyncEqlSearchRequest) o;
        return keepOnCompletion == request1.keepOnCompletion &&
            waitForCompletionTimeout.equals(request1.waitForCompletionTimeout) &&
            keepAlive.equals(request1.keepAlive) &&
            request.equals(request1.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(waitForCompletionTimeout, keepOnCompletion, keepAlive, request);
    }

    @Override
    public String toString() {
        return "SubmitAsyncEqlSearchRequest{" +
            "waitForCompletionTimeout=" + waitForCompletionTimeout +
            ", keepOnCompletion=" + keepOnCompletion +
            ", keepAlive=" + keepAlive +
            ", request=" + request +
            '}';
    }
}
