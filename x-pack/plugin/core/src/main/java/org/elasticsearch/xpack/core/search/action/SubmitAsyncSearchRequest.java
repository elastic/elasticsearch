/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.UntypedActionRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.async.AsyncTask;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.action.ValidateActions.addValidationError;

/**
 * A request to track asynchronously the progress of a search against one or more indices.
 *
 * @see AsyncSearchResponse
 */
public class SubmitAsyncSearchRequest extends UntypedActionRequest {
    public static final long MIN_KEEP_ALIVE = TimeValue.timeValueSeconds(1).millis();
    public static final TimeValue DEFAULT_KEEP_ALIVE = TimeValue.timeValueDays(5);

    private TimeValue waitForCompletionTimeout = TimeValue.timeValueSeconds(1);
    private boolean keepOnCompletion = false;
    @Nullable
    private TimeValue keepAlive = null;

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
        this.waitForCompletionTimeout = in.readTimeValue();
        if (in.getTransportVersion().supports(AsyncTask.ASYNC_DEFAULT_KEEP_ALIVE_SETTING)) {
            this.keepAlive = in.readOptionalTimeValue();
        } else {
            this.keepAlive = in.readTimeValue();
        }
        this.keepOnCompletion = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        request.writeTo(out);
        out.writeTimeValue(waitForCompletionTimeout);
        if (out.getTransportVersion().supports(AsyncTask.ASYNC_DEFAULT_KEEP_ALIVE_SETTING)) {
            out.writeOptionalTimeValue(keepAlive);
        } else {
            out.writeTimeValue(keepAlive != null ? keepAlive : DEFAULT_KEEP_ALIVE);
        }
        out.writeBoolean(keepOnCompletion);
    }

    /**
     * Sets the number of shard results that should be returned to notify search progress (default to 5).
     */
    public SubmitAsyncSearchRequest setBatchedReduceSize(int size) {
        request.setBatchedReduceSize(size);
        return this;
    }

    public int getBatchReduceSize() {
        return request.getBatchedReduceSize();
    }

    /**
     * Returns whether network round-trips should be minimized when executing cross-cluster search requests.
     * Defaults to <code>false</code>.
     */
    public boolean isCcsMinimizeRoundtrips() {
        return request.isCcsMinimizeRoundtrips();
    }

    /**
     * Sets whether network round-trips should be minimized when executing cross-cluster search requests.
     * Defaults to <code>false</code>.
     *
     * <p>WARNING: The progress and partial responses of searches executed on remote clusters will not be
     * available during the search if {@code ccsMinimizeRoundtrips} is enabled.</p>
     */
    public void setCcsMinimizeRoundtrips(boolean ccsMinimizeRoundtrips) {
        request.setCcsMinimizeRoundtrips(ccsMinimizeRoundtrips);
    }

    /**
     * Sets the minimum time that the request should wait before returning a partial result (defaults to 1 second).
     */
    public SubmitAsyncSearchRequest setWaitForCompletionTimeout(TimeValue waitForCompletionTimeout) {
        this.waitForCompletionTimeout = waitForCompletionTimeout;
        return this;
    }

    public TimeValue getWaitForCompletionTimeout() {
        return waitForCompletionTimeout;
    }

    /**
     * Sets the amount of time after which the result will expire. A {@code null} value means the transport
     * action will substitute the current value of the {@code async_search.default_keep_alive} cluster setting.
     */
    public SubmitAsyncSearchRequest setKeepAlive(@Nullable TimeValue keepAlive) {
        this.keepAlive = keepAlive;
        return this;
    }

    @Nullable
    public TimeValue getKeepAlive() {
        return keepAlive;
    }

    /**
     * Returns the underlying {@link SearchRequest}.
     */
    public SearchRequest getSearchRequest() {
        return request;
    }

    /**
     * Should the resource be kept on completion or failure (defaults to false).
     */
    public SubmitAsyncSearchRequest setKeepOnCompletion(boolean value) {
        this.keepOnCompletion = value;
        return this;
    }

    public boolean isKeepOnCompletion() {
        return keepOnCompletion;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = request.validate();
        if (request.scroll() != null) {
            validationException = addValidationError("[scroll] queries are not supported", validationException);
        }
        if (request.isSuggestOnly()) {
            validationException = addValidationError("suggest-only queries are not supported", validationException);
        }
        if (keepAlive != null && keepAlive.getMillis() < MIN_KEEP_ALIVE) {
            validationException = addValidationError(
                "[keep_alive] must be greater or equals than 1 second, got:" + keepAlive.toString(),
                validationException
            );
        }
        if (request.getPreFilterShardSize() == null || request.getPreFilterShardSize() != 1) {
            validationException = addValidationError(
                "[pre_filter_shard_size] cannot be changed for async search queries",
                validationException
            );
        }

        return validationException;
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new CancellableTask(id, type, action, null, parentTaskId, headers) {
            @Override
            public String getDescription() {
                // generating description in a lazy way since source can be quite big
                return "waitForCompletionTimeout["
                    + waitForCompletionTimeout
                    + "], keepOnCompletion["
                    + keepOnCompletion
                    + "] keepAlive["
                    + keepAlive
                    + "], request="
                    + request.buildDescription();
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
        SubmitAsyncSearchRequest request1 = (SubmitAsyncSearchRequest) o;
        return keepOnCompletion == request1.keepOnCompletion
            && waitForCompletionTimeout.equals(request1.waitForCompletionTimeout)
            && Objects.equals(keepAlive, request1.keepAlive)
            && request.equals(request1.request);
    }

    @Override
    public int hashCode() {
        return Objects.hash(waitForCompletionTimeout, keepOnCompletion, keepAlive, request);
    }
}
