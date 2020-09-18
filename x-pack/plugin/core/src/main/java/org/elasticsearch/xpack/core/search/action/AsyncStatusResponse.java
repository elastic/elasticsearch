/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xpack.core.async.AsyncResponse;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;

/**
 * A response of an async search request.
 */
public class AsyncStatusResponse extends ActionResponse implements StatusToXContentObject, AsyncResponse<AsyncStatusResponse> {
    private final String id;
    private final boolean isRunning;
    private final long startTimeMillis;
    private final long expirationTimeMillis;
    private final int totalShards;
    private final int successfulShards;
    private final int skippedShards;
    private final int failedShards;

    private AsyncStatusResponse(String id,
            boolean isRunning,
            long startTimeMillis,
            long expirationTimeMillis,
            int totalShards,
            int successfulShards,
            int skippedShards,
            int failedShards) {
        this.id = id;
        this.isRunning = isRunning;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.skippedShards = skippedShards;
        this.failedShards = failedShards;
    }

    /**
     * Creates a new {@link AsyncStatusResponse} for a running search.
     *
     * @param id The id of the async search.
     * @param startTimeMillis The start date of the search in milliseconds since epoch.
     * @param expirationTimeMillis The expiration date of the search in milliseconds since epoch.
     * @param totalShards The total number of shards the request search is executed on
     * @param successfulShards The number of shards the request is completed on
     * @param skippedShards The number of skipped shards
     * @param failedShards The number of shards that failed to executed the request
     */
    public AsyncStatusResponse(String id,
            long startTimeMillis,
            long expirationTimeMillis,
            int totalShards,
            int successfulShards,
            int skippedShards,
            int failedShards) {
        this(id, true, startTimeMillis, expirationTimeMillis, totalShards, successfulShards, skippedShards, failedShards);
    }

    /**
     * Creates a new {@link AsyncStatusResponse} for a completed search.
     *
     * @param id The id of the async search.
     * @param expirationTimeMillis – expiration time in milliseconds
     * @return status of the completed async search
     */
    public static AsyncStatusResponse getCompletedSearchStatusResponse(String id, long expirationTimeMillis) {
        return new AsyncStatusResponse(id, false, 0, expirationTimeMillis, 0, 0, 0, 0);
    }

    @Override
    public AsyncStatusResponse withExpirationTime(long pexpirationTimeMillis) {
        return new AsyncStatusResponse(
            id, isRunning, startTimeMillis, pexpirationTimeMillis, totalShards, successfulShards, skippedShards, failedShards);
    }



    public AsyncStatusResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.isRunning = in.readBoolean();
        this.startTimeMillis = in.readLong();
        this.expirationTimeMillis = in.readLong();
        this.totalShards = in.readVInt();
        this.successfulShards = in.readVInt();
        this.skippedShards = in.readVInt();
        this.failedShards = in.readVInt();
    }



    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(isRunning);
        out.writeLong(startTimeMillis);
        out.writeLong(expirationTimeMillis);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(skippedShards);
        out.writeVInt(failedShards);
    }

    @Override
    public RestStatus status() {
        return OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("is_running", isRunning);
        if (isRunning) { // information only reported for a currently running task
            builder.timeField("start_time_in_millis", "start_time", startTimeMillis);
        }
        builder.timeField("expiration_time_in_millis", "expiration_time", expirationTimeMillis);
        if (isRunning) { // information only reported for a currently running task
            RestActions.buildBroadcastShardsHeader(builder, params, totalShards, successfulShards, skippedShards, failedShards, null);
        }
        builder.endObject();
        return builder;
    }

    /**
     * Returns the id of the async search status request.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns {@code true} if the search is still running in the cluster,
     * or {@code false} if the search has been completed.
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * For a running search returns a timestamp when the search tasks started, in milliseconds since epoch.
     * For a completed search returns {@code null}.
     */
    public Long getStartTime() {
        return isRunning ? startTimeMillis : null;
    }

    /**
     * Returns a timestamp when the search will be expired, in milliseconds since epoch.
     */
    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    /**
     * For a running search returns the total number of shards the search is executed on.
     * For a completed search returns {@code null}.
     */
    public Integer getTotalShards() {
        return isRunning ? totalShards : null;
    }

    /**
     * For a running search returns the number of successful shards the search was executed on.
     * For a completed search returns {@code null}.
     */
    public Integer getSuccessfulShards() {
        return isRunning ? successfulShards : null;
    }

    /**
     * For a running search returns the number of skipped shards due to pre-filtering.
     * For a completed search returns {@code null}.
     */
    public Integer getSkippedShards() {
        return isRunning ? skippedShards : null;
    }

    /**
     * For a running search returns the number of failed shards the search was executed on.
     * For a completed search returns {@code null}.
     */
    public Integer getFailedShards() {
        return isRunning ? failedShards : null;
    }

}
