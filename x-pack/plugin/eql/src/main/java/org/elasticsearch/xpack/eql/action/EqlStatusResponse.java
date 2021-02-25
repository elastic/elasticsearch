/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.eql.action;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xpack.core.search.action.SearchStatusResponse;
import org.elasticsearch.xpack.eql.async.StoredAsyncResponse;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.rest.RestStatus.OK;

/**
 * A response for eql search status request
 */
public class EqlStatusResponse extends ActionResponse implements SearchStatusResponse, StatusToXContentObject {
    private final String id;
    private final boolean isRunning;
    private final boolean isPartial;
    private final Long startTimeMillis;
    private final long expirationTimeMillis;
    private final RestStatus completionStatus;

    public EqlStatusResponse(String id,
            boolean isRunning,
            boolean isPartial,
            Long startTimeMillis,
            long expirationTimeMillis,
            RestStatus completionStatus) {
        this.id = id;
        this.isRunning = isRunning;
        this.isPartial = isPartial;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.completionStatus = completionStatus;
    }

    /**
     * Get status from the stored eql search response
     * @param storedResponse - a response from a stored search
     * @param expirationTimeMillis – expiration time in milliseconds
     * @param id – encoded async search id
     * @return a status response
     */
    public static EqlStatusResponse getStatusFromStoredSearch(StoredAsyncResponse<EqlSearchResponse> storedResponse,
            long expirationTimeMillis, String id) {
        EqlSearchResponse searchResponse = storedResponse.getResponse();
        if (searchResponse != null) {
            assert searchResponse.isRunning() == false : "Stored eql search response must have a completed status!";
            return new EqlStatusResponse(
                searchResponse.id(),
                false,
                searchResponse.isPartial(),
                null,  // we dont' store in the index start time for completed response
                expirationTimeMillis,
                RestStatus.OK
            );
        } else {
            Exception exc = storedResponse.getException();
            assert exc != null : "Stored eql response must either have a search response or an exception!";
            return new EqlStatusResponse(
                id,
                false,
                false,
                null, // we dont' store in the index start time for completed response
                expirationTimeMillis,
                ExceptionsHelper.status(exc)
            );
        }
    }

    public EqlStatusResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.isRunning = in.readBoolean();
        this.isPartial = in.readBoolean();
        this.startTimeMillis = in.readOptionalLong();
        this.expirationTimeMillis = in.readLong();
        this.completionStatus = (this.isRunning == false) ? RestStatus.readFrom(in) : null;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(isRunning);
        out.writeBoolean(isPartial);
        out.writeOptionalLong(startTimeMillis);
        out.writeLong(expirationTimeMillis);
        if (isRunning == false) {
            RestStatus.writeTo(out, completionStatus);
        }
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
        builder.field("is_partial", isPartial);
        if (startTimeMillis != null) { // start time is available only for a running eql search
            builder.timeField("start_time_in_millis", "start_time", startTimeMillis);
        }
        builder.timeField("expiration_time_in_millis", "expiration_time", expirationTimeMillis);
        if (isRunning == false) { // completion status is available only for a completed eql search
            builder.field("completion_status", completionStatus.getStatus());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        EqlStatusResponse other = (EqlStatusResponse) obj;
        return id.equals(other.id)
            && isRunning == other.isRunning
            && isPartial == other.isPartial
            && Objects.equals(startTimeMillis, other.startTimeMillis)
            && expirationTimeMillis == other.expirationTimeMillis
            && Objects.equals(completionStatus, other.completionStatus);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, isRunning, isPartial, startTimeMillis, expirationTimeMillis, completionStatus);
    }

    /**
     * Returns the id of the eql search status request.
     */
    public String getId() {
        return id;
    }

    /**
     * Returns {@code true} if the eql search is still running in the cluster,
     * or {@code false} if the search has been completed.
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * Returns {@code true} if the eql search results are partial.
     * This could be either because eql search hasn't finished yet,
     * or if it finished and some shards have failed or timed out.
     */
    public boolean isPartial() {
        return isPartial;
    }

    /**
     * Returns a timestamp when the eql search task started, in milliseconds since epoch.
     * For a completed eql search returns {@code null}, as we don't store start time for completed searches.
     */
    public Long getStartTime() {
        return startTimeMillis;
    }

    /**
     * Returns a timestamp when the eql search will be expired, in milliseconds since epoch.
     */
    @Override
    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    /**
     * For a completed eql search returns the completion status.
     * For a still running eql search returns {@code null}.
     */
    public RestStatus getCompletionStatus() {
        return completionStatus;
    }
}
