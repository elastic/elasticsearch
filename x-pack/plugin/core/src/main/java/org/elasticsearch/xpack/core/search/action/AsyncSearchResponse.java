/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.OK;

/**
 * A response of an async search request.
 */
public class AsyncSearchResponse extends ActionResponse implements StatusToXContentObject {
    @Nullable
    private final String id;
    private final int version;
    private final SearchResponse searchResponse;
    private final ElasticsearchException error;
    private final boolean isRunning;
    private final boolean isPartial;

    private final long startTimeMillis;
    private final long expirationTimeMillis;

    /**
     * Creates an {@link AsyncSearchResponse} with meta-information only (not-modified).
     */
    public AsyncSearchResponse(String id,
                               int version,
                               boolean isPartial,
                               boolean isRunning,
                               long startTimeMillis,
                               long expirationTimeMillis) {
        this(id, version, null, null, isPartial, isRunning, startTimeMillis, expirationTimeMillis);
    }

    /**
     * Creates a new {@link AsyncSearchResponse}
     *
     * @param id The id of the search for further retrieval, <code>null</code> if not stored.
     * @param version The version number of this response.
     * @param searchResponse The actual search response.
     * @param error The error if the search failed, <code>null</code> if the search is running
     *                or has completed without failure.
     * @param isPartial Whether the <code>searchResponse</code> contains partial results.
     * @param isRunning Whether the search is running in the cluster.
     * @param startTimeMillis The start date of the search in milliseconds since epoch.
     */
    public AsyncSearchResponse(String id,
                               int version,
                               SearchResponse searchResponse,
                               ElasticsearchException error,
                               boolean isPartial,
                               boolean isRunning,
                               long startTimeMillis,
                               long expirationTimeMillis) {
        this.id = id;
        this.version = version;
        this.error = error;
        this.searchResponse = searchResponse;
        this.isPartial = isPartial;
        this.isRunning = isRunning;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
    }

    public AsyncSearchResponse(StreamInput in) throws IOException {
        this.id = in.readOptionalString();
        this.version = in.readVInt();
        this.error = in.readOptionalWriteable(ElasticsearchException::new);
        this.searchResponse = in.readOptionalWriteable(SearchResponse::new);
        this.isPartial = in.readBoolean();
        this.isRunning = in.readBoolean();
        this.startTimeMillis = in.readLong();
        this.expirationTimeMillis = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalString(id);
        out.writeVInt(version);
        out.writeOptionalWriteable(error);
        out.writeOptionalWriteable(searchResponse);
        out.writeBoolean(isPartial);
        out.writeBoolean(isRunning);
        out.writeLong(startTimeMillis);
        out.writeLong(expirationTimeMillis);
    }

    public AsyncSearchResponse clone(String id) {
        return new AsyncSearchResponse(id, version, searchResponse, error, isPartial, isRunning, startTimeMillis, expirationTimeMillis);
    }

    /**
     * Returns the id of the async search request or null if the response is not stored in the cluster.
     */
    @Nullable
    public String getId() {
        return id;
    }

    /**
     * Returns the version of this response.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Returns the current {@link SearchResponse} or <code>null</code> if not available.
     *
     * See {@link #isPartial()} to determine whether the response contains partial or complete
     * results.
     */
    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    /**
     * Returns the failure reason or null if the query is running or has completed normally.
     */
    public ElasticsearchException getFailure() {
        return error;
    }

    /**
     * Returns <code>true</code> if the {@link SearchResponse} contains partial
     * results computed from a subset of the total shards.
     */
    public boolean isPartial() {
        return isPartial;
    }

    /**
     * Whether the search is still running in the cluster.
     *
     * A value of <code>false</code> indicates that the response is final
     * even if {@link #isPartial()} returns <code>true</code>. In such case,
     * the partial response represents the status of the search before a
     * non-recoverable failure.
     */
    public boolean isRunning() {
        return isRunning;
    }

    /**
     * When this response was created as a timestamp in milliseconds since epoch.
     */
    public long getStartTime() {
        return startTimeMillis;
    }

    /**
     * When this response will expired as a timestamp in milliseconds since epoch.
     */
    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    @Override
    public RestStatus status() {
        if (searchResponse == null || isPartial) {
            // shard failures are not considered fatal for partial results so
            // we return OK until we get the final response even if we don't have
            // a single successful shard.
            return error != null ? error.status() : OK;
        } else {
            return searchResponse.status();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (id != null) {
            builder.field("id", id);
        }
        builder.field("version", version);
        builder.field("is_partial", isPartial);
        builder.field("is_running", isRunning);
        builder.field("start_time_in_millis", startTimeMillis);
        builder.field("expiration_time_in_millis", expirationTimeMillis);

        builder.field("response", searchResponse);
        if (error != null) {
            builder.startObject("error");
            error.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
