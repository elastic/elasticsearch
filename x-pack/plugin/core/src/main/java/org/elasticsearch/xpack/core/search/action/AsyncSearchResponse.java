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
    private final ElasticsearchException failure;
    private final boolean isRunning;
    private final boolean isPartial;

    private final long startTimeMillis;
    private long expirationTimeMillis = -1;

    /**
     * Creates an {@link AsyncSearchResponse} with meta informations that omits
     * the search response.
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
     * @param id The id of the search for further retrieval, <code>null</code> if not stored.
     * @param version The version number of this response.
     * @param searchResponse The actual search response.
     * @param failure The actual failure if the search failed, <code>null</code> if the search is running
     *                or completed without failure.
     * @param isPartial Whether the <code>searchResponse</code> contains partial results.
     * @param isRunning Whether the search is running in the cluster.
     * @param startTimeMillis The start date of the search in milliseconds since epoch.
     */
    public AsyncSearchResponse(String id,
                               int version,
                               SearchResponse searchResponse,
                               ElasticsearchException failure,
                               boolean isPartial,
                               boolean isRunning,
                               long startTimeMillis,
                               long expirationTimeMillis) {
        this.id = id;
        this.version = version;
        this.failure = failure;
        this.searchResponse = searchResponse;
        this.isPartial = isPartial;
        this.isRunning = isRunning;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
    }

    public AsyncSearchResponse(StreamInput in) throws IOException {
        this.id = in.readOptionalString();
        this.version = in.readVInt();
        this.failure = in.readOptionalWriteable(ElasticsearchException::new);
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
        out.writeOptionalWriteable(failure);
        out.writeOptionalWriteable(searchResponse);
        out.writeBoolean(isPartial);
        out.writeBoolean(isRunning);
        out.writeLong(startTimeMillis);
        out.writeLong(expirationTimeMillis);
    }

    public AsyncSearchResponse clone(String id) {
        return new AsyncSearchResponse(id, version, searchResponse, failure, isPartial, isRunning, startTimeMillis, expirationTimeMillis);
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
     * See {@link #isPartial()} to determine whether the response contains partial or complete
     * results.
     */
    public SearchResponse getSearchResponse() {
        return searchResponse;
    }

    /**
     * Returns the failure reason or null if the query is running or completed normally.
     */
    public ElasticsearchException getFailure() {
        return failure;
    }

    /**
     * Returns <code>true</code> if the {@link SearchResponse} contains partial
     * results computed from a subset of the total shards.
     */
    public boolean isPartial() {
        return isPartial;
    }

    /**
     * When this response was created as a timestamp in milliseconds since epoch.
     */
    public long getStartTime() {
        return startTimeMillis;
    }

    /**
     * Whether the search is still running in the cluster.
     * A value of <code>false</code> indicates that the response is final even
     * if it contains partial results. In such case the failure should indicate
     * why the request could not finish and the search response represents the
     * last partial results before the failure.
     */
    public boolean isRunning() {
        return isRunning;
    }

    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    @Override
    public RestStatus status() {
        if (searchResponse == null || isPartial) {
            // shard failures are not considered fatal for partial results so
            // we return OK until we get the final response even if we don't have
            // a single successful shard.
            return failure != null ? failure.status() : OK;
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
        if (failure != null) {
            builder.startObject("failure");
            failure.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
