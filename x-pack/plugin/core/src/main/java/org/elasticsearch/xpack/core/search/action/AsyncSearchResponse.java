/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.StatusToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;

import static org.elasticsearch.rest.RestStatus.NOT_MODIFIED;
import static org.elasticsearch.rest.RestStatus.PARTIAL_CONTENT;

/**
 * A response of a search progress request that contains a non-null {@link PartialSearchResponse} if the request is running or has failed
 * before completion, or a final {@link SearchResponse} if the request succeeded.
 */
public class AsyncSearchResponse extends ActionResponse implements StatusToXContentObject {
    private final String id;
    private final int version;
    private final SearchResponse finalResponse;
    private final PartialSearchResponse partialResponse;
    private final ElasticsearchException failure;

    private final long timestamp;
    private final boolean isRunning;

    public AsyncSearchResponse(String id, int version, boolean isRunning) {
        this(id, null, null, null, version, isRunning);
    }

    public AsyncSearchResponse(String id, SearchResponse response, int version, boolean isRunning) {
        this(id, null, response, null, version, isRunning);
    }

    public AsyncSearchResponse(String id, PartialSearchResponse response, int version, boolean isRunning) {
        this(id, response, null, null, version, isRunning);
    }

    public AsyncSearchResponse(String id, PartialSearchResponse response, ElasticsearchException failure, int version, boolean isRunning) {
        this(id, response, null, failure, version, isRunning);
    }

    private AsyncSearchResponse(String id,
                                PartialSearchResponse partialResponse,
                                SearchResponse finalResponse,
                                ElasticsearchException failure,
                                int version,
                                boolean isRunning) {
        this.id = id;
        this.version = version;
        this.partialResponse = partialResponse;
        this.failure = failure;
        this.finalResponse = finalResponse != null ? wrapFinalResponse(finalResponse) : null;
        this.timestamp = System.currentTimeMillis();
        this.isRunning = isRunning;
    }

    public AsyncSearchResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.version = in.readVInt();
        this.partialResponse = in.readOptionalWriteable(PartialSearchResponse::new);
        this.failure = in.readOptionalWriteable(ElasticsearchException::new);
        this.finalResponse = in.readOptionalWriteable(SearchResponse::new);
        this.timestamp = in.readLong();
        this.isRunning = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeVInt(version);
        out.writeOptionalWriteable(partialResponse);
        out.writeOptionalWriteable(failure);
        out.writeOptionalWriteable(finalResponse);
        out.writeLong(timestamp);
        out.writeBoolean(isRunning);
    }

    /**
     * Return the id of the search progress request.
     */
    public String id() {
        return id;
    }

    /**
     * Return the version of this response.
     */
    public int getVersion() {
        return version;
    }

    /**
     * Return <code>true</code> if the request has failed.
     */
    public boolean hasFailed() {
        return failure != null;
    }

    /**
     * Return <code>true</code> if a partial response is available.
     */
    public boolean hasPartialResponse() {
        return partialResponse != null;
    }

    /**
     * Return <code>true</code> if the final response is available.
     */
    public boolean isFinalResponse() {
        return finalResponse != null;
    }

    /**
     * The final {@link SearchResponse} if the request has completed, or <code>null</code> if the
     * request is running or failed.
     */
    public SearchResponse getSearchResponse() {
        return finalResponse;
    }

    /**
     * The {@link PartialSearchResponse} if the request is running or failed, or <code>null</code>
     * if the request has completed.
     */
    public PartialSearchResponse getPartialResponse() {
        return partialResponse;
    }

    /**
     * The failure that occurred during the search.
     */
    public ElasticsearchException getFailure() {
        return failure;
    }

    /**
     * When this response was created.
     */
    public long getTimestamp() {
        return timestamp;
    }

    /**
     * Whether the search is still running in the cluster.
     */
    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public RestStatus status() {
        if (finalResponse == null && partialResponse == null) {
            return failure != null ? failure.status() : NOT_MODIFIED;
        } else if (finalResponse == null) {
            return failure != null ? failure.status() : PARTIAL_CONTENT;
        } else {
            return finalResponse.status();
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("version", version);
        builder.field("is_running", isRunning);
        builder.field("timestamp", timestamp);

        if (partialResponse != null) {
            builder.field("response", partialResponse);
        } else if (finalResponse != null) {
            builder.field("response", finalResponse);
        }
        if (failure != null) {
            builder.startObject("failure");
            failure.toXContent(builder, params);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private static SearchResponse wrapFinalResponse(SearchResponse response) {
        // Adds a partial flag set to false in the xcontent serialization
        return new SearchResponse(response) {
            @Override
            public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
                builder.field("is_partial", false);
                return super.innerToXContent(builder, params);
            }
        };
    }
}
