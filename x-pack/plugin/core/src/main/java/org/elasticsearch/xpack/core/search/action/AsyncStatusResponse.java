/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.search.action;

import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.RestActions;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * A response of an async search status request.
 */
public class AsyncStatusResponse extends ActionResponse implements SearchStatusResponse, ToXContentObject {
    private final String id;
    private final boolean isRunning;
    private final boolean isPartial;
    private final long startTimeMillis;
    private final long expirationTimeMillis;
    private final Long completionTimeMillis;
    private final int totalShards;
    private final int successfulShards;
    private final int skippedShards;
    private final int failedShards;
    private final RestStatus completionStatus;

    // Non-null for cross cluster searches
    @Nullable
    private final SearchResponse.Clusters clusters;

    public AsyncStatusResponse(
        String id,
        boolean isRunning,
        boolean isPartial,
        long startTimeMillis,
        long expirationTimeMillis,
        Long completionTimeMillis,
        int totalShards,
        int successfulShards,
        int skippedShards,
        int failedShards,
        RestStatus completionStatus,
        SearchResponse.Clusters clusters
    ) {
        this.id = id;
        this.isRunning = isRunning;
        this.isPartial = isPartial;
        this.startTimeMillis = startTimeMillis;
        this.expirationTimeMillis = expirationTimeMillis;
        this.completionTimeMillis = completionTimeMillis;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.skippedShards = skippedShards;
        this.failedShards = failedShards;
        this.completionStatus = completionStatus;
        this.clusters = clusters;
    }

    /**
     * Get status from the stored async search response
     * @param asyncSearchResponse stored async search response
     * @param expirationTimeMillis – expiration time in milliseconds
     * @param id – encoded async search id
     * @return status response
     */
    public static AsyncStatusResponse getStatusFromStoredSearch(
        AsyncSearchResponse asyncSearchResponse,
        long expirationTimeMillis,
        String id
    ) {
        int totalShards = 0;
        int successfulShards = 0;
        int skippedShards = 0;
        int failedShards = 0;
        SearchResponse.Clusters clusters = null;
        RestStatus completionStatus = null;
        SearchResponse searchResponse = asyncSearchResponse.getSearchResponse();
        if (searchResponse != null) {
            totalShards = searchResponse.getTotalShards();
            successfulShards = searchResponse.getSuccessfulShards();
            skippedShards = searchResponse.getSkippedShards();
            failedShards = searchResponse.getFailedShards();
            if (searchResponse.getClusters() != null && searchResponse.getClusters() != SearchResponse.Clusters.EMPTY) {
                clusters = searchResponse.getClusters();
            }
        }
        if (asyncSearchResponse.isRunning() == false) {
            Exception failure = asyncSearchResponse.getFailure();
            if (failure != null) {
                completionStatus = ExceptionsHelper.status(ExceptionsHelper.unwrapCause(failure));
            } else if (searchResponse != null) {
                completionStatus = searchResponse.status();
            } else {
                throw new IllegalStateException("Unable to retrieve async_search status. No SearchResponse or Exception could be found.");
            }
        }
        return new AsyncStatusResponse(
            id,
            asyncSearchResponse.isRunning(),
            asyncSearchResponse.isPartial(),
            asyncSearchResponse.getStartTime(),
            expirationTimeMillis,
            asyncSearchResponse.getCompletionTime(),
            totalShards,
            successfulShards,
            skippedShards,
            failedShards,
            completionStatus,
            clusters
        );
    }

    public AsyncStatusResponse(StreamInput in) throws IOException {
        this.id = in.readString();
        this.isRunning = in.readBoolean();
        this.isPartial = in.readBoolean();
        this.startTimeMillis = in.readLong();
        this.expirationTimeMillis = in.readLong();
        this.totalShards = in.readVInt();
        this.successfulShards = in.readVInt();
        this.skippedShards = in.readVInt();
        this.failedShards = in.readVInt();
        this.completionStatus = (this.isRunning == false) ? RestStatus.readFrom(in) : null;
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            this.clusters = in.readOptionalWriteable(SearchResponse.Clusters::new);
        } else {
            this.clusters = null;
        }
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
            this.completionTimeMillis = in.readOptionalVLong();
        } else {
            this.completionTimeMillis = null;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(id);
        out.writeBoolean(isRunning);
        out.writeBoolean(isPartial);
        out.writeLong(startTimeMillis);
        out.writeLong(expirationTimeMillis);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(skippedShards);
        out.writeVInt(failedShards);
        if (isRunning == false) {
            RestStatus.writeTo(out, completionStatus);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_9_X)) {
            // optional since only CCS uses is; it is null for local-only searches
            out.writeOptionalWriteable(clusters);
        }
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
            out.writeOptionalVLong(completionTimeMillis);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", id);
        builder.field("is_running", isRunning);
        builder.field("is_partial", isPartial);
        builder.timestampFieldsFromUnixEpochMillis("start_time_in_millis", "start_time", startTimeMillis);
        builder.timestampFieldsFromUnixEpochMillis("expiration_time_in_millis", "expiration_time", expirationTimeMillis);
        if (completionTimeMillis != null) {
            builder.timestampFieldsFromUnixEpochMillis("completion_time_in_millis", "completion_time", completionTimeMillis);
        }
        RestActions.buildBroadcastShardsHeader(builder, params, totalShards, successfulShards, skippedShards, failedShards, null);
        if (clusters != null) {
            builder = clusters.toXContent(builder, params);
        }
        if (isRunning == false) { // completion status information is only available for a completed search
            builder.field("completion_status", completionStatus.getStatus());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        AsyncStatusResponse other = (AsyncStatusResponse) obj;
        return id.equals(other.id)
            && isRunning == other.isRunning
            && isPartial == other.isPartial
            && startTimeMillis == other.startTimeMillis
            && expirationTimeMillis == other.expirationTimeMillis
            && totalShards == other.totalShards
            && successfulShards == other.successfulShards
            && skippedShards == other.skippedShards
            && failedShards == other.failedShards
            && Objects.equals(completionStatus, other.completionStatus)
            && Objects.equals(clusters, other.clusters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            isRunning,
            isPartial,
            startTimeMillis,
            expirationTimeMillis,
            totalShards,
            successfulShards,
            skippedShards,
            failedShards,
            completionStatus,
            clusters
        );
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
     * Returns {@code true} if the search results are partial.
     * This could be either because async search hasn't finished yet,
     * or if it finished and some shards have failed.
     */
    public boolean isPartial() {
        return isPartial;
    }

    /**
     * Returns a timestamp when the search tasks started, in milliseconds since epoch.
     */
    public long getStartTime() {
        return startTimeMillis;
    }

    /**
     * Returns a timestamp when the search will be expired, in milliseconds since epoch.
     */
    @Override
    public long getExpirationTime() {
        return expirationTimeMillis;
    }

    /**
     * @return completion_time_in_millis if set, otherwise null
     */
    public Long getCompletionTime() {
        return completionTimeMillis;
    }

    /**
     * Returns the total number of shards the search is executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * Returns the number of successful shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * Returns the number of skipped shards due to pre-filtering.
     */
    public int getSkippedShards() {
        return skippedShards;
    }

    /**
     * Returns the number of failed shards the search was executed on.
     */
    public int getFailedShards() {
        return failedShards;
    }

    /**
     * For a completed async search returns the completion status.
     * For a still running async search returns {@code null}.
     */
    public RestStatus getCompletionStatus() {
        return completionStatus;
    }

    /**
     * @return For CCS, clusters object that has information about the clustes being searched, such as total count
     * successful count and skipped. Will be null for local-only searches.
     */
    public SearchResponse.Clusters getClusters() {
        return clusters;
    }
}
