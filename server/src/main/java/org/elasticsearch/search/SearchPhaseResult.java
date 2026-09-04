/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.store.DirectoryMetrics;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.feature.RankFeatureResult;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

/**
 * This class is a base class for all search related results. It contains the shard target it
 * was executed against, a shard index used to reference the result on the coordinating node
 * and a request ID that is used to reference the request context on the executing node. The
 * request ID is particularly important since it is used to reference and maintain a context
 * across search phases to ensure the same point in time snapshot is used for querying and
 * fetching etc.
 */
public abstract class SearchPhaseResult extends TransportResponse {

    public static final TransportVersion SEARCH_PHASE_BYTES_READ = TransportVersion.fromName("search_phase_bytes_read");

    private SearchShardTarget searchShardTarget;
    private int shardIndex = -1;
    protected ShardSearchContextId contextId;
    private ShardSearchRequest shardSearchRequest;
    private RescoreDocIds rescoreDocIds = RescoreDocIds.EMPTY;
    private DirectoryMetrics directoryMetrics = DirectoryMetrics.EMPTY;

    protected SearchPhaseResult() {}

    /**
     * Specifies whether the specific search phase results are associated with an opened SearchContext on the shards that
     * executed the request.
     */
    public boolean hasSearchContext() {
        return false;
    }

    /**
     * Returns the search context ID that is used to reference the search context on the executing node
     * or <code>null</code> if no context was created.
     */
    @Nullable
    public ShardSearchContextId getContextId() {
        return contextId;
    }

    /**
     * Null out the context id and request tracked in this instance. This is used to mark shards for which merging results on the data node
     * made it clear that their search context won't be used in the fetch phase.
     */
    public void clearContextId() {
        this.shardSearchRequest = null;
        this.contextId = null;
    }

    /**
     * Returns the shard index in the context of the currently executing search request that is
     * used for accounting on the coordinating node
     */
    public int getShardIndex() {
        assert shardIndex != -1 : "shardIndex is not set";
        return shardIndex;
    }

    public SearchShardTarget getSearchShardTarget() {
        return searchShardTarget;
    }

    public void setSearchShardTarget(SearchShardTarget shardTarget) {
        this.searchShardTarget = shardTarget;
    }

    public void setShardIndex(int shardIndex) {
        assert shardIndex >= 0 : "shardIndex must be >= 0 but was: " + shardIndex;
        this.shardIndex = shardIndex;
    }

    /**
     * Returns the query result iff it's included in this response otherwise <code>null</code>
     */
    public QuerySearchResult queryResult() {
        return null;
    }

    /**
     * Returns the rank feature result iff it's included in this response otherwise <code>null</code>
     */
    public RankFeatureResult rankFeatureResult() {
        return null;
    }

    /**
     * Returns the fetch result iff it's included in this response otherwise <code>null</code>
     */
    public FetchSearchResult fetchResult() {
        return null;
    }

    /**
     * Returns the directory-level metrics captured while executing this phase on a data node, or
     * {@link DirectoryMetrics#EMPTY} when none were recorded (e.g. can-match, open-PIT, or any phase
     * result that never opened a reader). The value is never {@code null}.
     */
    public DirectoryMetrics getDirectoryMetrics() {
        return directoryMetrics;
    }

    /**
     * Set directory-level metrics for this phase result. Must not be null. This is invoked on the
     * data node once a phase finishes (with the resolved per-phase metrics) and again on the
     * coordinating node when the result is deserialized.
     */
    public void setDirectoryMetrics(DirectoryMetrics directoryMetrics) {
        this.directoryMetrics = directoryMetrics;
    }

    /**
     * Reads the directory metrics from the stream when the negotiated transport version supports it.
     * Subclasses that serialize metrics call this at the end of their stream-input constructor so the
     * feature-flag gating lives in a single place.
     */
    protected final void readDirectoryMetrics(StreamInput in) throws IOException {
        if (in.getTransportVersion().supports(SEARCH_PHASE_BYTES_READ)) {
            setDirectoryMetrics(new DirectoryMetrics(in));
        }
    }

    /**
     * Writes the directory metrics to the stream when the negotiated transport version supports it.
     * Counterpart to {@link #readDirectoryMetrics(StreamInput)}; subclasses call this at the end of
     * their {@link #writeTo(StreamOutput)} implementation.
     */
    protected final void writeDirectoryMetrics(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(SEARCH_PHASE_BYTES_READ)) {
            getDirectoryMetrics().writeTo(out);
        }
    }

    protected final void setShardSearchRequest(ShardSearchRequest shardSearchRequest) {
        // only include the SSR in the result if the coordinator cannot recreate it from its own data
        if (shardSearchRequest != null && shardSearchRequest.enableShardResultsSkipRequest() == false) {
            this.shardSearchRequest = shardSearchRequest;
        }
    }

    protected final void readShardSearchRequest(StreamInput in) throws IOException {
        this.shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
    }

    protected final void writeShardSearchRequest(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(shardSearchRequest);
    }

    public final RescoreDocIds getRescoreDocIds() {
        return rescoreDocIds;
    }

    public final void setRescoreDocIds(RescoreDocIds rescoreDocIds) {
        this.rescoreDocIds = rescoreDocIds;
    }

    /**
     * Writes this result to the stream output. Concrete implementation is up to each subclass.
     */
    @Override
    public abstract void writeTo(StreamOutput out) throws IOException;
}
