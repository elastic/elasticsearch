/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.transport.LeakTracker;

import java.io.IOException;

import static org.elasticsearch.search.fetch.chunk.TransportFetchPhaseCoordinationAction.CHUNKED_FETCH_PHASE;

public final class FetchSearchResult extends SearchPhaseResult {

    private SearchHits hits;
    // client side counter
    private transient int counter;

    private ProfileResult profileResult;

    /**
     * Sequence number of the first hit in the last chunk (embedded in this result).
     * Used by the coordinator to maintain correct ordering when processing the last chunk.
     * Value of -1 indicates no last chunk or sequence tracking not applicable.
     */
    private long lastChunkSequenceStart = -1;

    private final RefCounted refCounted = LeakTracker.wrap(new SimpleRefCounted());

    public FetchSearchResult() {}

    public FetchSearchResult(ShardSearchContextId id, SearchShardTarget shardTarget) {
        this.contextId = id;
        setSearchShardTarget(shardTarget);
    }

    public FetchSearchResult(StreamInput in) throws IOException {
        contextId = new ShardSearchContextId(in);
        hits = SearchHits.readFrom(in, true);
        profileResult = in.readOptionalWriteable(ProfileResult::new);
        if (in.getTransportVersion().onOrAfter(CHUNKED_FETCH_PHASE)) {
            lastChunkSequenceStart = in.readLong();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert hasReferences();
        contextId.writeTo(out);
        hits.writeTo(out);
        out.writeOptionalWriteable(profileResult);
        if (out.getTransportVersion().onOrAfter(CHUNKED_FETCH_PHASE)) {
            out.writeLong(lastChunkSequenceStart);
        }
    }

    @Override
    public FetchSearchResult fetchResult() {
        return this;
    }

    public void shardResult(SearchHits hits, ProfileResult profileResult) {
        assert assertNoSearchTarget(hits);
        assert hasReferences();
        var existing = this.hits;
        if (existing != null) {
            existing.decRef();
        }
        this.hits = hits;
        assert this.profileResult == null;
        this.profileResult = profileResult;
    }

    private static boolean assertNoSearchTarget(SearchHits hits) {
        for (SearchHit hit : hits.getHits()) {
            assert hit.getShard() == null : "expected null but got: " + hit.getShard();
        }
        return true;
    }

    public SearchHits hits() {
        assert hasReferences();
        return hits;
    }

    public FetchSearchResult initCounter() {
        counter = 0;
        return this;
    }

    public int counterGetAndIncrement() {
        return counter++;
    }

    public ProfileResult profileResult() {
        return profileResult;
    }

    @Override
    public void incRef() {
        refCounted.incRef();
    }

    @Override
    public boolean tryIncRef() {
        return refCounted.tryIncRef();
    }

    @Override
    public boolean decRef() {
        if (refCounted.decRef()) {
            deallocate();
            return true;
        }
        return false;
    }

    private void deallocate() {
        if (hits != null) {
            hits.decRef();
            hits = null;
        }
    }

    @Override
    public boolean hasReferences() {
        return refCounted.hasReferences();
    }

    /**
     * Sets the sequence start for the last chunk embedded in this result.
     * Called on the data node after iterating fetch phase results.
     *
     * @param sequenceStart the sequence number of the first hit in the last chunk
     */
    public void setLastChunkSequenceStart(long sequenceStart) {
        this.lastChunkSequenceStart = sequenceStart;
    }

    /**
     * Gets the sequence start for the last chunk embedded in this result.
     * Used by the coordinator to properly order last chunk hits with other chunks.
     *
     * @return the sequence number of the first hit in the last chunk, or -1 if not set
     */
    public long getLastChunkSequenceStart() {
        return lastChunkSequenceStart;
    }
}
