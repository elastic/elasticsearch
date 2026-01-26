/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
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

    private transient long searchHitsSizeBytes = 0L;

    // client side counter
    private transient int counter;

    private ProfileResult profileResult;

    /**
     * Sequence number of the first hit in the last chunk (embedded in this result).
     * Used by the coordinator to maintain correct ordering when processing the last chunk.
     * Value of -1 indicates no last chunk or sequence tracking not applicable.
     */
    private long lastChunkSequenceStart = -1;

    /**
     *  Raw serialized bytes of the last chunk's hits.
     */
    private BytesReference lastChunkBytes;

    /**
     * Number of hits in the last chunk bytes.
     * Used by the coordinator to know how many hits to deserialize from lastChunkBytes.
     */
    private int lastChunkHitCount;

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

        if (in.getTransportVersion().supports(CHUNKED_FETCH_PHASE)) {
            lastChunkSequenceStart = in.readLong();
            lastChunkHitCount = in.readInt();
            if (lastChunkHitCount > 0) {
                lastChunkBytes = in.readReleasableBytesReference();
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert hasReferences();
        contextId.writeTo(out);
        hits.writeTo(out);
        out.writeOptionalWriteable(profileResult);

        if (out.getTransportVersion().supports(CHUNKED_FETCH_PHASE)) {
            out.writeLong(lastChunkSequenceStart);
            out.writeInt(lastChunkHitCount);
            if (lastChunkHitCount > 0 && lastChunkBytes != null) {
                out.writeBytesReference(lastChunkBytes);
            }
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

    public void setSearchHitsSizeBytes(long bytes) {
        this.searchHitsSizeBytes = bytes;
    }

    public long getSearchHitsSizeBytes() {
        return searchHitsSizeBytes;
    }

    public void releaseCircuitBreakerBytes(CircuitBreaker circuitBreaker) {
        if (searchHitsSizeBytes > 0L) {
            circuitBreaker.addWithoutBreaking(-searchHitsSizeBytes);
            searchHitsSizeBytes = 0L;
        }
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
        releaseLastChunkBytes();
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

    /**
     * Sets the raw bytes of the last chunk.
     * Called on the data node in chunked fetch mode to avoid deserializing
     * large hit data that would cause OOM.
     *
     * <p>Takes ownership of the bytes reference - caller must not release it.
     *
     * @param bytes the serialized hit bytes
     * @param hitCount the number of hits in the bytes
     */
    public void setLastChunkBytes(BytesReference bytes, int hitCount) {
        releaseLastChunkBytes(); // Release any existing bytes
        this.lastChunkBytes = bytes;
        this.lastChunkHitCount = hitCount;
    }

    /**
     * Gets the raw bytes of the last chunk.
     * Used by the coordinator to deserialize and merge with other accumulated chunks.
     *
     * @return the serialized hit bytes, or null if not set
     */
    public BytesReference getLastChunkBytes() {
        return lastChunkBytes;
    }

    /**
     * Gets the number of hits in the last chunk bytes.
     *
     * @return the hit count, or 0 if no last chunk
     */
    public int getLastChunkHitCount() {
        return lastChunkHitCount;
    }

    /**
     * Releases the last chunk bytes if they are releasable.
     */
    private void releaseLastChunkBytes() {
        if (lastChunkBytes instanceof Releasable releasable) {
            Releasables.closeWhileHandlingException(releasable);
        }
        lastChunkBytes = null;
        lastChunkHitCount = 0;
    }
}
