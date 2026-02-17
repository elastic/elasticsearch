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
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.SimpleRefCounted;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.transport.DeferredCircuitBreakerRelease;
import org.elasticsearch.transport.LeakTracker;

import java.io.IOException;

public final class FetchSearchResult extends SearchPhaseResult implements DeferredCircuitBreakerRelease {

    private SearchHits hits;

    /**
     * Releasable that releases circuit breaker bytes when the response is sent over the network.
     * This is set by FetchPhase and taken by ChannelActionListener to pass to TransportChannel.
     */
    private transient Releasable circuitBreakerRelease;

    // client side counter
    private transient int counter;

    private ProfileResult profileResult;

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
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        assert hasReferences();
        contextId.writeTo(out);
        hits.writeTo(out);
        out.writeOptionalWriteable(profileResult);
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

    /**
     * Sets a releasable to be called when the response has been sent over the network.
     * Usually used to defer circuit breaker release until the response bytes are actually sent.
     */
    public void setDeferredCircuitBreakerRelease(Releasable release) {
        this.circuitBreakerRelease = release;
    }

    @Override
    public Releasable takeCircuitBreakerRelease() {
        Releasable release = this.circuitBreakerRelease;
        this.circuitBreakerRelease = null;
        return release;
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
}
