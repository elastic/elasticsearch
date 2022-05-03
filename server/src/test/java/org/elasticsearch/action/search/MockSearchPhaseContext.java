/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.transport.Transport;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * SearchPhaseContext for tests
 */
public final class MockSearchPhaseContext implements SearchPhaseContext {
    private static final Logger logger = LogManager.getLogger(MockSearchPhaseContext.class);
    final AtomicReference<Throwable> phaseFailure = new AtomicReference<>();
    final int numShards;
    final AtomicInteger numSuccess;
    final List<ShardSearchFailure> failures = Collections.synchronizedList(new ArrayList<>());
    SearchTransportService searchTransport;
    final Set<ShardSearchContextId> releasedSearchContexts = new HashSet<>();
    final SearchRequest searchRequest = new SearchRequest();
    final AtomicReference<SearchResponse> searchResponse = new AtomicReference<>();

    public MockSearchPhaseContext(int numShards) {
        this.numShards = numShards;
        numSuccess = new AtomicInteger(numShards);
    }

    public void assertNoFailure() {
        if (phaseFailure.get() != null) {
            throw new AssertionError(phaseFailure.get());
        }
    }

    @Override
    public int getNumShards() {
        return numShards;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public SearchTask getTask() {
        return new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
    }

    @Override
    public SearchRequest getRequest() {
        return searchRequest;
    }

    @Override
    public OriginalIndices getOriginalIndices(int shardIndex) {
        return new OriginalIndices(searchRequest.indices(), searchRequest.indicesOptions());
    }

    @Override
    public void sendSearchResponse(InternalSearchResponse internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        String scrollId = getRequest().scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
        String searchContextId = getRequest().pointInTimeBuilder() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
        searchResponse.set(
            new SearchResponse(
                internalSearchResponse,
                scrollId,
                numShards,
                numSuccess.get(),
                0,
                0,
                failures.toArray(ShardSearchFailure.EMPTY_ARRAY),
                SearchResponse.Clusters.EMPTY,
                searchContextId
            )
        );
    }

    @Override
    public void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
        phaseFailure.set(cause);
    }

    @Override
    public void onShardFailure(int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        failures.add(new ShardSearchFailure(e, shardTarget));
        numSuccess.decrementAndGet();
    }

    @Override
    public Transport.Connection getConnection(String clusterAlias, String nodeId) {
        return null; // null is ok here for this test
    }

    @Override
    public SearchTransportService getSearchTransport() {
        Assert.assertNotNull(searchTransport);
        return searchTransport;
    }

    @Override
    public ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt, int shardIndex) {
        Assert.fail("should not be called");
        return null;
    }

    @Override
    public void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {
        try {
            nextPhase.run();
        } catch (Exception e) {
            onPhaseFailure(nextPhase, "phase failed", e);
        }
    }

    @Override
    public void addReleasable(Releasable releasable) {
        // Noop
    }

    @Override
    public void execute(Runnable command) {
        command.run();
    }

    @Override
    public void onFailure(Exception e) {
        Assert.fail("should not be called");
    }

    @Override
    public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection, OriginalIndices originalIndices) {
        releasedSearchContexts.add(contextId);
    }

    @Override
    public boolean isPartOfPointInTime(ShardSearchContextId contextId) {
        return false;
    }
}
