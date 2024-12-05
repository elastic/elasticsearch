/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.transport.Transport;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

/**
 * SearchPhaseContext for tests
 */
public final class MockSearchPhaseContext extends AbstractSearchAsyncAction<SearchPhaseResult> {
    private static final Logger logger = LogManager.getLogger(MockSearchPhaseContext.class);
    public final AtomicReference<Throwable> phaseFailure = new AtomicReference<>();
    final int numShards;
    final AtomicInteger numSuccess;
    public final List<ShardSearchFailure> failures = Collections.synchronizedList(new ArrayList<>());
    SearchTransportService searchTransport;
    final Set<ShardSearchContextId> releasedSearchContexts = new HashSet<>();
    public final AtomicReference<SearchResponse> searchResponse = new AtomicReference<>();

    public MockSearchPhaseContext(int numShards) {
        super(
            "mock",
            logger,
            new NamedWriteableRegistry(List.of()),
            mock(SearchTransportService.class),
            (clusterAlias, nodeId) -> null,
            null,
            null,
            Runnable::run,
            new SearchRequest(),
            ActionListener.noop(),
            new GroupShardsIterator<SearchShardIterator>(List.of()),
            null,
            ClusterState.EMPTY_STATE,
            new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap()),
            new ArraySearchPhaseResults<>(numShards),
            5,
            null
        );
        this.numShards = numShards;
        numSuccess = new AtomicInteger(numShards);
    }

    public void assertNoFailure() {
        if (phaseFailure.get() != null) {
            throw new AssertionError(phaseFailure.get());
        }
    }

    @Override
    public OriginalIndices getOriginalIndices(int shardIndex) {
        var searchRequest = getRequest();
        return new OriginalIndices(searchRequest.indices(), searchRequest.indicesOptions());
    }

    @Override
    public void sendSearchResponse(SearchResponseSections internalSearchResponse, AtomicArray<SearchPhaseResult> queryResults) {
        String scrollId = getRequest().scroll() != null ? TransportSearchHelper.buildScrollId(queryResults) : null;
        BytesReference searchContextId = getRequest().pointInTimeBuilder() != null
            ? new BytesArray(TransportSearchHelper.buildScrollId(queryResults))
            : null;
        var existing = searchResponse.getAndSet(
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
        Releasables.close(releasables);
        releasables.clear();
        if (existing != null) {
            existing.decRef();
        }
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
    protected SearchPhase getNextPhase() {
        return null;
    }

    @Override
    public SearchTransportService getSearchTransport() {
        Assert.assertNotNull(searchTransport);
        return searchTransport;
    }

    @Override
    public void executeNextPhase(SearchPhase currentPhase, Supplier<SearchPhase> nextPhaseSupplier) {
        var nextPhase = nextPhaseSupplier.get();
        try {
            nextPhase.run();
        } catch (Exception e) {
            onPhaseFailure(nextPhase, "phase failed", e);
        }
    }

    @Override
    protected void executePhaseOnShard(
        SearchShardIterator shardIt,
        Transport.Connection shard,
        SearchActionListener<SearchPhaseResult> listener
    ) {
        onShardResult(new SearchPhaseResult() {
        }, shardIt);
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
