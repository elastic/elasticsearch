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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.action.search.SearchResponseMetrics;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.AliasFilter;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.telemetry.TelemetryProvider;
import org.elasticsearch.transport.CloseableConnection;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.junit.Assert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;

public final class MockSearchDfsQueryThenFetchAsyncAction extends SearchDfsQueryThenFetchAsyncAction {

    private static final Logger logger = LogManager.getLogger(MockSearchDfsQueryThenFetchAsyncAction.class);
    public final AtomicReference<Throwable> phaseFailure = new AtomicReference<>();
    final int numShards;
    final AtomicInteger numSuccess;
    public final List<ShardSearchFailure> failures = Collections.synchronizedList(new ArrayList<>());
    SearchTransportService searchTransport;
    final Set<ShardSearchContextId> releasedSearchContexts = new HashSet<>();

    public MockSearchDfsQueryThenFetchAsyncAction(int numShards) {
        this(numShards, new SearchRequest().allowPartialSearchResults(true));
    }

    public MockSearchDfsQueryThenFetchAsyncAction(int numShards, SearchRequest searchRequest) {
        super(
            logger,
            new NamedWriteableRegistry(List.of()),
            mock(SearchTransportService.class),
            new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofBytes(Long.MAX_VALUE)),
            (clusterAlias, nodeId) -> createMockConnection(nodeId),
            Map.of("uuid", AliasFilter.EMPTY),
            Map.of(),
            Runnable::run,
            null,
            searchRequest,
            ActionListener.noop(),
            createShardIterators(numShards),
            Collections.emptyMap(),
            new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0),
            ClusterState.EMPTY_STATE,
            new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap()),
            null,
            null,
            new SearchResponseMetrics(TelemetryProvider.NOOP.getMeterRegistry()),
            Map.of(),
            false
        );
        this.numShards = numShards;
        numSuccess = new AtomicInteger(numShards);
    }

    private static List<SearchShardIterator> createShardIterators(int numShards) {
        List<SearchShardIterator> shardIterators = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            shardIterators.add(
                new SearchShardIterator(null, new ShardId("index", "uuid", i), Collections.emptyList(), null, SplitShardCountSummary.UNSET)
            );
        }
        return shardIterators;
    }

    private static Transport.Connection createMockConnection(String nodeId) {
        return new CloseableConnection() {
            @Override
            public DiscoveryNode getNode() {
                return new DiscoveryNode(
                    nodeId,                                          // nodeName
                    nodeId,                                          // nodeId
                    new TransportAddress(TransportAddress.META_ADDRESS, 9300),  // address
                    Collections.emptyMap(),                          // attributes
                    Collections.emptySet(),                          // roles
                    null                                             // versionInfo (null = use current)
                );
            }

            @Override
            public TransportVersion getTransportVersion() {
                return TransportVersion.current();
            }

            @Override
            public void sendRequest(long requestId, String action, TransportRequest request, TransportRequestOptions options) {
                // Mock implementation - not needed for these tests
            }
        };
    }

    @Override
    public void onPhaseFailure(String phase, String msg, Throwable cause) {
        phaseFailure.set(cause);
    }

    public void assertNoFailure() {
        if (phaseFailure.get() != null) {
            throw new AssertionError(phaseFailure.get());
        }
    }

    @Override
    public void onShardFailure(int shardIndex, @Nullable SearchShardTarget shardTarget, Exception e) {
        failures.add(new ShardSearchFailure(e, shardTarget));
        numSuccess.decrementAndGet();
    }

    @Override
    public SearchTransportService getSearchTransport() {
        Assert.assertNotNull(searchTransport);
        return searchTransport;
    }

    @Override
    public void executeNextPhase(String currentPhase, Supplier<SearchPhase> nextPhaseSupplier) {
        var nextPhase = nextPhaseSupplier.get();
        try {
            nextPhase.run();
        } catch (Exception e) {
            onPhaseFailure(nextPhase.getName(), "phase failed", e);
        }
    }

    @Override
    public void sendReleaseSearchContext(ShardSearchContextId contextId, Transport.Connection connection) {
        releasedSearchContexts.add(contextId);
    }
}
