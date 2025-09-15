/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.fieldcaps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.gateway.TestGatewayAllocator;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportInterceptor;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.transport.netty4.Netty4Transport;
import org.elasticsearch.transport.netty4.SharedGroupFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Besides the assertions in each test, the variants of {@link RequestDispatcher} are verified in
 * {@link RequestTracker#verifyAfterComplete()} after each test.
 */
public class RequestDispatcherTests extends ESAllocationTestCase {
    static final Logger logger = LogManager.getLogger(RequestDispatcherTests.class);

    public void testHappyCluster() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(random()), IndexVersionUtils.randomVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 2)))
                );
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = randomBoolean();
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);
            dispatcher.execute();
            responseCollector.awaitCompletion();
            assertThat(responseCollector.responses.keySet(), equalTo(Sets.newHashSet(indices)));
            assertThat(responseCollector.failures, anEmptyMap());
            assertThat("Happy case should complete after one round", dispatcher.executionRound(), equalTo(1));
            for (NodeRequest nodeRequest : requestTracker.sentNodeRequests) {
                assertThat("All requests occur in round 0", nodeRequest.round, equalTo(0));
            }
            for (String index : indices) {
                final List<NodeRequest> nodeRequests = requestTracker.nodeRequests(index);
                if (withFilter) {
                    Set<ShardId> requestedShardIds = new HashSet<>();
                    for (NodeRequest nodeRequest : nodeRequests) {
                        for (ShardId shardId : nodeRequest.requestedShardIds(index)) {
                            assertTrue(requestedShardIds.add(shardId));
                        }
                    }
                    final Set<ShardId> assignedShardIds = clusterState.routingTable()
                        .index(index)
                        .randomAllActiveShardsIt()
                        .getShardRoutings()
                        .stream()
                        .map(ShardRouting::shardId)
                        .collect(Collectors.toSet());
                    assertThat(requestedShardIds, equalTo(assignedShardIds));
                } else {
                    assertThat("index " + index + " wasn't requested one time", nodeRequests, hasSize(1));
                }
            }
        }
    }

    public void testRetryThenOk() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(2, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(random()), IndexVersionUtils.randomVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(1, 3)))
                );
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = randomBoolean();
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);

            final Map<String, Integer> maxFailedRounds = new HashMap<>();
            for (String index : randomSubsetOf(between(1, indices.size()), indices)) {
                maxFailedRounds.put(index, randomIntBetween(1, maxPossibleRounds(clusterState, index, withFilter) - 1));
            }

            final AtomicInteger failedTimes = new AtomicInteger();
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    final int currentRound = dispatcher.executionRound();
                    FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                    Set<String> requestedIndices = nodeRequest.shardIds().stream().map(ShardId::getIndexName).collect(Collectors.toSet());
                    if (currentRound > 0) {
                        assertThat(
                            "Only failed indices are retried after the first found",
                            requestedIndices,
                            everyItem(in(maxFailedRounds.keySet()))
                        );
                    }
                    Set<String> successIndices = new HashSet<>();
                    List<ShardId> failedShards = new ArrayList<>();
                    for (ShardId shardId : nodeRequest.shardIds()) {
                        final Integer maxRound = maxFailedRounds.get(shardId.getIndexName());
                        if (maxRound == null || currentRound >= maxRound) {
                            successIndices.add(shardId.getIndexName());
                        } else {
                            failedShards.add(shardId);
                            failedTimes.incrementAndGet();
                        }
                    }
                    transportService.sendResponse(handler, randomNodeResponse(successIndices, failedShards, Collections.emptySet()));
                }
            });

            dispatcher.execute();
            responseCollector.awaitCompletion();
            assertThat(responseCollector.responses.keySet(), equalTo(Sets.newHashSet(indices)));
            assertThat(responseCollector.failures, anEmptyMap());
            int maxRound = maxFailedRounds.values().stream().mapToInt(n -> n).max().getAsInt();
            assertThat(dispatcher.executionRound(), equalTo(maxRound + 1));
            for (String index : indices) {
                if (withFilter) {
                    Map<ShardId, Integer> copies = new HashMap<>();
                    for (ShardRouting shardRouting : clusterState.routingTable().index(index).randomAllActiveShardsIt()) {
                        copies.merge(shardRouting.shardId(), 1, Integer::sum);
                    }
                    final int executedRounds = maxFailedRounds.getOrDefault(index, 0);
                    for (int round = 0; round <= executedRounds; round++) {
                        final Set<ShardId> requestedShards = new HashSet<>();
                        for (NodeRequest nodeRequest : requestTracker.nodeRequests(index, round)) {
                            for (ShardId shardId : nodeRequest.requestedShardIds(index)) {
                                assertTrue(requestedShards.add(shardId));
                            }
                        }
                        final Set<ShardId> availableShards = new HashSet<>();
                        for (var e : copies.entrySet()) {
                            if (e.getValue() > 0) {
                                availableShards.add(e.getKey());
                                copies.merge(e.getKey(), -1, Integer::sum);
                            }
                        }
                        assertThat("round: " + round, requestedShards, equalTo(availableShards));
                    }
                } else {
                    final Integer failedRounds = maxFailedRounds.get(index);
                    final int sentRequests = requestTracker.nodeRequests(index).size();
                    if (failedRounds != null) {
                        assertThat(sentRequests, equalTo(failedRounds + 1));
                    } else {
                        assertThat(sentRequests, equalTo(1));
                    }
                }
            }
        }
    }

    public void testRetryButFails() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(random()), IndexVersionUtils.randomVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 3)))
                );
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = randomBoolean();
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);

            List<String> failedIndices = randomSubsetOf(between(1, indices.size()), indices);

            final AtomicInteger failedTimes = new AtomicInteger();
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    final int currentRound = dispatcher.executionRound();
                    FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                    if (currentRound > 0) {
                        for (ShardId shardId : nodeRequest.shardIds()) {
                            assertThat("Only failed indices are retried after the first found", shardId.getIndexName(), in(failedIndices));
                        }
                    }
                    Set<String> toRespondIndices = new HashSet<>();
                    Set<ShardId> toFailShards = new HashSet<>();
                    for (ShardId shardId : nodeRequest.shardIds()) {
                        if (failedIndices.contains(shardId.getIndexName())) {
                            toFailShards.add(shardId);
                            failedTimes.incrementAndGet();
                        } else {
                            toRespondIndices.add(shardId.getIndexName());
                        }
                    }
                    transportService.sendResponse(handler, randomNodeResponse(toRespondIndices, toFailShards, Collections.emptySet()));
                }
            });

            dispatcher.execute();
            responseCollector.awaitCompletion();
            assertThat(failedTimes.get(), greaterThan(0));
            assertThat(
                responseCollector.responses.keySet(),
                equalTo(indices.stream().filter(i -> failedIndices.contains(i) == false).collect(Collectors.toSet()))
            );
            assertThat(responseCollector.failures.keySet(), equalTo(Sets.newHashSet(failedIndices)));

            int maxRound = failedIndices.stream().mapToInt(index -> maxPossibleRounds(clusterState, index, withFilter)).max().getAsInt();
            assertThat(dispatcher.executionRound(), equalTo(maxRound));
            for (String index : indices) {
                if (withFilter) {
                    Map<ShardId, Integer> copies = new HashMap<>();
                    for (ShardRouting shardRouting : clusterState.routingTable().index(index).randomAllActiveShardsIt()) {
                        copies.merge(shardRouting.shardId(), 1, Integer::sum);
                    }
                    final int executedRounds = failedIndices.contains(index) ? maxPossibleRounds(clusterState, index, true) : 0;
                    for (int round = 0; round <= executedRounds; round++) {
                        final Set<ShardId> requestedShards = new HashSet<>();
                        for (NodeRequest nodeRequest : requestTracker.nodeRequests(index, round)) {
                            for (ShardId shardId : nodeRequest.requestedShardIds(index)) {
                                assertTrue(requestedShards.add(shardId));
                            }
                        }
                        final Set<ShardId> availableShards = new HashSet<>();
                        for (var e : copies.entrySet()) {
                            if (e.getValue() > 0) {
                                availableShards.add(e.getKey());
                                copies.merge(e.getKey(), -1, Integer::sum);
                            }
                        }
                        assertThat("round: " + round, requestedShards, equalTo(availableShards));
                    }
                    if (failedIndices.contains(index)) {
                        for (var e : copies.entrySet()) {
                            assertThat("All copies of shard " + e.getKey() + " must be tried", e.getValue(), equalTo(0));
                        }
                    }
                } else {
                    final int sentRequests = requestTracker.nodeRequests(index).size();
                    if (failedIndices.contains(index)) {
                        assertThat(sentRequests, equalTo(maxPossibleRounds(clusterState, index, false)));
                    } else {
                        assertThat(sentRequests, equalTo(1));
                    }
                }
            }
        }
    }

    public void testSuccessWithAnyMatch() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(random()), IndexVersionUtils.randomVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(2, 10), between(0, 2)))
                );
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = true;
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);
            final Set<ShardId> allUnmatchedShardIds = new HashSet<>();
            for (String index : indices) {
                final Set<ShardId> shardIds = new HashSet<>();
                for (ShardRouting shardRouting : clusterState.routingTable().index(index).randomAllActiveShardsIt()) {
                    shardIds.add(shardRouting.shardId());
                }
                assertThat("suspect index requires at lease two shards", shardIds.size(), greaterThan(1));
                allUnmatchedShardIds.addAll(randomSubsetOf(between(1, shardIds.size() - 1), shardIds));
            }
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                    Set<String> toRespondIndices = new HashSet<>();
                    Set<ShardId> unmatchedShardIds = new HashSet<>();
                    for (ShardId shardId : nodeRequest.shardIds()) {
                        if (allUnmatchedShardIds.contains(shardId)) {
                            assertTrue(unmatchedShardIds.add(shardId));
                        } else {
                            toRespondIndices.add(shardId.getIndexName());
                        }
                    }
                    transportService.sendResponse(
                        handler,
                        randomNodeResponse(toRespondIndices, Collections.emptyList(), unmatchedShardIds)
                    );
                }
            });
            dispatcher.execute();
            responseCollector.awaitCompletion();
            assertThat(responseCollector.responses.keySet(), equalTo(Sets.newHashSet(indices)));
            assertThat(responseCollector.failures, anEmptyMap());
            assertThat(dispatcher.executionRound(), equalTo(1));
            for (String index : indices) {
                final List<NodeRequest> nodeRequests = requestTracker.nodeRequests(index);
                Set<ShardId> requestedShardIds = new HashSet<>();
                for (NodeRequest nodeRequest : nodeRequests) {
                    for (ShardId shardId : nodeRequest.requestedShardIds(index)) {
                        assertTrue(requestedShardIds.add(shardId));
                    }
                }
                final Set<ShardId> assignedShardIds = clusterState.routingTable()
                    .index(index)
                    .randomAllActiveShardsIt()
                    .getShardRoutings()
                    .stream()
                    .map(ShardRouting::shardId)
                    .collect(Collectors.toSet());
                assertThat(requestedShardIds, equalTo(assignedShardIds));
            }
        }
    }

    public void testStopAfterAllShardsUnmatched() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(random()), IndexVersionUtils.randomVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 2)))
                );
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = true;
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);
            final List<String> unmatchedIndices = randomSubsetOf(between(1, indices.size()), indices);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                    Set<String> toRespondIndices = new HashSet<>();
                    Set<ShardId> unmatchedShardIds = new HashSet<>();
                    for (ShardId shardId : nodeRequest.shardIds()) {
                        if (unmatchedIndices.contains(shardId.getIndexName())) {
                            assertTrue(unmatchedShardIds.add(shardId));
                        } else {
                            toRespondIndices.add(shardId.getIndexName());
                        }
                    }
                    transportService.sendResponse(
                        handler,
                        randomNodeResponse(toRespondIndices, Collections.emptyList(), unmatchedShardIds)
                    );
                }
            });
            dispatcher.execute();
            responseCollector.awaitCompletion();
            assertThat(
                responseCollector.responses.keySet(),
                equalTo(indices.stream().filter(index -> unmatchedIndices.contains(index) == false).collect(Collectors.toSet()))
            );
            assertThat(responseCollector.failures, anEmptyMap());
            assertThat(dispatcher.executionRound(), equalTo(1));
            for (String index : indices) {
                final List<NodeRequest> nodeRequests = requestTracker.nodeRequests(index);
                Set<ShardId> requestedShardIds = new HashSet<>();
                for (NodeRequest nodeRequest : nodeRequests) {
                    for (ShardId shardId : nodeRequest.requestedShardIds(index)) {
                        assertTrue(requestedShardIds.add(shardId));
                    }
                }
                final Set<ShardId> assignedShardIds = clusterState.routingTable()
                    .index(index)
                    .randomAllActiveShardsIt()
                    .getShardRoutings()
                    .stream()
                    .map(ShardRouting::shardId)
                    .collect(Collectors.toSet());
                assertThat(requestedShardIds, equalTo(assignedShardIds));
            }
        }
    }

    public void testFailWithSameException() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(random()), IndexVersionUtils.randomVersion()));
            }
            Metadata.Builder metadata = Metadata.builder();
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 3)))
                );
            }
            clusterState = newClusterState(metadata.build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> targetIndices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            final ResponseCollector responseCollector = new ResponseCollector();
            boolean withFilter = randomBoolean();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                targetIndices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);

            RuntimeException ex = new RuntimeException("shared");
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    Exception failure = randomFrom(ex, new RuntimeException("second"), new IllegalStateException("third"));
                    handler.executor().execute(() -> handler.handleException(new TransportException(failure)));
                }
            });
            dispatcher.execute();
            responseCollector.awaitCompletion();
            assertThat(responseCollector.failures.keySet(), equalTo(Sets.newHashSet(targetIndices)));
        }
    }

    private static class NodeRequest {
        final int round;
        final DiscoveryNode node;
        final FieldCapabilitiesNodeRequest request;

        NodeRequest(int round, DiscoveryNode node, FieldCapabilitiesNodeRequest request) {
            this.round = round;
            this.node = node;
            this.request = request;
        }

        Set<String> indices() {
            return request.shardIds().stream().map(ShardId::getIndexName).collect(Collectors.toSet());
        }

        Set<ShardId> requestedShardIds(String index) {
            return request.shardIds().stream().filter(s -> s.getIndexName().equals(index)).collect(Collectors.toSet());
        }
    }

    private static class RequestTracker {
        private final RequestDispatcher dispatcher;
        private final RoutingTable routingTable;
        private final boolean withFilter;
        private final AtomicInteger currentRound = new AtomicInteger();
        final List<NodeRequest> sentNodeRequests = new CopyOnWriteArrayList<>();

        RequestTracker(RequestDispatcher dispatcher, RoutingTable routingTable, boolean withFilter) {
            this.dispatcher = dispatcher;
            this.routingTable = routingTable;
            this.withFilter = withFilter;
        }

        void verifyAfterComplete() {
            final int lastRound = dispatcher.executionRound();
            // No requests are sent in the last round
            for (NodeRequest request : sentNodeRequests) {
                assertThat(request.round, lessThan(lastRound));
            }
            for (int i = 0; i < lastRound; i++) {
                int round = i;
                List<NodeRequest> nodeRequests = sentNodeRequests.stream().filter(r -> r.round == round).toList();
                if (withFilter == false) {
                    // Without filter, each index is requested once in each round.
                    Map<String, Integer> requestsPerIndex = new HashMap<>();
                    nodeRequests.forEach(r -> r.indices().forEach(index -> requestsPerIndex.merge(index, 1, Integer::sum)));
                    for (var e : requestsPerIndex.entrySet()) {
                        assertThat("index " + e.getKey() + " has requested more than once", e.getValue(), equalTo(1));
                    }
                }
                // With or without filter, each new node receives at most one request each round
                final Map<DiscoveryNode, List<NodeRequest>> requestsPerNode = sentNodeRequests.stream()
                    .filter(r -> r.round == round)
                    .collect(Collectors.groupingBy(r -> r.node));
                for (Map.Entry<DiscoveryNode, List<NodeRequest>> e : requestsPerNode.entrySet()) {
                    assertThat(
                        "node " + e.getKey().getName() + " receives more than 1 requests in round " + currentRound,
                        e.getValue(),
                        hasSize(1)
                    );
                }
                // No shardId is requested more than once in a round
                Set<ShardId> requestedShards = new HashSet<>();
                for (NodeRequest nodeRequest : nodeRequests) {
                    for (ShardId shardId : nodeRequest.request.shardIds()) {
                        assertTrue(requestedShards.add(shardId));
                    }
                }
            }
            // Request only shards that assigned to target nodes
            for (NodeRequest nodeRequest : sentNodeRequests) {
                for (String index : nodeRequest.indices()) {
                    final Set<ShardId> requestedShardIds = nodeRequest.requestedShardIds(index);
                    final Set<ShardId> assignedShardIds = assignedShardsOnNode(routingTable.index(index), nodeRequest.node.getId());
                    assertThat(requestedShardIds, everyItem(in(assignedShardIds)));
                }
            }
            // No shard is requested twice each node
            Map<String, Set<ShardId>> requestedShardIdsPerNode = new HashMap<>();
            for (NodeRequest nodeRequest : sentNodeRequests) {
                final Set<ShardId> shardIds = requestedShardIdsPerNode.computeIfAbsent(nodeRequest.node.getId(), k -> new HashSet<>());
                for (ShardId shardId : nodeRequest.request.shardIds()) {
                    assertTrue(shardIds.add(shardId));
                }
            }
        }

        void verifyAndTrackRequest(Transport.Connection connection, String action, TransportRequest request) {
            final int requestRound = dispatcher.executionRound();
            final DiscoveryNode node = connection.getNode();
            if (action.equals(TransportFieldCapabilitiesAction.ACTION_NODE_NAME)) {
                assertThat(request, instanceOf(FieldCapabilitiesNodeRequest.class));
                FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                sentNodeRequests.add(new NodeRequest(requestRound, node, nodeRequest));
            }
        }

        List<NodeRequest> nodeRequests(String index, int round) {
            return sentNodeRequests.stream().filter(r -> r.round == round && r.indices().contains(index)).toList();
        }

        List<NodeRequest> nodeRequests(String index) {
            return sentNodeRequests.stream().filter(r -> r.indices().contains(index)).toList();
        }
    }

    private static class TestTransportService extends TransportService {
        final SetOnce<RequestTracker> requestTracker = new SetOnce<>();

        final ThreadPool threadPool;
        private TransportInterceptor.AsyncSender interceptor = null;

        private TestTransportService(Transport transport, TransportInterceptor.AsyncSender asyncSender, ThreadPool threadPool) {
            super(Settings.EMPTY, transport, threadPool, new TransportInterceptor() {
                @Override
                public AsyncSender interceptSender(AsyncSender sender) {
                    return asyncSender;
                }
            }, addr -> newNode("local"), null, Collections.emptySet());
            this.threadPool = threadPool;
        }

        @Override
        public Transport.Connection getConnection(DiscoveryNode node) {
            final Transport.Connection conn = mock(Transport.Connection.class);
            when(conn.getNode()).thenReturn(node);
            return conn;
        }

        static TestTransportService newTestTransportService() {
            final TestThreadPool threadPool = new TestThreadPool("test");
            TcpTransport transport = new Netty4Transport(
                Settings.EMPTY,
                TransportVersion.current(),
                threadPool,
                new NetworkService(Collections.emptyList()),
                PageCacheRecycler.NON_RECYCLING_INSTANCE,
                new NamedWriteableRegistry(Collections.emptyList()),
                new NoneCircuitBreakerService(),
                new SharedGroupFactory(Settings.EMPTY)
            );
            SetOnce<TransportInterceptor.AsyncSender> asyncSenderHolder = new SetOnce<>();
            TestTransportService transportService = new TestTransportService(transport, new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    final TransportInterceptor.AsyncSender asyncSender = asyncSenderHolder.get();
                    assertNotNull(asyncSender);
                    asyncSender.sendRequest(connection, action, request, options, handler);
                }
            }, threadPool);
            asyncSenderHolder.set(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    final RequestTracker requestTracker = transportService.requestTracker.get();
                    assertNotNull("Request tracker wasn't set", requestTracker);
                    requestTracker.verifyAndTrackRequest(connection, action, request);

                    if (transportService.interceptor != null) {
                        transportService.interceptor.sendRequest(connection, action, request, options, handler);
                    } else {
                        FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                        Set<String> indices = nodeRequest.shardIds().stream().map(ShardId::getIndexName).collect(Collectors.toSet());
                        transportService.sendResponse(
                            handler,
                            randomNodeResponse(indices, Collections.emptyList(), Collections.emptySet())
                        );
                    }
                }
            });
            transportService.start();
            return transportService;
        }

        void setTransportInterceptor(TransportInterceptor.AsyncSender interceptor) {
            this.interceptor = interceptor;
        }

        @Override
        protected void doClose() throws IOException {
            super.doClose();
            threadPool.shutdown();
            requestTracker.get().verifyAfterComplete();
        }

        @SuppressWarnings("unchecked")
        <T extends TransportResponse> void sendResponse(TransportResponseHandler<T> handler, TransportResponse resp) {
            threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION).submit(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }

                @Override
                protected void doRun() {
                    handler.handleResponse((T) resp);
                }
            });
        }
    }

    static FieldCapabilitiesRequest randomFieldCapRequest(boolean withFilter) {
        final QueryBuilder filter = withFilter ? new RangeQueryBuilder("timestamp").from(randomNonNegativeLong()) : null;
        return new FieldCapabilitiesRequest().fields("*").indexFilter(filter);
    }

    static FieldCapabilitiesNodeResponse randomNodeResponse(
        Collection<String> successIndices,
        Collection<ShardId> failedShards,
        Set<ShardId> unmatchedShards
    ) {
        final Map<ShardId, Exception> failures = new HashMap<>();
        for (ShardId shardId : failedShards) {
            failures.put(shardId, new IllegalStateException(randomAlphaOfLength(10)));
        }
        final List<FieldCapabilitiesIndexResponse> indexResponses = new ArrayList<>();
        Map<String, List<String>> indicesWithMappingHash = new HashMap<>();
        for (String index : successIndices) {
            if (randomBoolean()) {
                indicesWithMappingHash.computeIfAbsent(index, k -> new ArrayList<>()).add(index);
            } else {
                indexResponses.add(
                    new FieldCapabilitiesIndexResponse(
                        index,
                        null,
                        FieldCapabilitiesIndexResponseTests.randomFieldCaps(),
                        true,
                        randomFrom(IndexMode.values())
                    )
                );
            }
        }
        indexResponses.addAll(FieldCapabilitiesIndexResponseTests.randomIndexResponsesWithMappingHash(indicesWithMappingHash));
        Randomness.shuffle(indexResponses);
        return new FieldCapabilitiesNodeResponse(indexResponses, failures, unmatchedShards);
    }

    static class ResponseCollector {
        final Map<String, FieldCapabilitiesIndexResponse> responses = ConcurrentCollections.newConcurrentMap();
        final Map<String, Exception> failures = ConcurrentCollections.newConcurrentMap();
        final CountDownLatch latch = new CountDownLatch(1);

        void addIndexResponse(FieldCapabilitiesIndexResponse resp) {
            assertTrue("Only matched responses are updated", resp.canMatch());
            final String index = resp.getIndexName();
            final FieldCapabilitiesIndexResponse existing = responses.put(index, resp);
            assertNull("index [" + index + "] was responded already", existing);
            assertThat("index [" + index + "]was failed already", index, not(in(failures.keySet())));
        }

        void addIndexFailure(String index, Exception e) {
            final Exception existing = failures.put(index, e);
            assertNull("index [" + index + "] was failed already", existing);
            assertThat("index [" + index + "]was responded already", index, not(in(responses.keySet())));
        }

        void onComplete() {
            latch.countDown();
        }

        void awaitCompletion() throws Exception {
            assertTrue(latch.await(1, TimeUnit.MINUTES));
        }
    }

    static Set<ShardId> assignedShardsOnNode(IndexRoutingTable routingTable, String nodeId) {
        final Set<ShardId> shardIds = new HashSet<>();
        for (ShardRouting shardRouting : routingTable.randomAllActiveShardsIt()) {
            if (shardRouting.currentNodeId().equals(nodeId)) {
                shardIds.add(shardRouting.shardId());
            }
        }
        return shardIds;
    }

    static Task newRandomParentTask() {
        return new Task(0, "type", "action", randomAlphaOfLength(10), TaskId.EMPTY_TASK_ID, Collections.emptyMap());
    }

    private ClusterState newClusterState(Metadata metadata, DiscoveryNodes discoveryNodes) {
        final RoutingTable.Builder routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
        for (IndexMetadata imd : metadata) {
            routingTable.addAsNew(metadata.index(imd.getIndex()));
        }
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTable.build())
            .build();
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = createBuiltInClusterSettings(settings);
        final ArrayList<AllocationDecider> deciders = new ArrayList<>();
        deciders.add(new EnableAllocationDecider(clusterSettings));
        deciders.add(new SameShardAllocationDecider(clusterSettings));
        deciders.add(new ReplicaAfterPrimaryActiveAllocationDecider());
        Collections.shuffle(deciders, random());
        final MockAllocationService allocationService = new MockAllocationService(
            new AllocationDeciders(deciders),
            new TestGatewayAllocator(),
            new BalancedShardsAllocator(settings),
            EmptyClusterInfoService.INSTANCE,
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES
        );
        return applyStartedShardsUntilNoChange(clusterState, allocationService);
    }

    /**
     * Returns the maximum number of rounds that a given index can be executed in case of failures.
     */
    static int maxPossibleRounds(ClusterState clusterState, String index, boolean withFilter) {
        final IndexRoutingTable routingTable = clusterState.routingTable().index(index);
        if (withFilter) {
            Map<ShardId, Integer> numCopiesPerShard = new HashMap<>();
            for (ShardRouting shard : routingTable.randomAllActiveShardsIt()) {
                numCopiesPerShard.merge(shard.shardId(), 1, Integer::sum);
            }
            int maxRound = 0;
            for (var numCopies : numCopiesPerShard.values()) {
                maxRound = Math.max(maxRound, numCopies);
            }
            return maxRound;
        } else {
            Map<String, Integer> requestsPerNode = new HashMap<>();
            for (ShardRouting shard : routingTable.randomAllActiveShardsIt()) {
                requestsPerNode.put(shard.currentNodeId(), 1);
            }
            int totalRequests = 0;
            for (var nodeRequests : requestsPerNode.values()) {
                totalRequests += nodeRequests;
            }
            return totalRequests;
        }
    }

    static ClusterService mockClusterService(ClusterState clusterState) {
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final OperationRouting operationRouting = new OperationRouting(Settings.EMPTY, clusterSettings);
        when(clusterService.operationRouting()).thenReturn(operationRouting);
        return clusterService;
    }

    static CoordinatorRewriteContextProvider coordinatorRewriteContextProvider() {
        return mock(CoordinatorRewriteContextProvider.class);
    }
}
