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
import org.elasticsearch.action.NoShardAvailableActionException;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.EmptyClusterInfoService;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ReplicaAfterPrimaryActiveAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.SameShardAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.query.CoordinatorRewriteContextProvider;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.breaker.NoneCircuitBreakerService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskCancelHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ClusterServiceUtils;
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
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

/**
 * Besides the assertions in each test, the variants of {@link RequestDispatcher} are verified in
 * {@link RequestTracker#verifyAfterComplete()} after each test.
 */
public class RequestDispatcherTests extends ESAllocationTestCase {
    static final Logger logger = LogManager.getLogger(RequestDispatcherTests.class);

    public void testResolvesAgainFromAppliedState() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final ProjectMetadata.Builder projectMetadata = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)));
        final Metadata metadata = Metadata.builder().put(projectMetadata).build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ClusterState laterState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();
        final ClusterService clusterService = mockClusterService(initialState);
        when(clusterService.state()).thenReturn(initialState, laterState);

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    if (connection.getNode().getId().equals("node_0")) {
                        transportService.sendResponse(
                            handler,
                            new FieldCapabilitiesNodeResponse(
                                List.of(),
                                Map.of(shardId, new NoShardAvailableActionException(shardId)),
                                Set.of()
                            )
                        );
                    } else {
                        transportService.sendResponse(handler, randomNodeResponse(List.of(index), List.of(), Set.of()));
                    }
                }
            });

            dispatcher.start();
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses.keySet(), equalTo(Set.of(index)));
            assertThat(responseCollector.failures, anEmptyMap());
            assertEquals(0, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId), send("node_1", shardId));
        }
    }

    public void testUntriedCopiesClassifiesRoutingWithoutThrowing() {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final ClusterState state = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardRouting shard = state.routingTable(projectId).index(index).shard(0).primaryShard();
        final List<ShardRouting> copies = new ArrayList<>();

        assertEquals(
            RequestDispatcher.Routing.UNTRIED_COPY,
            RequestDispatcher.untriedCopies(state, projectId, shard.shardId(), Map.of(), copies)
        );
        assertEquals(List.of(shard), copies);
        copies.clear();
        assertEquals(
            RequestDispatcher.Routing.NONE_YET,
            RequestDispatcher.untriedCopies(state, projectId, shard.shardId(), Map.of(shard.allocationId().getId(), true), copies)
        );
        assertEquals(
            RequestDispatcher.Routing.UNTRIED_COPY,
            RequestDispatcher.untriedCopies(state, projectId, shard.shardId(), Map.of(shard.allocationId().getId(), false), copies)
        );
        assertEquals(
            RequestDispatcher.Routing.GONE,
            RequestDispatcher.untriedCopies(state, ProjectId.fromId("missing"), shard.shardId(), Map.of(), new ArrayList<>())
        );
        assertEquals(
            RequestDispatcher.Routing.GONE,
            RequestDispatcher.untriedCopies(
                state,
                projectId,
                new ShardId(new Index(index, randomUUID()), shard.id()),
                Map.of(),
                new ArrayList<>()
            )
        );

        // NO_ACTIVE_COPY: a shard with only an initializing copy has no active searchable copy
        final ClusterState initState = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build(),
            projectId,
            List.of(
                TestShardRouting.newShardRouting(
                    shard.shardId(),
                    "node_0",
                    true,
                    ShardRoutingState.INITIALIZING,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE
                )
            )
        );
        assertEquals(
            RequestDispatcher.Routing.NO_ACTIVE_COPY,
            RequestDispatcher.untriedCopies(initState, projectId, shard.shardId(), Map.of(), new ArrayList<>())
        );

        // Same node under a new allocation id: the shard is an untried copy
        final String oldAllocationId = shard.allocationId().getId();
        final ShardRouting reAllocated = TestShardRouting.newShardRouting(shard.shardId(), "node_0", true, ShardRoutingState.STARTED);
        assertNotEquals(oldAllocationId, reAllocated.allocationId().getId());
        final ClusterState reAllocatedState = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build(),
            projectId,
            List.of(reAllocated)
        );
        final List<ShardRouting> reAllocCopies = new ArrayList<>();
        assertEquals(
            RequestDispatcher.Routing.UNTRIED_COPY,
            RequestDispatcher.untriedCopies(reAllocatedState, projectId, shard.shardId(), Map.of(oldAllocationId, true), reAllocCopies)
        );
        assertEquals(List.of(reAllocated), reAllocCopies);
    }

    public void testRoutingWaitTimesOutWithOriginalFailure() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final ClusterState initialState = newClusterState(
            Metadata.builder()
                .put(
                    ProjectMetadata.builder(projectId)
                        .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                )
                .build(),
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();
        final NoShardAvailableActionException originalFailure = new NoShardAvailableActionException(shardId);

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMillis(50),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    transportService.sendResponse(
                        handler,
                        new FieldCapabilitiesNodeResponse(List.of(), Map.of(shardId, originalFailure), Set.of())
                    );
                }
            });

            dispatcher.start();
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses, anEmptyMap());
            assertSame(originalFailure, responseCollector.failures.get(index));
            assertEquals(1, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId));
        }
    }

    public void testMovedShardIsResolvedAgainWhenAnotherShardAnswered() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 2, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ClusterState laterState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId movedShard = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();
        final ShardId otherShard = initialState.routingTable(projectId).index(index).shard(1).primaryShard().shardId();
        final ClusterService clusterService = mockClusterService(initialState);
        when(clusterService.state()).thenReturn(initialState, laterState);

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<FieldCapabilitiesIndexResponse> responses = new CopyOnWriteArrayList<>();
            final Map<String, Exception> failures = ConcurrentCollections.newConcurrentMap();
            final CountDownLatch completed = new CountDownLatch(1);
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                new FieldCapabilitiesRequest().fields("*").includeEmptyFields(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.ZERO,
                responses::add,
                failures::put,
                completed::countDown,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    final FieldCapabilitiesNodeResponse successful = randomNodeResponse(List.of(index), List.of(), Set.of());
                    if (connection.getNode().getId().equals("node_0")) {
                        transportService.sendResponse(
                            handler,
                            new FieldCapabilitiesNodeResponse(
                                successful.getIndexResponses(),
                                Map.of(movedShard, new NoShardAvailableActionException(movedShard)),
                                Set.of()
                            )
                        );
                    } else {
                        transportService.sendResponse(handler, successful);
                    }
                }
            });

            dispatcher.start();
            assertTrue(completed.await(1, TimeUnit.MINUTES));

            assertEquals(2, responses.size());
            // The dispatcher keeps the original per-shard failure as it does today. The action drops it when another
            // shard of the index answered; reporting hidden partial results is deliberately left to the companion change.
            assertThat(failures.get(index), instanceOf(NoShardAvailableActionException.class));
            // shard 1 answered on node_0 and is not asked again
            assertSends(requestTracker, send("node_0", movedShard, otherShard), send("node_1", movedShard));
        }
    }

    public void testWaitsForNewerRouting() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ClusterState laterRouting = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    if (connection.getNode().getId().equals("node_0")) {
                        transportService.sendResponse(
                            handler,
                            new FieldCapabilitiesNodeResponse(
                                List.of(),
                                Map.of(shardId, new NoShardAvailableActionException(shardId)),
                                Set.of()
                            )
                        );
                    } else {
                        transportService.sendResponse(handler, randomNodeResponse(List.of(index), List.of(), Set.of()));
                    }
                }
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // A newer but unrelated state must not wake the routing predicate.
            ClusterServiceUtils.setState(clusterService, clusterService.state());
            assertEquals(1, requestTracker.sentNodeRequests.size());

            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(laterRouting).version(clusterService.state().version()).build()
            );
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses.keySet(), equalTo(Set.of(index)));
            assertThat(responseCollector.failures, anEmptyMap());
            assertEquals(1, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId), send("node_1", shardId));
        }
    }

    public void testOtherFailureIsNotResolvedAgain() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final ProjectMetadata.Builder projectMetadata = ProjectMetadata.builder(projectId)
            .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)));
        final Metadata metadata = Metadata.builder().put(projectMetadata).build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ClusterState laterState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();
        final ClusterService clusterService = mockClusterService(initialState);
        when(clusterService.state()).thenReturn(initialState, laterState);
        // Neither unit resolves a shard again whose failure is not an unavailable copy, although the applied state has a new copy.
        final boolean shardIsUnit = randomBoolean();

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                shardIsUnit ? new FieldCapabilitiesRequest().fields("*").includeEmptyFields(false) : randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    transportService.sendResponse(
                        handler,
                        new FieldCapabilitiesNodeResponse(
                            List.of(),
                            Map.of(shardId, new CircuitBreakingException("test", 0, 0, CircuitBreaker.Durability.TRANSIENT)),
                            Set.of()
                        )
                    );
                }
            });

            dispatcher.start();
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses, anEmptyMap());
            assertNotNull(responseCollector.failures.get(index));
            assertEquals(0, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId));
        }
    }

    public void testNoWaitWhenTimeoutIsZero() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();
        final ClusterService clusterService = mockClusterService(initialState);

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    transportService.sendResponse(
                        handler,
                        new FieldCapabilitiesNodeResponse(
                            List.of(),
                            Map.of(shardId, new NoShardAvailableActionException(shardId)),
                            Set.of()
                        )
                    );
                }
            });

            dispatcher.start();
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses, anEmptyMap());
            assertNotNull(responseCollector.failures.get(index));
            assertEquals(0, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId));
        }
    }

    public void testGoneShardFailsWithoutWaiting() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();
        // The applied state has the index deleted (empty metadata for the project).
        final ClusterState laterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build())
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId)).build())
            .routingTable(
                GlobalRoutingTable.builder()
                    .put(projectId, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).build())
                    .build()
            )
            .build();
        final ClusterService clusterService = mockClusterService(initialState);
        when(clusterService.state()).thenReturn(initialState, laterState);

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    transportService.sendResponse(
                        handler,
                        new FieldCapabilitiesNodeResponse(
                            List.of(),
                            Map.of(shardId, new NoShardAvailableActionException(shardId)),
                            Set.of()
                        )
                    );
                }
            });

            dispatcher.start();
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses, anEmptyMap());
            assertNotNull(responseCollector.failures.get(index));
            assertEquals(0, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId));
        }
    }

    /**
     * An index whose only copy is initializing, as during a recovery, is tried by the first resolution: the request goes to that copy.
     * When the copy answers as unavailable, the second resolution finds no active copy, which is terminal: the index fails in that
     * round with the copy's failure, whatever the bound.
     */
    public void testInitializingOnlyCopyFailsAtOnce() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
            .build();
        final ShardId shardId = newClusterState(metadata, nodes).routingTable(projectId).index(index).shard(0).shardId();
        // Built by hand: the allocation service starts every shard it initializes. The captured and the applied state are this one.
        final ClusterState initializingOnly = buildStateWithRouting(
            metadata,
            nodes,
            projectId,
            List.of(
                TestShardRouting.newShardRouting(
                    shardId,
                    "node_0",
                    true,
                    ShardRoutingState.INITIALIZING,
                    RecoverySource.EmptyStoreRecoverySource.INSTANCE
                )
            )
        );
        final NoShardAvailableActionException originalFailure = new NoShardAvailableActionException(shardId);

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initializingOnly, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = newDispatcher(
                clusterService,
                transportService,
                projectId,
                newRandomParentTask(),
                randomFieldCapRequest(false),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initializingOnly.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            scriptNodes(
                transportService,
                (nodeId, request, respond) -> respond.accept(
                    new FieldCapabilitiesNodeResponse(List.of(), Map.of(shardId, originalFailure), Set.of())
                )
            );

            dispatcher.start();
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses, anEmptyMap());
            assertSame(originalFailure, responseCollector.failures.get(index));
            assertEquals(0, dispatcher.routingWaits());
            // the constructor did try the initializing copy
            assertSends(requestTracker, send("node_0", shardId));
        }
    }

    /**
     * Shard unit, bound zero. The applied state has an untried copy for shard 0 and nothing new for shard 1. The copy that was found is
     * asked although its sibling has nothing to ask, and the sibling then fails with its original failure, without a wait.
     */
    public void testZeroBoundSendsTheCopyItFound() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 2, 0)))
            )
            .build();
        final DiscoveryNode node0 = newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final DiscoveryNode node1 = newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final ClusterState initialState = newClusterState(metadata, DiscoveryNodes.builder().add(node0).build());
        final ShardRouting shard0 = initialState.routingTable(projectId).index(index).shard(0).primaryShard();
        final ShardRouting shard1 = initialState.routingTable(projectId).index(index).shard(1).primaryShard();
        // Built by hand: shard 0 under a new allocation on node_1, shard 1 the very routing the dispatcher captured.
        final ClusterState appliedState = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder().add(node0).add(node1).build(),
            projectId,
            List.of(TestShardRouting.newShardRouting(shard0.shardId(), "node_1", true, ShardRoutingState.STARTED), shard1)
        );

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final List<FieldCapabilitiesIndexResponse> responses = new CopyOnWriteArrayList<>();
            final Map<String, Exception> failures = ConcurrentCollections.newConcurrentMap();
            final CountDownLatch completed = new CountDownLatch(1);
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = newDispatcher(
                clusterService,
                transportService,
                projectId,
                newRandomParentTask(),
                new FieldCapabilitiesRequest().fields("*").includeEmptyFields(false),
                new String[] { index },
                executor,
                executor,
                TimeValue.ZERO,
                responses::add,
                failures::put,
                completed::countDown
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            scriptNodes(transportService, (nodeId, request, respond) -> {
                if (nodeId.equals("node_0")) {
                    respond.accept(unavailable(request.shardIds()));
                } else {
                    respond.accept(randomNodeResponse(List.of(index), List.of(), Set.of()));
                }
            });
            // The dispatcher captured the initial state; its second resolution reads this one.
            applyState(clusterService, appliedState);

            dispatcher.start();
            assertTrue(completed.await(1, TimeUnit.MINUTES));

            assertSends(requestTracker, send("node_0", shard0.shardId(), shard1.shardId()), send("node_1", shard0.shardId()));
            assertThat(responses, hasSize(1));
            assertThat(failures.get(index), instanceOf(NoShardAvailableActionException.class));
            assertEquals(0, dispatcher.routingWaits());
        }
    }

    /**
     * Shard unit. The first second resolution queues a copy for shard 0 and finds shard 1 stalled. The copy is asked in that round,
     * before any wait, and the budget starts in that round too: when the dispatcher parks for shard 1, a round later, part of the
     * budget is already gone.
     */
    public void testDeadlineStartsWithTheFirstStalledShard() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 2, 0)))
            )
            .build();
        final DiscoveryNode node0 = newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final DiscoveryNode node1 = newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final DiscoveryNodes bothNodes = DiscoveryNodes.builder().add(node0).add(node1).build();
        final ClusterState initialState = newClusterState(metadata, DiscoveryNodes.builder().add(node0).build());
        final ShardRouting shard0 = initialState.routingTable(projectId).index(index).shard(0).primaryShard();
        final ShardRouting shard1 = initialState.routingTable(projectId).index(index).shard(1).primaryShard();
        // Built by hand: shard 0 under a new allocation on node_1, shard 1 the very routing the dispatcher captured.
        final ClusterState appliedState = buildStateWithRouting(
            metadata,
            bothNodes,
            projectId,
            List.of(TestShardRouting.newShardRouting(shard0.shardId(), "node_1", true, ShardRoutingState.STARTED), shard1)
        );
        final TimeValue bound = TimeValue.timeValueSeconds(10);

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final List<FieldCapabilitiesIndexResponse> responses = new CopyOnWriteArrayList<>();
            final Map<String, Exception> failures = ConcurrentCollections.newConcurrentMap();
            final CountDownLatch completed = new CountDownLatch(1);
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = newDispatcher(
                clusterService,
                transportService,
                projectId,
                newRandomParentTask(),
                new FieldCapabilitiesRequest().fields("*").includeEmptyFields(false),
                new String[] { index },
                executor,
                executor,
                bound,
                responses::add,
                failures::put,
                completed::countDown
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            scriptNodes(transportService, (nodeId, request, respond) -> {
                if (nodeId.equals("node_0")) {
                    respond.accept(unavailable(request.shardIds()));
                } else {
                    // Held back, so that the round that parks for shard 1 runs measurably later than the round that found it stalled.
                    final FieldCapabilitiesNodeResponse response = randomNodeResponse(List.of(index), List.of(), Set.of());
                    transportService.threadPool.schedule(
                        () -> respond.accept(response),
                        TimeValue.timeValueSeconds(1),
                        transportService.threadPool.generic()
                    );
                }
            });
            // The dispatcher captured the initial state; its second resolution reads this one.
            applyState(clusterService, appliedState);

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // The copy that was found was asked before the wait started.
            assertSends(requestTracker, send("node_0", shard0.shardId(), shard1.shardId()), send("node_1", shard0.shardId()));
            assertThat(responses, hasSize(1));
            final long remainingMillis = dispatcher.routingDeadlineRemainingMillis();
            assertThat(remainingMillis, greaterThan(0L));
            assertThat(remainingMillis, lessThanOrEqualTo(bound.millis() - 500));
            assertEquals(1, completed.getCount());

            // End the wait without spending the budget: the index is deleted.
            applyState(clusterService, stateWithoutIndices(projectId, bothNodes));
            assertTrue(completed.await(1, TimeUnit.MINUTES));

            assertThat(failures.get(index), instanceOf(NoShardAvailableActionException.class));
            assertEquals(1, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shard0.shardId(), shard1.shardId()), send("node_1", shard0.shardId()));
        }
    }

    public void testMixedFailuresOnOneIndex() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 2, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shard0 = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();
        final ShardId shard1 = initialState.routingTable(projectId).index(index).shard(1).primaryShard().shardId();
        // The applied state has a new copy for shard 0 on node_1 and leaves shard 1 where it was. Built by hand, so that the node of
        // the new copy is known.
        final ClusterState laterState = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder()
                .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .build(),
            projectId,
            List.of(
                TestShardRouting.newShardRouting(shard0, "node_1", true, ShardRoutingState.STARTED),
                initialState.routingTable(projectId).index(index).shard(1).primaryShard()
            )
        );
        final ClusterService clusterService = mockClusterService(initialState);
        when(clusterService.state()).thenReturn(initialState, laterState);
        final boolean shardIsUnit = randomBoolean();

        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<FieldCapabilitiesIndexResponse> responses = new CopyOnWriteArrayList<>();
            final Map<String, Exception> failures = ConcurrentCollections.newConcurrentMap();
            final CountDownLatch completed = new CountDownLatch(1);
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                shardIsUnit ? new FieldCapabilitiesRequest().fields("*").includeEmptyFields(false) : randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.ZERO,
                responses::add,
                failures::put,
                completed::countDown,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    // shard 0 fails as unavailable on every copy, shard 1 fails with CircuitBreakingException
                    final Map<ShardId, Exception> shardFailures = new HashMap<>();
                    for (ShardId requested : ((FieldCapabilitiesNodeRequest) request).shardIds()) {
                        shardFailures.put(
                            requested,
                            requested.equals(shard0)
                                ? new NoShardAvailableActionException(shard0)
                                : new CircuitBreakingException("test", 0, 0, CircuitBreaker.Durability.TRANSIENT)
                        );
                    }
                    transportService.sendResponse(handler, new FieldCapabilitiesNodeResponse(List.of(), shardFailures, Set.of()));
                }
            });

            dispatcher.start();
            assertTrue(completed.await(1, TimeUnit.MINUTES));

            if (shardIsUnit) {
                // shard 0 is resolved again and asked; shard 1 is not
                assertSends(requestTracker, send("node_0", shard0, shard1), send("node_1", shard0));
            } else {
                // Index unit: mixed failures => no second resolution, the new copy of shard 0 is not asked
                assertSends(requestTracker, send("node_0", shard0, shard1));
            }
            assertThat(responses, empty());
            assertNotNull(failures.get(index));
            assertEquals(0, dispatcher.routingWaits());
        }
    }

    public void testCancelDuringRoutingWait() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final ClusterState initialState = newClusterState(
            Metadata.builder()
                .put(
                    ProjectMetadata.builder(projectId)
                        .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                )
                .build(),
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final CancellableTask parentTask = new CancellableTask(
                0,
                "type",
                "action",
                randomAlphaOfLength(10),
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap()
            );
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                parentTask,
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    transportService.sendResponse(
                        handler,
                        new FieldCapabilitiesNodeResponse(
                            List.of(),
                            Map.of(shardId, new NoShardAvailableActionException(shardId)),
                            Set.of()
                        )
                    );
                }
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));
            TaskCancelHelper.cancel(parentTask, "simulated cancel during routing wait");

            responseCollector.awaitCompletion();
            assertThat(responseCollector.responses, anEmptyMap());
            assertThat(responseCollector.failures, anEmptyMap());
            assertEquals(1, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId));
        }
    }

    public void testIndexDeletedDuringRoutingWait() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final ClusterState initialState = newClusterState(
            Metadata.builder()
                .put(
                    ProjectMetadata.builder(projectId)
                        .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                )
                .build(),
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    transportService.sendResponse(
                        handler,
                        new FieldCapabilitiesNodeResponse(
                            List.of(),
                            Map.of(shardId, new NoShardAvailableActionException(shardId)),
                            Set.of()
                        )
                    );
                }
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // Apply a state without the index
            final ClusterState deletedState = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(clusterService.state().nodes())
                .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId)).build())
                .routingTable(
                    GlobalRoutingTable.builder()
                        .put(projectId, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).build())
                        .build()
                )
                .version(clusterService.state().version())
                .build();
            ClusterServiceUtils.setState(clusterService, deletedState);

            responseCollector.awaitCompletion();
            assertThat(responseCollector.responses, anEmptyMap());
            assertThat(responseCollector.failures.get(index), instanceOf(NoShardAvailableActionException.class));
            assertEquals(1, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shardId));
        }
    }

    /**
     * Index A has no copy left and nothing new in the applied state, while index B still has a second copy to ask. B's copy is asked in
     * the round that finds A stalled; the wait for A starts only when nothing is left to send.
     */
    public void testStalledIndexDoesNotHoldOthers() throws Exception {
        final String indexA = "index_a";
        final String indexB = "index_b";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(indexA).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                    .put(IndexMetadata.builder(indexB).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 1)))
            )
            .build();
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
            .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
            .build();
        final ClusterState initialState = newClusterState(metadata, nodes);

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = newDispatcher(
                clusterService,
                transportService,
                projectId,
                newRandomParentTask(),
                randomFieldCapRequest(false),
                new String[] { indexA, indexB },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            transportService.requestTracker.set(new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false));
            final AtomicInteger requestsForB = new AtomicInteger();
            final AtomicInteger waitsWhenBAnswered = new AtomicInteger(-1);
            scriptNodes(transportService, (nodeId, request, respond) -> {
                // A is unavailable everywhere. B is unavailable on the first copy asked and answers on the second.
                final Map<ShardId, Exception> shardFailures = new HashMap<>();
                final List<String> answeredIndices = new ArrayList<>();
                for (ShardId shardId : request.shardIds()) {
                    if (shardId.getIndexName().equals(indexB) && requestsForB.incrementAndGet() == 2) {
                        waitsWhenBAnswered.set(dispatcher.routingWaits());
                        answeredIndices.add(indexB);
                    } else {
                        shardFailures.put(shardId, new NoShardAvailableActionException(shardId));
                    }
                }
                respond.accept(
                    new FieldCapabilitiesNodeResponse(
                        randomNodeResponse(answeredIndices, List.of(), Set.of()).getIndexResponses(),
                        shardFailures,
                        Set.of()
                    )
                );
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // B's second copy was asked, and answered, before any wait: the stalled A did not hold it.
            assertEquals(0, waitsWhenBAnswered.get());
            assertNotNull(responseCollector.responses.get(indexB));
            assertThat(responseCollector.failures, anEmptyMap());
            assertEquals(1, responseCollector.latch.getCount());

            // End the wait for A without spending the budget: the indices are deleted.
            applyState(clusterService, stateWithoutIndices(projectId, nodes));
            responseCollector.awaitCompletion();

            assertThat(responseCollector.failures.get(indexA), instanceOf(NoShardAvailableActionException.class));
            assertThat(responseCollector.responses.keySet(), equalTo(Set.of(indexB)));
            assertEquals(1, dispatcher.routingWaits());
        }
    }

    /**
     * Two indices A and B both parked. A state gives A a copy and leaves B: A's copy is asked at once, B is parked again. Then B's
     * index is deleted: B fails without waiting for the deadline.
     */
    public void testOneReadyUnitWakesTheWait() throws Exception {
        final String indexA = "index_a";
        final String indexB = "index_b";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(indexA).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                    .put(IndexMetadata.builder(indexB).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardA = initialState.routingTable(projectId).index(indexA).shard(0).primaryShard().shardId();
        final ShardId shardB = initialState.routingTable(projectId).index(indexB).shard(0).primaryShard().shardId();

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { indexA, indexB },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            final AtomicInteger requestCount = new AtomicInteger();
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    int count = requestCount.incrementAndGet();
                    if (count == 1) {
                        // First request: both shards fail as unavailable
                        transportService.sendResponse(
                            handler,
                            new FieldCapabilitiesNodeResponse(
                                List.of(),
                                Map.of(
                                    shardA,
                                    new NoShardAvailableActionException(shardA),
                                    shardB,
                                    new NoShardAvailableActionException(shardB)
                                ),
                                Set.of()
                            )
                        );
                    } else {
                        // Later requests (node_1 for index_a): succeed
                        FieldCapabilitiesNodeRequest nodeRequest = (FieldCapabilitiesNodeRequest) request;
                        Set<String> indices = nodeRequest.shardIds().stream().map(ShardId::getIndexName).collect(Collectors.toSet());
                        transportService.sendResponse(handler, randomNodeResponse(indices, List.of(), Set.of()));
                    }
                }
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // A state that gives index_a a new copy on node_1 but leaves index_b's shard unchanged (same allocation → NONE_YET).
            // Built by hand so the allocator doesn't place index_b on node_1 too.
            final ShardRouting originalShardB = initialState.routingTable(projectId).index(indexB).shard(0).primaryShard();
            final DiscoveryNodes laterNodes = DiscoveryNodes.builder()
                .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .build();
            final Index indexAIndex = initialState.routingTable(projectId).index(indexA).getIndex();
            final Index indexBIndex = initialState.routingTable(projectId).index(indexB).getIndex();
            final ClusterState stateWithCopyForA = buildStateWithRouting(
                metadata,
                laterNodes,
                projectId,
                Map.of(
                    indexAIndex,
                    List.of(TestShardRouting.newShardRouting(shardA, "node_1", true, ShardRoutingState.STARTED)),
                    indexBIndex,
                    List.of(originalShardB)
                )
            );
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(stateWithCopyForA).version(clusterService.state().version()).build()
            );

            // The wake finds A ready and sends its copy, but B re-parks (tried allocation, NONE_YET). Wait for the second wait.
            assertBusy(() -> assertEquals(2, dispatcher.routingWaits()));

            // Now delete index_b: the second wake ends it
            final Metadata metadataWithoutB = Metadata.builder()
                .put(
                    ProjectMetadata.builder(projectId)
                        .put(IndexMetadata.builder(indexA).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                )
                .build();
            final ClusterState deletedBState = buildStateWithRouting(
                metadataWithoutB,
                laterNodes,
                projectId,
                Map.of(indexAIndex, List.of(TestShardRouting.newShardRouting(shardA, "node_1", true, ShardRoutingState.STARTED)))
            );
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(deletedBState).version(clusterService.state().version()).build()
            );

            responseCollector.awaitCompletion();

            // index_a succeeds via node_1; index_b fails (deleted)
            assertNotNull(responseCollector.responses.get(indexA));
            assertNotNull(responseCollector.failures.get(indexB));
        }
    }

    /**
     * A shard that failed on node_0 reappears on node_0 under a new allocation id in a later state. It is asked again.
     */
    public void testNewAllocationOnATriedNode() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            final AtomicInteger requestCount = new AtomicInteger();
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    int count = requestCount.incrementAndGet();
                    if (count == 1) {
                        transportService.sendResponse(
                            handler,
                            new FieldCapabilitiesNodeResponse(
                                List.of(),
                                Map.of(shardId, new NoShardAvailableActionException(shardId)),
                                Set.of()
                            )
                        );
                    } else {
                        transportService.sendResponse(handler, randomNodeResponse(List.of(index), List.of(), Set.of()));
                    }
                }
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // Apply a state where the shard is on node_0 under a new allocation id (e.g. after a full recovery)
            final ShardRouting reAllocated = TestShardRouting.newShardRouting(shardId, "node_0", true, ShardRoutingState.STARTED);
            final ClusterState laterState = buildStateWithRouting(
                metadata,
                DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build(),
                projectId,
                List.of(reAllocated)
            );
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(laterState).version(clusterService.state().version()).build()
            );

            responseCollector.awaitCompletion();

            assertNotNull(responseCollector.responses.get(index));
            assertThat(responseCollector.failures, anEmptyMap());
            assertEquals(1, dispatcher.routingWaits());
            // the same shard on the same node, asked again under its new allocation
            assertSends(requestTracker, send("node_0", shardId), send("node_0", shardId));
        }
    }

    /**
     * The captured state shows the shard relocating. The target is tried while initializing and fails; the state that shows it started
     * ends the wait; it is asked again on the same node.
     */
    public void testRelocationTargetTriedTwice() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final ClusterState initialState = newClusterState(
            metadata,
            DiscoveryNodes.builder()
                .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .build()
        );

        // Build the relocating state: shard relocating from node_0 to node_1.
        // Only the RELOCATING shard is added; IndexShardRoutingTable synthesizes the initializing target.
        final ShardRouting originalShard = initialState.routingTable(projectId).index(index).shard(0).primaryShard();
        final ShardId shardId = originalShard.shardId();
        final AllocationId relocatingAllocId = AllocationId.newRelocation(originalShard.allocationId());
        final ShardRouting relocatingShard = TestShardRouting.shardRoutingBuilder(shardId, "node_0", true, ShardRoutingState.RELOCATING)
            .withRelocatingNodeId("node_1")
            .withAllocationId(relocatingAllocId)
            .build();

        final ClusterState relocatingState = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder()
                .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                .build(),
            projectId,
            List.of(relocatingShard)
        );

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(relocatingState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, relocatingState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            final AtomicInteger requestCount = new AtomicInteger();
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    int count = requestCount.incrementAndGet();
                    if (count <= 2) {
                        // The first two requests (source and initializing target) fail
                        transportService.sendResponse(
                            handler,
                            new FieldCapabilitiesNodeResponse(
                                List.of(),
                                Map.of(shardId, new NoShardAvailableActionException(shardId)),
                                Set.of()
                            )
                        );
                    } else {
                        // The target once started succeeds
                        transportService.sendResponse(handler, randomNodeResponse(List.of(index), List.of(), Set.of()));
                    }
                }
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // Apply a state where the shard has finished relocating: now started on node_1
            final ShardRouting startedOnTarget = TestShardRouting.shardRoutingBuilder(shardId, "node_1", true, ShardRoutingState.STARTED)
                .withAllocationId(AllocationId.newInitializing(relocatingAllocId.getRelocationId()))
                .build();
            final ClusterState finishedState = buildStateWithRouting(
                metadata,
                DiscoveryNodes.builder()
                    .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                    .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                    .build(),
                projectId,
                List.of(startedOnTarget)
            );
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(finishedState).version(clusterService.state().version()).build()
            );

            responseCollector.awaitCompletion();

            assertNotNull(responseCollector.responses.get(index));
            assertThat(responseCollector.failures, anEmptyMap());
            assertEquals(1, dispatcher.routingWaits());
            // The source and the initializing target were both tried, in the order the dispatcher keeps its nodes, and failed. The
            // target was then asked again once started: node_1 twice.
            final String first = requestTracker.sentNodeRequests.get(0).node.getId();
            final String second = first.equals("node_0") ? "node_1" : "node_0";
            assertSends(requestTracker, send(first, shardId), send(second, shardId), send("node_1", shardId));
        }
    }

    /**
     * Index unit, two shards that both failed as unavailable. At the second resolution shard 0 has no active copy, which is terminal for
     * that shard, and shard 1 has nothing new yet. The index parks for shard 1. When shard 1 gets a new copy, that copy is asked and
     * answers for the index: the terminal sibling must not have used up the index's second resolution.
     */
    public void testTerminalShardDoesNotBlockItsSibling() throws Exception {
        doTestTerminalShardNextToStalledSibling(false);
    }

    /**
     * The same routing with the bound at zero: nothing is queued and nothing may wait, so the index fails at once with its original
     * failure.
     */
    public void testTerminalShardNextToStalledSiblingFailsAtZeroBound() throws Exception {
        doTestTerminalShardNextToStalledSibling(true);
    }

    private void doTestTerminalShardNextToStalledSibling(boolean zeroBound) throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 2, 0)))
            )
            .build();
        final DiscoveryNode node0 = newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final DiscoveryNode node1 = newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final ClusterState initialState = newClusterState(metadata, DiscoveryNodes.builder().add(node0).build());
        final ShardRouting shard0 = initialState.routingTable(projectId).index(index).shard(0).primaryShard();
        final ShardRouting shard1 = initialState.routingTable(projectId).index(index).shard(1).primaryShard();
        // Built by hand, since the allocation service starts every shard it initializes: shard 0 with only an initializing copy,
        // shard 1 the very routing the dispatcher captured.
        final ShardRouting initializingShard0 = TestShardRouting.newShardRouting(
            shard0.shardId(),
            "node_0",
            true,
            ShardRoutingState.INITIALIZING,
            RecoverySource.EmptyStoreRecoverySource.INSTANCE
        );
        final ClusterState stalledState = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder().add(node0).build(),
            projectId,
            List.of(initializingShard0, shard1)
        );
        // Later: shard 0 still has no active copy, shard 1 is started on node_1 under a new allocation.
        final ClusterState siblingMovedState = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder().add(node0).add(node1).build(),
            projectId,
            List.of(initializingShard0, TestShardRouting.newShardRouting(shard1.shardId(), "node_1", true, ShardRoutingState.STARTED))
        );

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = newDispatcher(
                clusterService,
                transportService,
                projectId,
                newRandomParentTask(),
                randomFieldCapRequest(false),
                new String[] { index },
                executor,
                executor,
                zeroBound ? TimeValue.ZERO : TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            scriptNodes(transportService, (nodeId, request, respond) -> {
                if (nodeId.equals("node_0")) {
                    respond.accept(unavailable(request.shardIds()));
                } else {
                    respond.accept(randomNodeResponse(List.of(index), List.of(), Set.of()));
                }
            });
            // The dispatcher captured the initial state; its second resolution reads this one.
            applyState(clusterService, stalledState);

            dispatcher.start();
            if (zeroBound) {
                responseCollector.awaitCompletion();

                assertThat(responseCollector.responses, anEmptyMap());
                assertThat(responseCollector.failures.get(index), instanceOf(NoShardAvailableActionException.class));
                assertEquals(0, dispatcher.routingWaits());
                assertSends(requestTracker, send("node_0", shard0.shardId(), shard1.shardId()));
                return;
            }
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));
            assertSends(requestTracker, send("node_0", shard0.shardId(), shard1.shardId()));

            applyState(clusterService, siblingMovedState);
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses.keySet(), equalTo(Set.of(index)));
            assertThat(responseCollector.failures, anEmptyMap());
            assertEquals(1, dispatcher.routingWaits());
            assertSends(requestTracker, send("node_0", shard0.shardId(), shard1.shardId()), send("node_1", shard1.shardId()));
        }
    }

    /**
     * The executor rejects the hand-off of a wait's outcome. The dispatch ends there: the parked index fails with its original failure,
     * the answered one keeps its response, no further round runs, and the completion runs once, on a {@code search_coordination}
     * thread, through the forced view of the action's own runner. That is the pool the action's merge asserts.
     */
    public void testRejectedHandOffTerminates() throws Exception {
        doTestRejectedHandOff(false);
    }

    /**
     * As {@link #testRejectedHandOffTerminates()}, with the forced task refused too, as by a pool that is shut down. The dispatch still
     * completes once, on a {@code generic} thread and not on the thread that ended the wait.
     */
    public void testRejectedHandOffWhenTerminalIsRefused() throws Exception {
        doTestRejectedHandOff(true);
    }

    private void doTestRejectedHandOff(boolean terminalRefused) throws Exception {
        final String indexA = "index_a";
        final String indexB = "index_b";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(indexA).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                    .put(IndexMetadata.builder(indexB).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
            )
            .build();
        final DiscoveryNode node0 = newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final DiscoveryNode node1 = newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion());
        final ClusterState initialState = newClusterState(metadata, DiscoveryNodes.builder().add(node0).build());
        final ShardRouting shardA = initialState.routingTable(projectId).index(indexA).shard(0).primaryShard();
        final ShardRouting shardB = initialState.routingTable(projectId).index(indexB).shard(0).primaryShard();
        final NoShardAvailableActionException originalFailure = new NoShardAvailableActionException(shardB.shardId());
        // A state that would let the dispatch go on: index_b has a new started copy on node_1.
        final ClusterState newCopyForB = buildStateWithRouting(
            metadata,
            DiscoveryNodes.builder().add(node0).add(node1).build(),
            projectId,
            Map.of(
                shardA.index(),
                List.of(shardA),
                shardB.index(),
                List.of(TestShardRouting.newShardRouting(shardB.shardId(), "node_1", true, ShardRoutingState.STARTED))
            )
        );

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final RejectingExecutors rejecting = new RejectingExecutors(
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION)
            );
            final ResponseCollector responseCollector = new ResponseCollector();
            final AtomicInteger completions = new AtomicInteger();
            final AtomicReference<String> completionThread = new AtomicReference<>();
            final RequestDispatcher dispatcher = newDispatcher(
                clusterService,
                transportService,
                projectId,
                newRandomParentTask(),
                randomFieldCapRequest(false),
                new String[] { indexA, indexB },
                rejecting.executors.regular(),
                rejecting.executors.terminal(),
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                () -> {
                    completionThread.set(Thread.currentThread().getName());
                    completions.incrementAndGet();
                    responseCollector.onComplete();
                }
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            scriptNodes(transportService, (nodeId, request, respond) -> {
                // index_a answers; index_b is unavailable
                final boolean asksA = request.shardIds().contains(shardA.shardId());
                final boolean asksB = request.shardIds().contains(shardB.shardId());
                respond.accept(
                    new FieldCapabilitiesNodeResponse(
                        randomNodeResponse(asksA ? List.of(indexA) : List.of(), List.of(), Set.of()).getIndexResponses(),
                        asksB ? Map.of(shardB.shardId(), originalFailure) : Map.of(),
                        Set.of()
                    )
                );
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));
            final int roundAtPark = dispatcher.executionRound();
            assertSends(requestTracker, send("node_0", shardA.shardId(), shardB.shardId()));
            assertNotNull(responseCollector.responses.get(indexA));
            assertEquals(0, completions.get());

            // The parked dispatch submits nothing until its wait ends, so the next submission is the hand-off of the wait's outcome.
            rejecting.rejectNext.set(true);
            rejecting.refuseForced.set(terminalRefused);
            applyState(clusterService, newCopyForB);
            responseCollector.awaitCompletion();

            assertFalse("the hand-off was not rejected", rejecting.rejectNext.get());
            assertSame(originalFailure, responseCollector.failures.get(indexB));
            assertThat(responseCollector.responses.keySet(), equalTo(Set.of(indexA)));
            assertEquals(1, completions.get());
            assertThat(completionThread.get(), containsString(terminalRefused ? "[generic]" : "[search_coordination]"));
            // No round ran after the rejection: the new copy of index_b was not asked.
            assertEquals(roundAtPark, dispatcher.executionRound());
            assertSends(requestTracker, send("node_0", shardA.shardId(), shardB.shardId()));
            assertEquals(1, dispatcher.routingWaits());
        }
    }

    public void testEndedWaitReleasesItsState() throws Exception {
        final String index = "index";
        final ProjectId projectId = randomProjectIdOrDefault();
        final ClusterState initialState = newClusterState(
            Metadata.builder()
                .put(
                    ProjectMetadata.builder(projectId)
                        .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                )
                .build(),
            DiscoveryNodes.builder().add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion())).build()
        );
        final ShardId shardId = initialState.routingTable(projectId).index(index).shard(0).primaryShard().shardId();

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final CancellableTask parentTask = new CancellableTask(
                0,
                "type",
                "action",
                randomAlphaOfLength(10),
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap()
            );
            final RequestDispatcher dispatcher = new RequestDispatcher(
                clusterService,
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                parentTask,
                randomFieldCapRequest(false),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                new String[] { index },
                executor,
                executor,
                TimeValue.timeValueMinutes(10),
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false);
            transportService.requestTracker.set(requestTracker);
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    transportService.sendResponse(
                        handler,
                        new FieldCapabilitiesNodeResponse(
                            List.of(),
                            Map.of(shardId, new NoShardAvailableActionException(shardId)),
                            Set.of()
                        )
                    );
                }
            });

            dispatcher.start();
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));

            // Cancel and complete the dispatch
            TaskCancelHelper.cancel(parentTask, "end wait");
            responseCollector.awaitCompletion();
            final int roundAtCompletion = dispatcher.executionRound();

            // A state applied after the dispatch ended must not trigger anything, although it has a new copy for the shard
            final Metadata laterMetadata = Metadata.builder()
                .put(
                    ProjectMetadata.builder(projectId)
                        .put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                )
                .build();
            ClusterServiceUtils.setState(
                clusterService,
                ClusterState.builder(
                    newClusterState(
                        laterMetadata,
                        DiscoveryNodes.builder()
                            .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
                            .build()
                    )
                ).version(clusterService.state().version()).build()
            );

            // The dispatch already completed: no further round, and the new copy is not asked
            assertEquals(roundAtCompletion, dispatcher.executionRound());
            assertSends(requestTracker, send("node_0", shardId));
            assertEquals(1, dispatcher.routingWaits());
            assertThat(responseCollector.responses, anEmptyMap());
            assertThat(responseCollector.failures, anEmptyMap());
        }
    }

    /**
     * Two indices stall at different times. A has one copy and stalls first, which starts the budget. B has a second copy; it stalls a
     * round later, after that copy failed too. B gets what is left of the one budget, not a fresh bound.
     */
    public void testOneDeadlinePerDispatch() throws Exception {
        final String indexA = "index_a";
        final String indexB = "index_b";
        final ProjectId projectId = randomProjectIdOrDefault();
        final Metadata metadata = Metadata.builder()
            .put(
                ProjectMetadata.builder(projectId)
                    .put(IndexMetadata.builder(indexA).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)))
                    .put(IndexMetadata.builder(indexB).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 1)))
            )
            .build();
        final DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
            .add(newNode("node_1", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
            .build();
        final ClusterState initialState = newClusterState(metadata, nodes);
        final TimeValue bound = TimeValue.timeValueSeconds(10);

        try (
            TestTransportService transportService = TestTransportService.newTestTransportService();
            ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, transportService.threadPool)
        ) {
            final ResponseCollector responseCollector = new ResponseCollector();
            final Executor executor = transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION);
            final RequestDispatcher dispatcher = newDispatcher(
                clusterService,
                transportService,
                projectId,
                newRandomParentTask(),
                randomFieldCapRequest(false),
                new String[] { indexA, indexB },
                executor,
                executor,
                bound,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete
            );
            transportService.requestTracker.set(new RequestTracker(dispatcher, initialState.routingTable(projectId), false, false));
            final AtomicInteger requestsForB = new AtomicInteger();
            scriptNodes(transportService, (nodeId, request, respond) -> {
                // Every copy of both indices is unavailable. The answer of B's second copy is held back, so that B stalls measurably
                // later than A.
                boolean holdBack = false;
                for (ShardId shardId : request.shardIds()) {
                    if (shardId.getIndexName().equals(indexB) && requestsForB.incrementAndGet() == 2) {
                        holdBack = true;
                    }
                }
                final FieldCapabilitiesNodeResponse response = unavailable(request.shardIds());
                if (holdBack) {
                    transportService.threadPool.schedule(
                        () -> respond.accept(response),
                        TimeValue.timeValueSeconds(1),
                        transportService.threadPool.generic()
                    );
                } else {
                    respond.accept(response);
                }
            });

            dispatcher.start();
            // A single wait, started when B stalled too: while B still had a copy to ask, the stalled A did not park the dispatch.
            assertBusy(() -> assertEquals(1, dispatcher.routingWaits()));
            assertEquals(2, requestsForB.get());

            // The budget started when A stalled, a round and a held-back answer earlier. B did not get a fresh bound.
            final long remainingMillis = dispatcher.routingDeadlineRemainingMillis();
            assertThat(remainingMillis, greaterThan(0L));
            assertThat(remainingMillis, lessThanOrEqualTo(bound.millis() - 500));
            assertEquals(1, responseCollector.latch.getCount());

            // End the wait without spending the budget: the indices are deleted.
            applyState(clusterService, stateWithoutIndices(projectId, nodes));
            responseCollector.awaitCompletion();

            assertThat(responseCollector.responses, anEmptyMap());
            assertThat(responseCollector.failures.get(indexA), instanceOf(NoShardAvailableActionException.class));
            assertThat(responseCollector.failures.get(indexB), instanceOf(NoShardAvailableActionException.class));
            assertEquals(1, dispatcher.routingWaits());
        }
    }

    public void testHappyCluster() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        final ProjectId projectId = randomProjectIdOrDefault();
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 2)))
                );
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = randomBoolean();
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);
            dispatcher.start();
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
        final ProjectId projectId = randomProjectIdOrDefault();
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(2, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(1, 3)))
                );
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = randomBoolean();
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
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

            dispatcher.start();
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
        final ProjectId projectId = randomProjectIdOrDefault();
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 3)))
                );
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = randomBoolean();
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
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

            dispatcher.start();
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
        final ProjectId projectId = randomProjectIdOrDefault();
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(2, 10), between(0, 2)))
                );
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = true;
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
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
            dispatcher.start();
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
        final ProjectId projectId = randomProjectIdOrDefault();
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 2)))
                );
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            logger.debug("--> test with indices {}", indices);
            final boolean withFilter = true;
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
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
            dispatcher.start();
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
        final ProjectId projectId = randomProjectIdOrDefault();
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 3)))
                );
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> targetIndices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            final ResponseCollector responseCollector = new ResponseCollector();
            boolean withFilter = randomBoolean();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                newRandomParentTask(),
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                targetIndices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
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
            dispatcher.start();
            responseCollector.awaitCompletion();
            assertThat(responseCollector.failures.keySet(), equalTo(Sets.newHashSet(targetIndices)));
        }
    }

    public void testStopDispatchingMidFlightWhenCancelled() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, between(1, 5)).mapToObj(n -> "index_" + n).toList();
        final ProjectId projectId = randomProjectIdOrDefault();
        final ClusterState clusterState;
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            discoNodes.add(newNode("node_0", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0)));
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final CancellableTask parentTask = new CancellableTask(
                0,
                "type",
                "action",
                randomAlphaOfLength(10),
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap()
            );
            final boolean withFilter = false;
            final ResponseCollector responseCollector = new ResponseCollector();
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                parentTask,
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                allIndices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);

            final CountDownLatch round0RequestReceived = new CountDownLatch(1);
            final AtomicReference<Runnable> heldResponder = new AtomicReference<>();
            transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
                @Override
                public <T extends TransportResponse> void sendRequest(
                    Transport.Connection connection,
                    String action,
                    TransportRequest request,
                    TransportRequestOptions options,
                    TransportResponseHandler<T> handler
                ) {
                    FieldCapabilitiesNodeRequest nodeReq = (FieldCapabilitiesNodeRequest) request;
                    heldResponder.set(
                        () -> transportService.sendResponse(
                            handler,
                            randomNodeResponse(Collections.emptySet(), nodeReq.shardIds(), Collections.emptySet())
                        )
                    );
                    round0RequestReceived.countDown();
                }
            });

            dispatcher.start();
            assertTrue("round-0 node request should be dispatched", round0RequestReceived.await(30, TimeUnit.SECONDS));
            TaskCancelHelper.cancel(parentTask, "simulated mid-flight cancel");
            heldResponder.get().run();

            responseCollector.awaitCompletion();
            assertThat("round-0 should dispatch exactly one node request", requestTracker.sentNodeRequests, hasSize(1));
            assertThat(requestTracker.sentNodeRequests.get(0).round, equalTo(0));
            assertThat(dispatcher.executionRound(), equalTo(1));
            assertThat(responseCollector.responses, anEmptyMap());
            assertThat(responseCollector.failures, anEmptyMap());
        }
    }

    public void testStopDispatchingWhenCancelled() throws Exception {
        final List<String> allIndices = IntStream.rangeClosed(1, 5).mapToObj(n -> "index_" + n).toList();
        final ClusterState clusterState;
        final ProjectId projectId = randomProjectIdOrDefault();
        {
            DiscoveryNodes.Builder discoNodes = DiscoveryNodes.builder();
            int numNodes = randomIntBetween(1, 10);
            for (int i = 0; i < numNodes; i++) {
                discoNodes.add(newNode("node_" + i, VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()));
            }
            ProjectMetadata.Builder metadata = ProjectMetadata.builder(projectId);
            for (String index : allIndices) {
                metadata.put(
                    IndexMetadata.builder(index).settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, between(1, 10), between(0, 2)))
                );
            }
            clusterState = newClusterState(Metadata.builder().put(metadata).build(), discoNodes.build());
        }
        try (TestTransportService transportService = TestTransportService.newTestTransportService()) {
            final List<String> indices = randomSubsetOf(between(1, allIndices.size()), allIndices);
            final boolean withFilter = randomBoolean();
            final ResponseCollector responseCollector = new ResponseCollector();
            final CancellableTask parentTask = new CancellableTask(
                0,
                "type",
                "action",
                randomAlphaOfLength(10),
                TaskId.EMPTY_TASK_ID,
                Collections.emptyMap()
            );
            TaskCancelHelper.cancel(parentTask, "simulated");
            final RequestDispatcher dispatcher = new RequestDispatcher(
                mockClusterService(clusterState),
                transportService,
                TestProjectResolvers.singleProject(projectId),
                coordinatorRewriteContextProvider(),
                parentTask,
                randomFieldCapRequest(withFilter),
                OriginalIndices.NONE,
                randomNonNegativeLong(),
                indices.toArray(new String[0]),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                transportService.threadPool.executor(ThreadPool.Names.SEARCH_COORDINATION),
                TimeValue.ZERO,
                responseCollector::addIndexResponse,
                responseCollector::addIndexFailure,
                responseCollector::onComplete,
                null
            );
            final RequestTracker requestTracker = new RequestTracker(dispatcher, clusterState.routingTable(), withFilter);
            transportService.requestTracker.set(requestTracker);
            dispatcher.start();
            responseCollector.awaitCompletion();
            assertThat("no node requests should be dispatched for a cancelled task", requestTracker.sentNodeRequests, hasSize(0));
            assertThat("no rounds should be executed for a cancelled task", dispatcher.executionRound(), equalTo(0));
            assertThat(responseCollector.responses, anEmptyMap());
            assertThat(responseCollector.failures, anEmptyMap());
        }
    }

    public void testCache() throws Exception {
        String index = "index";
        ProjectId projectId = randomProjectIdOrDefault();
        ProjectMetadata projectMetadata = ProjectMetadata.builder(projectId)
            .put(
                IndexMetadata.builder(index)
                    .settings(indexSettings(IndexVersions.MINIMUM_COMPATIBLE, 1, 0))
                    .settingsVersion(2)
                    .mappingVersion(3)
            )
            .build();
        DiscoveryNodes nodes = DiscoveryNodes.builder()
            .add(newNode("node", VersionUtils.randomVersion(), IndexVersionUtils.randomVersion()))
            .build();
        ClusterState clusterState = newClusterState(Metadata.builder().put(projectMetadata).build(), nodes);
        IndexMetadata indexMetadata = projectMetadata.index(index);
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().fields("_index").filters("-nested").includeUnmapped(true);
        FieldCapabilitiesIndexResponse response = new FieldCapabilitiesIndexResponse(
            index,
            null,
            Map.of(),
            true,
            IndexMode.STANDARD,
            1,
            0,
            indexMetadata.getSettingsVersion(),
            indexMetadata.getMappingVersion(),
            null
        );
        FieldCapsCache cache = new FieldCapsCache();
        cache.put(
            new FieldCapsCache.Key(
                indexMetadata.getIndexUUID(),
                indexMetadata.getSettingsVersion(),
                indexMetadata.getMappingVersion(),
                request.fields(),
                request.filters()
            ),
            response
        );

        ResponseCollector responseCollector = new ResponseCollector();
        TransportService transportService = mock(TransportService.class);
        var dispatcher = new RequestDispatcher(
            mockClusterService(clusterState),
            transportService,
            TestProjectResolvers.singleProject(projectId),
            coordinatorRewriteContextProvider(),
            newRandomParentTask(),
            request,
            OriginalIndices.NONE,
            randomNonNegativeLong(),
            new String[] { index },
            r -> fail("cache hit must not use the executor"),
            r -> fail("cache hit must not use the terminal executor"),
            TimeValue.ZERO,
            responseCollector::addIndexResponse,
            responseCollector::addIndexFailure,
            responseCollector::onComplete,
            cache
        );
        dispatcher.start();
        responseCollector.awaitCompletion();
        assertSame(response, responseCollector.responses.get(index));
        assertThat(responseCollector.failures, anEmptyMap());
        verify(transportService).getThreadPool();
        verifyNoMoreInteractions(transportService);
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
        private final boolean verifyWholeRun;
        private final AtomicInteger currentRound = new AtomicInteger();
        final List<NodeRequest> sentNodeRequests = new CopyOnWriteArrayList<>();

        RequestTracker(RequestDispatcher dispatcher, RoutingTable routingTable, boolean withFilter) {
            this(dispatcher, routingTable, withFilter, true);
        }

        RequestTracker(RequestDispatcher dispatcher, RoutingTable routingTable, boolean withFilter, boolean verifyWholeRun) {
            this.dispatcher = dispatcher;
            this.routingTable = routingTable;
            this.withFilter = withFilter;
            this.verifyWholeRun = verifyWholeRun;
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
            if (verifyWholeRun == false) {
                return;
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
                        randomFrom(IndexMode.availableModes()),
                        0,
                        0,
                        0,
                        0,
                        null
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

    /** One expected send for {@link #assertSends}: a node id and the shards requested from it. */
    static Map.Entry<String, Set<ShardId>> send(String nodeId, ShardId... shardIds) {
        return Map.entry(nodeId, Set.of(shardIds));
    }

    /** Scripts what a node answers to a node-level request; {@code respond} delivers the answer as the transport would. */
    @FunctionalInterface
    private interface NodeScript {
        void answer(String nodeId, FieldCapabilitiesNodeRequest request, Consumer<FieldCapabilitiesNodeResponse> respond);
    }

    /** Installs {@code script} as the answer to every node-level request of the test. */
    private static void scriptNodes(TestTransportService transportService, NodeScript script) {
        transportService.setTransportInterceptor(new TransportInterceptor.AsyncSender() {
            @Override
            public <T extends TransportResponse> void sendRequest(
                Transport.Connection connection,
                String action,
                TransportRequest request,
                TransportRequestOptions options,
                TransportResponseHandler<T> handler
            ) {
                script.answer(
                    connection.getNode().getId(),
                    (FieldCapabilitiesNodeRequest) request,
                    response -> transportService.sendResponse(handler, response)
                );
            }
        });
    }

    /** A node response in which every given shard failed as a copy that is not on the node. */
    private static FieldCapabilitiesNodeResponse unavailable(Collection<ShardId> shardIds) {
        final Map<ShardId, Exception> failures = new HashMap<>();
        for (ShardId shardId : shardIds) {
            failures.put(shardId, new NoShardAvailableActionException(shardId));
        }
        return new FieldCapabilitiesNodeResponse(List.of(), failures, Set.of());
    }

    /** A dispatcher over a single project and without a cache, with the arguments fixed that the routing tests do not vary. */
    private static RequestDispatcher newDispatcher(
        ClusterService clusterService,
        TestTransportService transportService,
        ProjectId projectId,
        Task parentTask,
        FieldCapabilitiesRequest request,
        String[] indices,
        Executor executor,
        Executor terminalExecutor,
        TimeValue routingWaitTimeout,
        Consumer<FieldCapabilitiesIndexResponse> onIndexResponse,
        BiConsumer<String, Exception> onIndexFailure,
        Runnable onComplete
    ) {
        return new RequestDispatcher(
            clusterService,
            transportService,
            TestProjectResolvers.singleProject(projectId),
            coordinatorRewriteContextProvider(),
            parentTask,
            request,
            OriginalIndices.NONE,
            randomNonNegativeLong(),
            indices,
            executor,
            terminalExecutor,
            routingWaitTimeout,
            onIndexResponse,
            onIndexFailure,
            onComplete,
            null
        );
    }

    /** Applies {@code state} as the next cluster state of {@code clusterService} and returns once it is applied. */
    private static void applyState(ClusterService clusterService, ClusterState state) {
        ClusterServiceUtils.setState(clusterService, ClusterState.builder(state).version(clusterService.state().version()).build());
    }

    /** A state in which the project has no index, as after the deletion of every index of a test. */
    private static ClusterState stateWithoutIndices(ProjectId projectId, DiscoveryNodes nodes) {
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(Metadata.builder().put(ProjectMetadata.builder(projectId)).build())
            .routingTable(
                GlobalRoutingTable.builder()
                    .put(projectId, RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).build())
                    .build()
            )
            .build();
    }

    /**
     * The two views of the action's own single-threaded runner, over a pool that rejects on demand. A rejection is delivered the way
     * {@code EsThreadPoolExecutor} delivers it: {@code onRejection} and then {@code onAfter}, on the submitting thread.
     */
    private static final class RejectingExecutors {
        /** Rejects the next task that is not forced, once. */
        final AtomicBoolean rejectNext = new AtomicBoolean();
        /** Refuses forced tasks too, as a pool that is shut down does. */
        final AtomicBoolean refuseForced = new AtomicBoolean();
        final TransportFieldCapabilitiesAction.DispatcherExecutors executors;

        RejectingExecutors(Executor pool) {
            this.executors = TransportFieldCapabilitiesAction.buildSingleThreadedExecutor(command -> {
                // The action's runner submits nothing but AbstractRunnables.
                final AbstractRunnable runnable = (AbstractRunnable) command;
                final boolean reject = runnable.isForceExecution() ? refuseForced.get() : rejectNext.compareAndSet(true, false);
                if (reject) {
                    try {
                        runnable.onRejection(new EsRejectedExecutionException("simulated rejection"));
                    } finally {
                        runnable.onAfter();
                    }
                } else {
                    pool.execute(command);
                }
            });
        }
    }

    /**
     * Asserts that the tracker's sends match the expected list in order. Each expected send is (nodeId, shardIds).
     */
    @SafeVarargs
    static void assertSends(RequestTracker tracker, Map.Entry<String, Set<ShardId>>... expected) {
        final List<NodeRequest> actual = tracker.sentNodeRequests;
        assertEquals("number of sends", expected.length, actual.size());
        for (int i = 0; i < expected.length; i++) {
            final NodeRequest request = actual.get(i);
            assertEquals("send " + i + " node", expected[i].getKey(), request.node.getId());
            assertEquals("send " + i + " shards", expected[i].getValue(), new HashSet<>(request.request.shardIds()));
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
        GlobalRoutingTable.Builder routingTables = GlobalRoutingTable.builder();
        for (var p : metadata.projects().entrySet()) {
            RoutingTable.Builder routingTable = RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY);
            for (IndexMetadata imd : p.getValue()) {
                routingTable.addAsNew(p.getValue().index(imd.getIndex()));
            }
            routingTables.put(p.getKey(), routingTable.build());
        }
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(metadata)
            .routingTable(routingTables.build())
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
            SNAPSHOT_INFO_SERVICE_WITH_NO_SHARD_SIZES,
            TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY
        );
        return applyStartedShardsUntilNoChange(clusterState, allocationService);
    }

    /**
     * Builds a cluster state with hand-crafted shard routing, bypassing the allocation service. Use this when the allocation service
     * cannot produce the routing topology needed (e.g. an initializing-only primary, or a shard on a specific node under a known
     * allocation id). Each entry in {@code shardsPerIndex} describes all shard copies for one index.
     */
    private static ClusterState buildStateWithRouting(
        Metadata metadata,
        DiscoveryNodes nodes,
        ProjectId projectId,
        Map<Index, List<ShardRouting>> shardsPerIndex
    ) {
        final RoutingTable.Builder rtBuilder = new RoutingTable.Builder(ShardRoutingRoleStrategy.NO_SHARD_CREATION);
        for (Map.Entry<Index, List<ShardRouting>> entry : shardsPerIndex.entrySet()) {
            final IndexRoutingTable.Builder irtBuilder = new IndexRoutingTable.Builder(
                ShardRoutingRoleStrategy.NO_SHARD_CREATION,
                entry.getKey()
            );
            for (ShardRouting shard : entry.getValue()) {
                irtBuilder.addShard(shard);
            }
            rtBuilder.add(irtBuilder);
        }
        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodes)
            .metadata(metadata)
            .routingTable(GlobalRoutingTable.builder().put(projectId, rtBuilder.build()).build())
            .build();
    }

    /** Convenience overload for a single index. */
    private static ClusterState buildStateWithRouting(
        Metadata metadata,
        DiscoveryNodes nodes,
        ProjectId projectId,
        List<ShardRouting> shards
    ) {
        return buildStateWithRouting(metadata, nodes, projectId, Map.of(shards.get(0).index(), shards));
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
