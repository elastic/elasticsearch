/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.search;

import com.carrotsearch.hppc.cursors.IntObjectCursor;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.AckedClusterStateUpdateTask;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ack.AckedRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.ConnectTransportException;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.cluster.node.DiscoveryNodeRole.DATA_ROLE;
import static org.hamcrest.Matchers.equalTo;

public class RetryableSearchPhaseTests extends ESTestCase {
    private static ThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    public void beforeTest() {
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    public void afterTest() throws Exception {
        IOUtils.close(clusterService);
    }

    @BeforeClass
    public static void beforeClass() {
        threadPool = new TestThreadPool(RetryableSearchPhaseTests.getTestClass().getName());
    }

    @AfterClass
    public static void afterClass() {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
    }

    public void testFlappyNodeConnectionSucceedsAfterRetries() throws Exception {
        DiscoveryNodes nodes = addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 0, ShardAssignment.ALL_COPIES_ASSIGNED);

        final TimeValue timeout = TimeValue.timeValueSeconds(4);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators =
            GroupShardsIterator.sortAndCreate(shardIteratorFromRoutingTable(indexRoutingTable));
        TimeoutChecker timeoutChecker = new TimeoutChecker(timeout, threadPool::relativeTimeInMillis);
        SearchShardIteratorRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return true;
            }
        }.run();

        for (int i = 0; i < 2; i++) {
            searchTransportService.waitForRequestForNode("node1", pendingRequest -> {
                DiscoveryNode node1 = nodes.get("node1");
                assertNotNull(node1);
                pendingRequest.onFailure(new NodeDisconnectedException(node1, "test"));
            });
        }

        searchTransportService.waitForRequestForNode("node1", networkRequest -> networkRequest.onResponse(new EmptyPhaseResult()));

        // The request was successful
        SearchResponse searchResponse = searchPhaseContext.getSearchResponse();
        assertNotNull(searchResponse);
    }

    public void testNodeLeavesInTheMiddleOfSearch() throws Exception {
        DiscoveryNodes nodes = addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 1, ShardAssignment.ALL_COPIES_ASSIGNED);
        ShardId shardId = indexRoutingTable.shard(0).shardId();
        Index index = indexRoutingTable.getIndex();

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators =
            GroupShardsIterator.sortAndCreate(shardIteratorFromRoutingTable(indexRoutingTable));
        TimeoutChecker timeoutChecker = new TimeoutChecker(TimeValue.timeValueSeconds(90), threadPool::relativeTimeInMillis);
        ClusterStateSearchShardRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, nodes, 1);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return true;
            }
        }.run();

        searchTransportService.waitForRequests(1);

        updateClusterState(clusterState -> {
            DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(clusterState.nodes());
            nodesBuilder.remove("node1");

            ShardRouting newPrimary = createStartedShard(shardId, true, "node2");
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index).addShard(newPrimary);
            RoutingTable.Builder routingTableBuilder =
                RoutingTable.builder(clusterState.getRoutingTable()).add(indexRoutingTableBuilder.build());
            return ClusterState.builder(clusterState).nodes(nodesBuilder).routingTable(routingTableBuilder.build()).build();
        });

        // Even though NodeDisconnectedException allows to retry the request, that node left the cluster
        // and therefore we won't retry on that node
        searchTransportService.waitForRequestForNode("node1",
            networkRequest -> networkRequest.onFailure(new NodeDisconnectedException(nodes.get("node1"), "search")));

        // Fail on the second copy
        searchTransportService.waitForRequestForNode("node2",
            networkRequest -> networkRequest.onFailure(new NodeNotConnectedException(nodes.get("node2"), "search")));

        // Since the previous failure was one of the legal failures, it retries on the same shard copy
        searchTransportService.waitForRequestForNode("node2",
            networkRequest -> networkRequest.onResponse(new EmptyPhaseResult()));

        SearchResponse searchResponse = searchPhaseContext.getSearchResponse();
        assertThat(searchResponse.getSuccessfulShards(), equalTo(1));
        assertThat(searchTransportService.pendingRequestCount(), equalTo(0));
    }

    public void testSearchIsCancelledAfterIndexRemoval() throws Exception {
        addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 1, ShardAssignment.ALL_COPIES_ASSIGNED);
        Index index = indexRoutingTable.getIndex();
        List<RetryableSearchShardIterator> searchShardIterators = shardIteratorFromRoutingTable(indexRoutingTable);

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators = GroupShardsIterator.sortAndCreate(searchShardIterators);
        TimeoutChecker timeoutChecker = new TimeoutChecker(TimeValue.timeValueSeconds(2), threadPool::relativeTimeInMillis);
        ClusterStateSearchShardRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return true;
            }
        }.run();

        // Remove index
        updateClusterState(clusterState -> {
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.getRoutingTable()).remove(index.getName());
            Metadata.Builder builder = Metadata.builder(clusterState.metadata());
            builder.remove(index.getName());
            return ClusterState.builder(clusterState)
                .metadata(builder)
                .routingTable(routingTableBuilder.build())
                .build();
        });

        searchTransportService.waitForRequestForNode("node1",
            networkRequest -> networkRequest.onFailure(new IndexNotFoundException(index)));

        expectThrows(SearchPhaseExecutionException.class, searchPhaseContext::getSearchResponse);
        assertThat(searchTransportService.pendingRequestCount(), equalTo(0));
    }

    public void testShardIsMovedAroundFromUnassignedState() throws Exception {
        addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 0, ShardAssignment.UNASSIGNED);
        Index index = indexRoutingTable.getIndex();

        List<RetryableSearchShardIterator> searchShardIterators = shardIteratorFromRoutingTable(indexRoutingTable);
        GroupShardsIterator<RetryableSearchShardIterator> shardIterators = GroupShardsIterator.sortAndCreate(searchShardIterators);
        TimeoutChecker timeoutChecker = new TimeoutChecker(TimeValue.timeValueSeconds(60), threadPool::relativeTimeInMillis);
        ClusterStateSearchShardRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return true;
            }
        }.run();

        IndexShardRoutingTable shard = indexRoutingTable.shard(0);
        ShardId shardId = shard.shardId();
        final ShardRouting primaryAssigned = createStartedShard(shardId, true, "node1");
        // Add the primary
        updateClusterState(clusterState -> {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index).addShard(primaryAssigned);
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.getRoutingTable()).add(indexRoutingTableBuilder);
            return ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();
        });

        searchTransportService.waitForRequests(1);

        // Move the shard to unassigned
        final ShardRouting unassignedShard =
            ShardRoutingHelper.moveToUnassigned(primaryAssigned,
                new UnassignedInfo(UnassignedInfo.Reason.REALLOCATED_REPLICA, "Shard reallocated"));
        updateClusterState(clusterState -> {
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index).addShard(unassignedShard);
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.getRoutingTable()).add(indexRoutingTableBuilder);
            return ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();
        });

        // Respond with IndexNotFoundException
        searchTransportService.waitForRequestForNode("node1",
            networkRequest -> networkRequest.onFailure(new IndexNotFoundException(index)));

        final ShardRouting relocated = ShardRoutingHelper.relocate(primaryAssigned, "node2");
        updateClusterState(clusterState -> {
            ShardRouting targetRelocatingShard = relocated.getTargetRelocatingShard();
            targetRelocatingShard = targetRelocatingShard.moveToStarted();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index)
                .addShard(targetRelocatingShard);

            RoutingTable.Builder routingTableBuilder = RoutingTable.builder().add(indexRoutingTableBuilder);

            return ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();
        });

        searchTransportService.waitForRequestForNode("node2",
            networkRequest -> networkRequest.onResponse(new EmptyPhaseResult()));

        searchPhaseContext.getSearchResponse();
        assertThat(searchTransportService.pendingRequestCount(), equalTo(0));
    }

    public void testSearchWaitsUntilPrimaryAllocation() throws Exception {
        addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 0, ShardAssignment.UNASSIGNED);
        Index index = indexRoutingTable.getIndex();

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators =
            GroupShardsIterator.sortAndCreate(shardIteratorFromRoutingTable(indexRoutingTable));

        TimeoutChecker timeoutChecker = new TimeoutChecker(TimeValue.timeValueMillis(3000), threadPool::relativeTimeInMillis);
        SearchShardIteratorRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return false;
            }
        }.run();

        // Add the primary
        ShardId shardId = indexRoutingTable.shard(0).shardId();
        updateClusterState(clusterState -> {
            ShardRouting shardRouting1 = createStartedShard(shardId, true, "node1");

            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index).addShard(shardRouting1);
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.getRoutingTable()).add(indexRoutingTableBuilder);
            return ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();
        });

        searchTransportService.waitForRequestForNode("node1", networkRequest -> networkRequest.onResponse(new EmptyPhaseResult()));
        SearchResponse searchResponse = searchPhaseContext.getSearchResponse();
        assertNotNull(searchResponse);
        assertThat(searchResponse.getSuccessfulShards(), equalTo(1));
    }

    public void testItAllowsOtherShardRequestsToMakeProgressWhileItWaitsUntilPrimaryAllocation() throws Exception {
        DiscoveryNodes nodes = addDataNodesToClusterState("node1");
        IndexRoutingTable indexRoutingTable1 = addIndexToClusterState("documents1", 1, 0, ShardAssignment.UNASSIGNED);
        IndexRoutingTable indexRoutingTable2 = addIndexToClusterState("documents2", 1, 0, ShardAssignment.ALL_COPIES_ASSIGNED);


        List<RetryableSearchShardIterator> searchShardIterators = new ArrayList<>();
        searchShardIterators.addAll(shardIteratorFromRoutingTable(indexRoutingTable1));
        searchShardIterators.addAll(shardIteratorFromRoutingTable(indexRoutingTable2));

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators = GroupShardsIterator.sortAndCreate(searchShardIterators);
        final TimeValue timeout = TimeValue.timeValueMillis(100);
        TimeoutChecker timeoutChecker = new TimeoutChecker(timeout, threadPool::relativeTimeInMillis);
        SearchShardIteratorRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, nodes, 2);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return false;
            }
        }.run();

        // The other search shard request is triggered without waiting
        NetworkRequest<EmptyPhaseResult> networkRequest = searchTransportService.waitForRequestForNode("node1");
        SearchShardRequest request = networkRequest.request;
        assertThat(request.searchShardTarget.getShardId(), equalTo(indexRoutingTable2.shard(0).shardId()));
        networkRequest.onResponse(new EmptyPhaseResult());

        // Add the primary for documents1
        updateClusterState(clusterState -> {
            ShardId shardId = indexRoutingTable1.shard(0).shardId();
            ShardRouting assignedShardRouting1 = createStartedShard(shardId, true, "node1");

            Index index = indexRoutingTable1.getIndex();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index).addShard(assignedShardRouting1);
            RoutingTable.Builder routingTableBuilder = RoutingTable.builder(clusterState.getRoutingTable()).add(indexRoutingTableBuilder);
            return ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();
        });

        searchTransportService.waitForRequestForNode("node1", networkRequest1 -> networkRequest1.onResponse(new EmptyPhaseResult()));

        SearchResponse searchResponse = searchPhaseContext.getSearchResponse();
        assertNotNull(searchResponse);
        assertThat(searchResponse.getSuccessfulShards(), equalTo(2));
    }

    public void testShardSearchRetryEventuallyFailsAfterTimeout() throws Exception {
        DiscoveryNodes nodes = addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 0, ShardAssignment.ALL_COPIES_ASSIGNED);

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators =
            GroupShardsIterator.sortAndCreate(shardIteratorFromRoutingTable(indexRoutingTable));

        final TimeValue timeout = TimeValue.timeValueMillis(300);
        TimeoutChecker timeoutChecker = new TimeoutChecker(timeout, threadPool::relativeTimeInMillis);
        SearchShardIteratorRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                return true;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return false;
            }
        }.run();

        searchTransportService.waitForRequestForNode("node1", networkRequest ->
            networkRequest.onFailure(new NodeDisconnectedException(nodes.get("node1"), "search")));

        expectThrows(SearchPhaseExecutionException.class, searchPhaseContext::getSearchResponse);
        // Response is ignored
        searchTransportService.waitForRequestForNode("node1",
            networkRequest -> networkRequest.onResponse(new EmptyPhaseResult()));
    }

    public void testSearchPhaseTimesOutIfPrimaryAllocationTakesLongerThanTimeout() throws Exception {
        addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 0, ShardAssignment.UNASSIGNED);

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators =
            GroupShardsIterator.sortAndCreate(shardIteratorFromRoutingTable(indexRoutingTable));

        AtomicBoolean refreshCalled = new AtomicBoolean(false);
        // Hold the refresh listener indefinitely
        SearchShardIteratorRefresher refresher =
            ((searchShardIterator, refreshTimeout, refreshListener) -> refreshCalled.compareAndSet(false, true));
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        TimeoutChecker timeoutChecker = new TimeoutChecker(TimeValue.timeValueMillis(100), threadPool::relativeTimeInMillis);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return false;
            }
        }.run();

        assertThat(refreshCalled.get(), equalTo(true));

        expectThrows(SearchPhaseExecutionException.class, searchPhaseContext::getSearchResponse);
    }

    public void testAllShardCopiesFailAndRefresherDoesNotProvideAnUpdatedIterator() throws Exception {
        addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 1, ShardAssignment.ALL_COPIES_ASSIGNED);

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators =
            GroupShardsIterator.sortAndCreate(shardIteratorFromRoutingTable(indexRoutingTable));

        final TimeValue timeout = TimeValue.timeValueSeconds(10);
        final TimeoutChecker timeoutChecker = new TimeoutChecker(timeout, threadPool::relativeTimeInMillis);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        SearchShardIteratorRefresher searchShardIteratorRefresher =
            (searchShardIterator, refreshTimeout, refreshListener) -> refreshListener.onFailure(new RuntimeException("No more shards"));

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            searchShardIteratorRefresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return true;
            }
        }.run();

        searchTransportService.waitForRequestForNode("node1", networkRequest ->
            networkRequest.onFailure(new EsRejectedExecutionException("Unable to execute on node 1")));
        searchTransportService.waitForRequestForNode("node2", networkRequest ->
            networkRequest.onFailure(new EsRejectedExecutionException("Unable to execute on node 2")));

        expectThrows(SearchPhaseExecutionException.class, searchPhaseContext::getSearchResponse);
        assertThat(searchTransportService.pendingRequestCount(), equalTo(0));
    }

    public void testExistingShardIsReallocatedDuringSearch() throws Exception {
        addDataNodesToClusterState("node1", "node2");
        IndexRoutingTable indexRoutingTable =
            addIndexToClusterState(randomAlphaOfLength(10), 1, 0, ShardAssignment.ALL_COPIES_ASSIGNED);
        Index index = indexRoutingTable.getIndex();

        GroupShardsIterator<RetryableSearchShardIterator> shardIterators =
            GroupShardsIterator.sortAndCreate(shardIteratorFromRoutingTable(indexRoutingTable));
        final TimeValue timeout = TimeValue.timeValueSeconds(60);
        TimeoutChecker timeoutChecker = new TimeoutChecker(timeout, threadPool::relativeTimeInMillis);
        ClusterStateSearchShardRefresher refresher = new ClusterStateSearchShardRefresher(clusterService, threadPool);
        ShardInformationValidator shardInformationValidator =
            new ShardInformationValidator.ClusterStateShardInformationValidator(clusterService);
        FakeSearchTransportService<EmptyPhaseResult> searchTransportService = new FakeSearchTransportService<>(clusterService);
        FakeSearchContext searchPhaseContext = new FakeSearchContext(searchTransportService, clusterService.state().nodes(), 1);

        new TestRetryableSearchPhase(
            threadPool,
            searchPhaseContext,
            searchTransportService,
            timeoutChecker,
            refresher,
            shardIterators,
            shardInformationValidator) {

            @Override
            protected boolean shouldRetryOnSameShardCopy(Exception e, SearchShardTarget searchShardTarget) {
                Throwable throwable = ExceptionsHelper.unwrapCause(e);
                return throwable instanceof ConnectTransportException;
            }

            @Override
            protected boolean shouldTryOnDifferentShardCopy(int shardIndex, ShardId shardId, Exception e) {
                return true;
            }
        }.run();

        // We already got a request
        searchTransportService.waitForRequests(1);

        // Reallocate shard to node 2
        ShardRouting primary = indexRoutingTable.shard(0).primaryShard();
        final ShardRouting relocated = ShardRoutingHelper.relocate(primary, "node2");
        updateClusterState(clusterState -> {
            ShardRouting targetRelocatingShard = relocated.getTargetRelocatingShard();
            targetRelocatingShard = targetRelocatingShard.moveToStarted();
            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index)
                .addShard(targetRelocatingShard);

            RoutingTable.Builder routingTableBuilder = RoutingTable.builder().add(indexRoutingTableBuilder);

            return ClusterState.builder(clusterState).routingTable(routingTableBuilder.build()).build();
        });

        // Fail with IndexNotFoundException since the shard was reallocated
        searchTransportService.waitForRequestForNode("node1",
            pendingRequest -> pendingRequest.onFailure(new IndexNotFoundException("test")));

        searchTransportService.waitForRequestForNode("node2",
            pendingRequest -> pendingRequest.onResponse(new EmptyPhaseResult()));

        // The request was successful
        SearchResponse searchResponse = searchPhaseContext.getSearchResponse();
        assertThat(searchResponse.getSuccessfulShards(), equalTo(1));
    }

    abstract static class TestRetryableSearchPhase extends RetryableSearchPhase<EmptyPhaseResult> {
        private final FakeSearchTransportService<EmptyPhaseResult> searchTransportService;

        TestRetryableSearchPhase(ThreadPool threadPool,
                                 SearchPhaseContext searchPhaseContext,
                                 FakeSearchTransportService<EmptyPhaseResult> searchTransportService,
                                 TimeoutChecker timeoutChecker,
                                 SearchShardIteratorRefresher shardIteratorRefresher,
                                 GroupShardsIterator<RetryableSearchShardIterator> shardsIterator,
                                 ShardInformationValidator shardInformationValidator) {
            super("search",
                threadPool,
                searchPhaseContext,
                timeoutChecker,
                shardIteratorRefresher,
                shardsIterator,
                shardInformationValidator);
            this.searchTransportService = searchTransportService;
        }

        @Override
        protected void executePhaseOnShard(SearchShardTarget shard, ActionListener<EmptyPhaseResult> listener) {
            SearchShardRequest request = new SearchShardRequest(shard);
            Transport.Connection connection = getSearchPhaseContext().getConnection(shard.getClusterAlias(), shard.getNodeId());
            searchTransportService.sendRequest(connection, request, listener);
        }

        @Override
        protected void onPhaseDone() {
            getSearchPhaseContext().sendSearchResponse(InternalSearchResponse.empty(), null);
        }

        @Override
        protected boolean shouldRefreshShardTargets(int shardIndex, ShardId shardId) {
            return true;
        }
    }

    private DiscoveryNodes addDataNodesToClusterState(String... nodeIds) {
        updateClusterState(clusterState -> {
            DiscoveryNodes.Builder builder = DiscoveryNodes.builder(clusterState.nodes());
            for (String nodeId : nodeIds) {
                DiscoveryNode node =
                    new DiscoveryNode(nodeId, nodeId, buildNewFakeTransportAddress(), emptyMap(), Set.of(DATA_ROLE), Version.CURRENT);
                builder.add(node);
            }

            return ClusterState.builder(clusterState).nodes(builder).build();
        });
        return clusterService.state().nodes();
    }

    enum ShardAssignment {
        UNASSIGNED,
        PRIMARY_ASSIGNED,
        ALL_COPIES_ASSIGNED
    }

    private IndexRoutingTable addIndexToClusterState(String name, int shardCount, int replicas, ShardAssignment assignment) {
        Index index = new Index(name, "_na_");

        updateClusterState((clusterState -> {
            List<ShardId> shards = new ArrayList<>(shardCount);
            for (int i = 0; i < shardCount; i++) {
                shards.add(new ShardId(index, i));
            }

            final IndexMetadata indexMetadata = IndexMetadata.builder(name)
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(shardCount)
                .numberOfReplicas(replicas)
                .build();

            IndexRoutingTable.Builder indexRoutingTableBuilder = IndexRoutingTable.builder(index);
            switch (assignment) {
                case UNASSIGNED:
                    for (ShardId shardId : shards) {
                        ShardRouting primaryUnassigned = ShardRouting.newUnassigned(shardId,
                            true, RecoverySource.EmptyStoreRecoverySource.INSTANCE,
                            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, ""));
                        indexRoutingTableBuilder.addShard(primaryUnassigned);
                    }
                    break;
                case PRIMARY_ASSIGNED:
                    for (ShardId shardId : shards) {
                        Iterator<DiscoveryNode> nodesIterator = getNodesIterator(clusterState);
                        if (nodesIterator.hasNext() == false) {
                            throw new IllegalStateException("Not enough nodes");
                        }
                        DiscoveryNode node = nodesIterator.next();
                        indexRoutingTableBuilder.addShard(createStartedShard(shardId, true, node.getId()));
                    }
                    break;
                case ALL_COPIES_ASSIGNED:
                    for (ShardId shardId : shards) {
                        Iterator<DiscoveryNode> nodesIterator = getNodesIterator(clusterState);
                        if (nodesIterator.hasNext() == false) {
                            throw new IllegalStateException("Not enough nodes");
                        }
                        DiscoveryNode node = nodesIterator.next();
                        indexRoutingTableBuilder.addShard(createStartedShard(shardId, true, node.getId()));
                        for (int i = 0; i < replicas; i++) {
                            if (nodesIterator.hasNext() == false) {
                                throw new IllegalStateException("Not enough nodes");
                            }
                            node = nodesIterator.next();
                            indexRoutingTableBuilder.addShard(createStartedShard(shardId, false, node.getId()));
                        }
                    }
                    break;
            }

            RoutingTable routingTable = RoutingTable.builder(clusterState.routingTable()).add(indexRoutingTableBuilder.build()).build();

            Metadata.Builder metadataBuilder = Metadata.builder(clusterState.metadata());
            metadataBuilder.put(indexMetadata, false);
            return ClusterState.builder(clusterState)
                .metadata(metadataBuilder)
                .routingTable(routingTable)
                .build();
        }));

        assertThat(clusterService.state().routingTable().hasIndex(index), equalTo(true));
        return clusterService.state().routingTable().index(index);
    }

    private Iterator<DiscoveryNode> getNodesIterator(ClusterState clusterState) {
        DiscoveryNodes nodes = clusterState.nodes();
        return StreamSupport.stream(nodes.spliterator(), false)
            .filter(node -> node.isMasterNode() == false)
            .sorted(Comparator.comparing(DiscoveryNode::getId))
            .iterator();
    }

    List<RetryableSearchShardIterator> shardIteratorFromRoutingTable(IndexRoutingTable routingTable) {
        List<RetryableSearchShardIterator> searchShardIterators = new ArrayList<>();
        for (IntObjectCursor<IndexShardRoutingTable> shardCursor : routingTable.shards()) {
            IndexShardRoutingTable shard = shardCursor.value;
            RetryableSearchShardIterator searchShardIterator =
                new RetryableSearchShardIterator(null, shard.shardId(), shard.activeShards(), OriginalIndices.NONE);
            searchShardIterators.add(searchShardIterator);
        }
        return searchShardIterators;
    }

    static class NetworkRequest<Result extends SearchPhaseResult> implements ActionListener<Result> {
        private final ActionListener<Result> listener;
        private final SearchShardRequest request;

        NetworkRequest(ActionListener<Result> listener, SearchShardRequest request) {
            this.listener = listener;
            this.request = request;
        }

        private String getSenderNodeId() {
            return request.searchShardTarget.getNodeId();
        }

        @Override
        public void onResponse(Result res) {
            listener.onResponse(res);
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }
    }

    private static final class FakeSearchTransportService<Response extends SearchPhaseResult> extends SearchTransportService {
        private final ClusterService clusterService;
        private final BlockingQueue<NetworkRequest<Response>> pendingRequests = new ArrayBlockingQueue<>(10);

        FakeSearchTransportService(ClusterService clusterService) {
            super(null, null, null);
            this.clusterService = clusterService;
        }

        void sendRequest(Transport.Connection connection,
                                SearchShardRequest request,
                                ActionListener<Response> listener) {
            assertThat(connection.getNode().getId(), equalTo(request.searchShardTarget.getNodeId()));
            pendingRequests.add(new NetworkRequest<>(listener, request));
        }

        @Override
        public Transport.Connection getConnection(String clusterAlias, DiscoveryNode node) {
            if (clusterService.state().nodes().get(node.getId()) == null) {
                throw new NodeNotConnectedException(node, "Node not connected");
            }

            return new Transport.Connection() {
                @Override
                public DiscoveryNode getNode() {
                    return node;
                }

                @Override
                public void sendRequest(long requestId,
                                        String action,
                                        TransportRequest request,
                                        TransportRequestOptions options) throws TransportException {

                }

                @Override
                public void addCloseListener(ActionListener<Void> listener) {

                }

                @Override
                public boolean isClosed() {
                    return false;
                }

                @Override
                public void close() {

                }
            };
        }

        private void waitForRequestForNode(String nodeId, Consumer<NetworkRequest<Response>> consumer) throws Exception {
            NetworkRequest<Response> networkRequest = waitForRequestForNode(nodeId);
            consumer.accept(networkRequest);
        }

        private NetworkRequest<Response> waitForRequestForNode(String nodeId) throws Exception {
            NetworkRequest<Response> networkRequest = pendingRequests.poll(15, TimeUnit.SECONDS);
            assertNotNull("Unable to get a request to " + nodeId + " after 15 seconds", networkRequest);
            assertThat(networkRequest.getSenderNodeId(), equalTo(nodeId));
            return networkRequest;
        }

        private void waitForRequests(int numberOfRequests) throws Exception {
            assertBusy(() -> assertThat(pendingRequestCount(), equalTo(numberOfRequests)));
        }

        private int pendingRequestCount() {
            return pendingRequests.size();
        }
    }

    private static final class FakeSearchContext implements SearchPhaseContext {
        private final SearchTransportService searchTransportService;
        private final DiscoveryNodes nodes;
        private final int numShards;
        private final List<ShardSearchFailure> failures;
        private final PlainActionFuture<SearchResponse> listener = PlainActionFuture.newFuture();

        private FakeSearchContext(SearchTransportService searchTransportService,
                                 DiscoveryNodes nodes,
                                 int numShards) {
            this.searchTransportService = searchTransportService;
            this.nodes = nodes;
            this.numShards = numShards;
            this.failures = new ArrayList<>(numShards);
        }

        @Override
        public int getNumShards() {
            return numShards;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public SearchTask getTask() {
            return new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        }

        @Override
        public SearchRequest getRequest() {
            return new SearchRequest();
        }

        @Override
        public synchronized void sendSearchResponse(InternalSearchResponse internalSearchResponse,
                                                    AtomicArray<SearchPhaseResult> queryResults) {
            if (failures.size() > 0) {
                listener.onFailure(new SearchPhaseExecutionException("",
                    "Shard failures",
                    null,
                    failures.toArray(ShardSearchFailure.EMPTY_ARRAY)));
            } else {
                SearchResponse searchResponse =
                    new SearchResponse(internalSearchResponse,
                        null,
                        numShards,
                        numShards,
                        0,
                        0,
                        ShardSearchFailure.EMPTY_ARRAY,
                        SearchResponse.Clusters.EMPTY,
                        null);
                listener.onResponse(searchResponse);
            }
        }

        @Override
        public void onFailure(Exception e) {
            listener.onFailure(e);
        }

        @Override
        public void onPhaseFailure(SearchPhase phase, String msg, Throwable cause) {
            listener.onFailure(new RuntimeException(cause));
        }

        @Override
        public synchronized void onShardFailure(int shardIndex, SearchShardTarget shardTarget, Exception e) {
            failures.add(shardIndex, new ShardSearchFailure(e, shardTarget));
        }

        @Override
        public Transport.Connection getConnection(String clusterAlias, String nodeId) {
            DiscoveryNode discoveryNode = nodes.get(nodeId);
            if (discoveryNode == null) {
                throw new IllegalStateException("Unknown node " + nodeId);
            }

            return searchTransportService.getConnection(clusterAlias, discoveryNode);
        }

        @Override
        public SearchTransportService getSearchTransport() {
            return searchTransportService;
        }

        @Override
        public ShardSearchRequest buildShardSearchRequest(SearchShardIterator shardIt) {
            return null;
        }

        @Override
        public void executeNextPhase(SearchPhase currentPhase, SearchPhase nextPhase) {

        }

        @Override
        public void addReleasable(Releasable releasable) {

        }

        @Override
        public void execute(Runnable command) {

        }

        private SearchResponse getSearchResponse() {
            return listener.actionGet(TimeValue.timeValueSeconds(5));
        }
    }

    private ShardRouting createStartedShard(ShardId shardId, boolean primary, String node) {
        ShardRouting shardRouting = ShardRouting.newUnassigned(shardId,
            primary, primary ? RecoverySource.EmptyStoreRecoverySource.INSTANCE : RecoverySource.PeerRecoverySource.INSTANCE,
            new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "index created"));
        shardRouting = ShardRoutingHelper.initialize(shardRouting, node);
        return ShardRoutingHelper.moveToStarted(shardRouting);
    }
    private void updateClusterState(Function<ClusterState, ClusterState> transformFn) {
        PlainActionFuture<Void> future = PlainActionFuture.newFuture();

        AckedRequest request = new AckedRequest() {
            @Override
            public TimeValue ackTimeout() {
                return TimeValue.timeValueSeconds(1);
            }

            @Override
            public TimeValue masterNodeTimeout() {
                return TimeValue.timeValueSeconds(1);
            }
        };

        clusterService.submitStateUpdateTask("test", new AckedClusterStateUpdateTask<Void>(request, null) {
            @Override
            protected Void newResponse(boolean acknowledged) {
                return null;
            }

            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                future.onResponse(null);
            }

            public ClusterState execute(ClusterState currentState) {
                return transformFn.apply(currentState);
            }
        });
        future.actionGet();
    }

    static class SearchShardRequest extends TransportRequest {
        private final SearchShardTarget searchShardTarget;

        SearchShardRequest(SearchShardTarget searchShardTarget) {
            this.searchShardTarget = searchShardTarget;
        }
    }

    static final class EmptyPhaseResult extends SearchPhaseResult {
    }
}
