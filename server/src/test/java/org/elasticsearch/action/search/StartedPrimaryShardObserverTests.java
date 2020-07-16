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

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainListenableActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class StartedPrimaryShardObserverTests extends ESTestCase {

    private void runTest(
        final ClusterState initialState,
        final CheckedBiConsumer<ClusterService, StartedPrimaryShardObserver, Exception> consumer
    ) throws Exception {
        final ThreadPool threadPool = new TestThreadPool(getTestName());
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool)) {
            ClusterServiceUtils.setState(clusterService, initialState);

            StartedPrimaryShardObserver startedPrimaryShardObserver =
                new StartedPrimaryShardObserver(clusterService, threadPool);

            consumer.accept(clusterService, startedPrimaryShardObserver);
        } finally {
            terminate(threadPool);
        }
    }

    public void testTimeoutIsTriggeredIfShardIsNotStarted() throws Exception {
        ShardId shardId = newShardId();

        ClusterState initialState = newEmptyClusterState();
        runTest(initialState, (clusterService, startedPrimaryShardObserver) -> {
            final PlainListenableActionFuture<ShardRouting> listener = PlainListenableActionFuture.newListenableFuture();
            startedPrimaryShardObserver.waitUntilPrimaryShardIsStarted(shardId, TimeValue.timeValueMillis(100), listener);

            expectThrows(ExecutionException.class, StartedPrimaryShardObserver.PrimaryShardStartTimeout.class, listener::get);
        });
    }

    public void testTimeoutIsTriggeredIfShardIsNotStartedImmediatelyWithZeroTimeout() throws Exception {
        ShardId shardId = newShardId();

        ClusterState initialState = newEmptyClusterState();
        runTest(initialState, (clusterService, startedPrimaryShardObserver) -> {
            final PlainListenableActionFuture<ShardRouting> listener = PlainListenableActionFuture.newListenableFuture();
            startedPrimaryShardObserver.waitUntilPrimaryShardIsStarted(shardId, TimeValue.ZERO, listener);

            expectThrows(ExecutionException.class, StartedPrimaryShardObserver.PrimaryShardStartTimeout.class, listener::get);
        });
    }

    public void testReturnsImmediatelyWhenPrimaryShardIsAlreadyStarted() throws Exception {
        ShardId shardId = newShardId();

        ClusterState initialState = addStartedPrimaryShardToClusterState(shardId, newEmptyClusterState());
        runTest(initialState, (clusterService, startedPrimaryShardObserver) -> {
            final PlainListenableActionFuture<ShardRouting> listener = PlainListenableActionFuture.newListenableFuture();
            startedPrimaryShardObserver.waitUntilPrimaryShardIsStarted(shardId, TimeValue.timeValueMillis(100), listener);

            ShardRouting shardRouting = listener.get(0, TimeUnit.MILLISECONDS);

            assertThat(shardRouting, is(notNullValue()));
            assertThat(shardRouting.started(), is(true));
            assertThat(shardRouting.primary(), is(true));
        });
    }

    public void testShardStartsBeforeTimeout() throws Exception {
        ShardId shardId = newShardId();

        ClusterState initialState = newEmptyClusterState();
        runTest(initialState, (clusterService, startedPrimaryShardObserver) -> {
            final PlainListenableActionFuture<ShardRouting> listener = PlainListenableActionFuture.newListenableFuture();
            startedPrimaryShardObserver.waitUntilPrimaryShardIsStarted(shardId, TimeValue.timeValueMillis(100), listener);

            ClusterState newState = addStartedPrimaryShardToClusterState(shardId, clusterService.state());
            ClusterServiceUtils.setState(clusterService, newState);

            ShardRouting shardRouting = listener.get();

            assertThat(shardRouting, is(notNullValue()));
            assertThat(shardRouting.started(), is(true));
            assertThat(shardRouting.primary(), is(true));
        });
    }

    public void testInterleavedStartingShards() throws Exception {
        ShardId shardId = newShardId();

        ClusterState initialState = newEmptyClusterState();
        runTest(initialState, (clusterService, startedPrimaryShardObserver) -> {
            final PlainListenableActionFuture<ShardRouting> listener = PlainListenableActionFuture.newListenableFuture();
            startedPrimaryShardObserver.waitUntilPrimaryShardIsStarted(shardId, TimeValue.timeValueMillis(1000), listener);

            ClusterState clusterState = clusterService.state();

            for (int i = 0; i < randomIntBetween(1, 5); i++) {
                clusterState = addStartedPrimaryShardToClusterState(newShardId(), clusterState);
                ClusterServiceUtils.setState(clusterService, clusterState);
            }

            clusterState = addStartedPrimaryShardToClusterState(shardId, clusterService.state());
            ClusterServiceUtils.setState(clusterService, clusterState);

            ShardRouting shardRouting = listener.get();

            assertThat(shardRouting, is(notNullValue()));
            assertThat(shardRouting.started(), is(true));
            assertThat(shardRouting.primary(), is(true));
        });
    }

    public void testCloseClusterService() throws Exception {
        ShardId shardId = newShardId();

        ClusterState initialState = newEmptyClusterState();
        runTest(initialState, (clusterService, startedPrimaryShardObserver) -> {
            final PlainListenableActionFuture<ShardRouting> listener = PlainListenableActionFuture.newListenableFuture();
            startedPrimaryShardObserver.waitUntilPrimaryShardIsStarted(shardId, TimeValue.timeValueMillis(100), listener);

            clusterService.close();

            expectThrows(ExecutionException.class, NodeClosedException.class, listener::get);
        });
    }

    private ShardId newShardId() {
        Index index = new Index(randomAlphaOfLength(10), randomAlphaOfLength(4));
        return new ShardId(index, randomInt(10));
    }

    private ClusterState newEmptyClusterState() {
        final DiscoveryNode node = new DiscoveryNode(randomAlphaOfLength(10), buildNewFakeTransportAddress(), Version.CURRENT);
        return ClusterState.builder(new ClusterName(getTestName()))
            .version(0L)
            .nodes(DiscoveryNodes.builder().add(node).localNodeId(node.getId()).masterNodeId(node.getId())).build();
    }

    private ClusterState addStartedPrimaryShardToClusterState(ShardId shardId, ClusterState state) {
        DiscoveryNode localNode = state.nodes().getLocalNode();
        ShardRouting primaryShard = TestShardRouting.newShardRouting(shardId, localNode.getId(), true, ShardRoutingState.STARTED);
        IndexShardRoutingTable indexShardRoutingTable = new IndexShardRoutingTable.Builder(shardId).addShard(primaryShard).build();
        IndexRoutingTable indexRoutingTable = IndexRoutingTable.builder(shardId.getIndex()).addIndexShard(indexShardRoutingTable).build();
        RoutingTable routingTable = RoutingTable.builder().add(indexRoutingTable).build();

        return ClusterState.builder(state).routingTable(routingTable).build();
    }
}
