/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.GroupShardsIterator;
import org.elasticsearch.cluster.routing.PlainShardIterator;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardIterator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchTransportRequest;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class SearchAsyncActionTests extends ESTestCase {

    public void testFanOutAndCollect() throws InterruptedException {
        SearchRequest request = new SearchRequest();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TestSearchResponse> response = new AtomicReference<>();
        ActionListener<SearchResponse> responseListener = new ActionListener<SearchResponse>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                response.set((TestSearchResponse) searchResponse);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("test failed", e);
                fail(e.getMessage());
            }
        };
        DiscoveryNode primaryNode = new DiscoveryNode("node_1", new LocalTransportAddress("foo"), Version.CURRENT);
        DiscoveryNode replicaNode = new DiscoveryNode("node_2", new LocalTransportAddress("bar"), Version.CURRENT);

        Map<DiscoveryNode, Set<Long>> nodeToContextMap = new HashMap<>();
        AtomicInteger contextIdGenerator = new AtomicInteger(0);
        GroupShardsIterator shardsIter = getShardsIter("idx", randomIntBetween(1, 10), randomBoolean(), primaryNode, replicaNode);
        AtomicInteger numFreedContext = new AtomicInteger();
        SearchTransportService transportService = new SearchTransportService(Settings.EMPTY, null) {
            @Override
            public void sendFreeContext(DiscoveryNode node, long contextId, SearchRequest request) {
                numFreedContext.incrementAndGet();
                assertTrue(nodeToContextMap.containsKey(node));
                assertTrue(nodeToContextMap.get(node).remove(contextId));
            }
        };
        Map<String, DiscoveryNode> lookup = new HashMap<>();
        lookup.put(primaryNode.getId(), primaryNode);
        AbstractSearchAsyncAction asyncAction = new AbstractSearchAsyncAction<TestSearchPhaseResult>(logger, transportService, lookup::get,
            Collections.emptyMap(), null, request, responseListener, shardsIter, 0, 0, null) {
            TestSearchResponse response = new TestSearchResponse();

            @Override
            protected void sendExecuteFirstPhase(DiscoveryNode node, ShardSearchTransportRequest request, ActionListener listener) {
                assertTrue("shard: " + request.shardId() + " has been queried twice", response.queried.add(request.shardId()));
                TestSearchPhaseResult testSearchPhaseResult = new TestSearchPhaseResult(contextIdGenerator.incrementAndGet(), node);
                Set<Long> ids = nodeToContextMap.computeIfAbsent(node, (n) -> new HashSet<>());
                ids.add(testSearchPhaseResult.id);
                if (randomBoolean()) {
                    listener.onResponse(testSearchPhaseResult);
                } else {
                    new Thread(() -> listener.onResponse(testSearchPhaseResult)).start();
                }
            }

            @Override
            protected void moveToSecondPhase() throws Exception {
                for (int i = 0; i < firstResults.length(); i++) {
                    TestSearchPhaseResult result = firstResults.get(i);
                    assertEquals(result.node.getId(), result.shardTarget().getNodeId());
                    sendReleaseSearchContext(result.id(), result.node);
                }
                responseListener.onResponse(response);
                latch.countDown();
            }

            @Override
            protected String firstPhaseName() {
                return "test";
            }

            @Override
            protected Executor getExecutor() {
                fail("no executor in this class");
                return null;
            }
        };
        asyncAction.start();
        latch.await();
        assertNotNull(response.get());
        assertFalse(nodeToContextMap.isEmpty());
        assertTrue(nodeToContextMap.containsKey(primaryNode));
        assertEquals(shardsIter.size(), numFreedContext.get());
        assertTrue(nodeToContextMap.get(primaryNode).toString(), nodeToContextMap.get(primaryNode).isEmpty());

    }

    private GroupShardsIterator getShardsIter(String index, int numShards, boolean doReplicas, DiscoveryNode primaryNode,
                                              DiscoveryNode replicaNode) {
        ArrayList<ShardIterator> list = new ArrayList<>();
        for (int i = 0; i < numShards; i++) {
            ArrayList<ShardRouting> started = new ArrayList<>();
            ArrayList<ShardRouting> initializing = new ArrayList<>();
            ArrayList<ShardRouting> unassigned = new ArrayList<>();

            ShardRouting routing = ShardRouting.newUnassigned(new ShardId(new Index(index, "_na_"), i), true,
                RecoverySource.StoreRecoverySource.EMPTY_STORE_INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"));
            routing = routing.initialize(primaryNode.getId(), i + "p", 0);
            routing.started();
            started.add(routing);
            if (doReplicas) {
                routing = ShardRouting.newUnassigned(new ShardId(new Index(index, "_na_"), i), false,
                    RecoverySource.PeerRecoverySource.INSTANCE, new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "foobar"));
                if (replicaNode != null) {
                    routing = routing.initialize(replicaNode.getId(), i + "r", 0);
                    if (randomBoolean()) {
                        routing.started();
                        started.add(routing);
                    } else {
                        initializing.add(routing);
                    }
                } else {
                    unassigned.add(routing); // unused yet
                }
            }
            Collections.shuffle(started, random());
            started.addAll(initializing);
            list.add(new PlainShardIterator(new ShardId(new Index(index, "_na_"), i), started));
        }
        return new GroupShardsIterator(list);
    }

    public static class TestSearchResponse extends SearchResponse {
        public final Set<ShardId> queried = new HashSet<>();
    }

    public static class TestSearchPhaseResult implements SearchPhaseResult {
        final long id;
        final DiscoveryNode node;
        SearchShardTarget shardTarget;

        public TestSearchPhaseResult(long id, DiscoveryNode node) {
            this.id = id;
            this.node = node;
        }

        @Override
        public long id() {
            return id;
        }

        @Override
        public SearchShardTarget shardTarget() {
            return this.shardTarget;
        }

        @Override
        public void shardTarget(SearchShardTarget shardTarget) {
            this.shardTarget = shardTarget;

        }

        @Override
        public void readFrom(StreamInput in) throws IOException {

        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {

        }
    }
}
