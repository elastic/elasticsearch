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
package org.elasticsearch.indices.state;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.elasticsearch.indices.state.CloseIndexIT.assertException;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(minNumDataNodes = 2)
public class CloseWhileRelocatingShardsIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 10)
            .put(CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), -1)
            .build();
    }

    @Override
    protected int numberOfReplicas() {
        return 1;
    }

    public void testCloseWhileRelocatingShards() throws Exception {
        final String[] indices = new String[randomIntBetween(3, 10)];
        final Map<String, AtomicInteger> docsPerIndex = new HashMap<>();

        for (int i = 0; i < indices.length; i++) {
            final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
            createIndex(indexName);

            int nbDocs = 0;
            if (randomBoolean()) {
                nbDocs = randomIntBetween(1, 20);
                indexRandom(randomBoolean(), false, randomBoolean(), IntStream.range(0, nbDocs)
                    .mapToObj(n -> client().prepareIndex(indexName, "_doc", String.valueOf(n)).setSource("num", n)).collect(toList()));
                docsPerIndex.put(indexName, new AtomicInteger(nbDocs));
            }
            indices[i] = indexName;
            docsPerIndex.put(indexName, new AtomicInteger(nbDocs));
        }

        ensureGreen(indices);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE.toString())));

        final List<Thread> indexingThreads = new ArrayList<>();
        final AtomicBoolean indexing = new AtomicBoolean(true);

        // start some concurrent indexing threads
        for (final String index : indices) {
            if (randomBoolean()) {
                final Thread thread = new Thread(() -> {
                    while (indexing.get()) {
                        if (randomBoolean()) {
                            try {
                                // Single doc indexing
                                IndexResponse response = client().prepareIndex(index, "_doc").setSource("num", randomInt()).get();
                                if (response.status() == RestStatus.CREATED) {
                                    docsPerIndex.get(index).incrementAndGet();
                                }
                            } catch (final Exception e) {
                                if (indexing.get()) {
                                    assertException(e, index);
                                }
                            }
                        } else {
                            // Bulk docs indexing
                            BulkRequestBuilder request = client().prepareBulk(index, "_doc");
                            for (int j = 0; j < randomIntBetween(1, 10); j++) {
                                request.add(new IndexRequest().source("num", randomInt()));
                            }

                            BulkResponse response = request.get();
                            for (BulkItemResponse itemResponse : response) {
                                if (itemResponse.isFailed() == false) {
                                    docsPerIndex.get(index).incrementAndGet();
                                } else {
                                    if (indexing.get()) {
                                        assertException(itemResponse.getFailure().getCause(), index);
                                    }
                                }
                            }
                        }
                    }
                });
                indexingThreads.add(thread);
                thread.start();
            }
        }

        final String newNode = internalCluster().startDataOnlyNode();
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            final List<Thread> threads = new ArrayList<>();

            // start shards relocating threads
            final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
            for (final String indexToRelocate : indices) {
                final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(indexToRelocate);
                for (int i = 0; i < getNumShards(indexToRelocate).numPrimaries; i++) {
                    final int shardId = i;
                    ShardRouting primary = indexRoutingTable.shard(shardId).primaryShard();
                    assertTrue(primary.started());
                    ShardRouting replica = indexRoutingTable.shard(shardId).replicaShards().iterator().next();
                    assertTrue(replica.started());

                    final String currentNodeId = randomBoolean() ? primary.currentNodeId() : replica.currentNodeId();
                    assertNotNull(currentNodeId);

                    final Thread thread = new Thread(() -> {
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            throw new AssertionError(e);
                        }
                        assertAcked(client().admin().cluster().prepareReroute()
                            .add(new MoveAllocationCommand(indexToRelocate, shardId, currentNodeId, newNode)));
                    });
                    threads.add(thread);
                    thread.start();
                }
            }

            // start index closing threads
            for (final String indexToClose : indices) {
                final Thread thread = new Thread(() -> {
                    try {
                        latch.await();
                    } catch (InterruptedException e) {
                        throw new AssertionError(e);
                    }
                    assertAcked(client().admin().indices().prepareClose(indexToClose).get());
                });
                threads.add(thread);
                thread.start();
            }

            latch.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            indexing.set(false);
            for (Thread indexingThread : indexingThreads) {
                indexingThread.join();
            }
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey())));
        }

        for(String index : indices) {
            assertIndexIsClosed(index);

            assertAcked(client().admin().indices().prepareOpen(index).setWaitForActiveShards(ActiveShardCount.ALL));
            assertHitCount(client().prepareSearch(index).setSize(0).setFetchSource(false).get(), docsPerIndex.get(index).get());
        }
    }
}
