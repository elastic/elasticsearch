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

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider.CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING;
import static org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING;
import static org.elasticsearch.indices.state.CloseIndexIT.assertException;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsClosed;
import static org.elasticsearch.indices.state.CloseIndexIT.assertIndexIsOpened;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

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
        final String[] indices = new String[randomIntBetween(1, 3)];
        final Map<String, Long> docsPerIndex = new HashMap<>();

        for (int i = 0; i < indices.length; i++) {
            final String indexName =  "index-" + i;
            createIndex(indexName);

            int nbDocs = 0;
            if (randomBoolean()) {
                nbDocs = randomIntBetween(1, 20);
                for (int j = 0; j < nbDocs; j++) {
                    IndexResponse indexResponse = client().prepareIndex(indexName, "_doc").setSource("num", j).get();
                    assertEquals(RestStatus.CREATED, indexResponse.status());
                }
            }
            docsPerIndex.put(indexName, (long) nbDocs);
            indices[i] = indexName;
        }

        ensureGreen(indices);
        assertAcked(client().admin().cluster().prepareUpdateSettings()
            .setTransientSettings(Settings.builder()
                .put(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE.toString())));

        // start some concurrent indexing threads
        final Map<String, BackgroundIndexer> indexers = new HashMap<>();
        for (final String index : indices) {
            if (randomBoolean()) {
                final BackgroundIndexer indexer = new BackgroundIndexer(index, "_doc", client(), -1, scaledRandomIntBetween(1, 3));
                waitForDocs(1, indexer);
                indexers.put(index, indexer);
            }
        }

        final Set<String> acknowledgedCloses = ConcurrentCollections.newConcurrentSet();
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
                    AcknowledgedResponse closeResponse = client().admin().indices().prepareClose(indexToClose).get();
                    if (closeResponse.isAcknowledged()) {
                        assertTrue("Index closing should not be acknowledged twice", acknowledgedCloses.add(indexToClose));
                    }
                });
                threads.add(thread);
                thread.start();
            }

            latch.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            for (Map.Entry<String, BackgroundIndexer> entry : indexers.entrySet()) {
                final BackgroundIndexer indexer = entry.getValue();
                indexer.setAssertNoFailuresOnStop(false);
                indexer.stop();

                final String indexName = entry.getKey();
                docsPerIndex.computeIfPresent(indexName, (key, value) -> value + indexer.totalIndexedDocs());

                final Throwable[] failures = indexer.getFailures();
                if (failures != null) {
                    for (Throwable failure : failures) {
                        assertException(failure, indexName);
                    }
                }
            }
        } finally {
            assertAcked(client().admin().cluster().prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(CLUSTER_ROUTING_REBALANCE_ENABLE_SETTING.getKey())));
        }

        for (String index : indices) {
            if (acknowledgedCloses.contains(index)) {
                assertIndexIsClosed(index);
            } else {
                assertIndexIsOpened(index);
            }
        }

        assertThat("Consider that the test failed if no indices were successfully closed", acknowledgedCloses.size(), greaterThan(0));
        assertAcked(client().admin().indices().prepareOpen("index-*"));
        ensureGreen(indices);

        for (String index : acknowledgedCloses) {
            long docsCount = client().prepareSearch(index).setSize(0).get().getHits().getTotalHits().value;
            assertEquals("Expected " + docsPerIndex.get(index) + " docs in index " + index + " but got " + docsCount
                + " (close acknowledged=" + acknowledgedCloses.contains(index) + ")", (long) docsPerIndex.get(index), docsCount);
        }
    }
}
