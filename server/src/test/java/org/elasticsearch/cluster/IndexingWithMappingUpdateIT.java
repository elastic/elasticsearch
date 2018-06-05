package org.elasticsearch.cluster;/*
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

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.discovery.DiscoverySettings;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.disruption.ServiceDisruptionScheme;
import org.elasticsearch.test.disruption.SlowClusterStateProcessing;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0, supportsDedicatedMasters = false)
@TestLogging("org.elasticsearch.cluster:TRACE,org.elasticsearch.discovery:TRACE")
public class IndexingWithMappingUpdateIT extends ESIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
            .put(DiscoverySettings.PUBLISH_TIMEOUT_SETTING.getKey(), TimeValue.ZERO)
            .put(DiscoverySettings.COMMIT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueSeconds(1)).build();
    }

    /**
     One of potential raise condition cases that triggers RetryOnReplicaException:

     title Replication Sequence, case 1

     Client -> Primary: doc1
     Primary -> Master: Update mapping
     Master -> Primary: mapping update
     Primary -> Master: ack mapping update
     Primary -> Primary: updates local mapping

     Client -> Primary: doc2
     Primary -> Replica: doc2


     Master -> Replica: mapping update
     Replica -> Master: ack mapping update
     Master -> Primary: all nodes acked
     Primary -> Replica: doc1
     */
    public void testRaiseConditionOnFollowingDocWithMappingUpdate() throws Exception {

        final String index = "test";
        final String type = "doc";

        // dedicated master + 1 primary + 1 replica
        final String masterNodeName = internalCluster().startMasterOnlyNode();
        final int nodes = randomIntBetween(2, 10);
        final int replicas = nodes - 1;
        internalCluster().startDataOnlyNodes(nodes);

        // single primary + all other nodes are replicas
        assertAcked(admin().indices().prepareCreate(index).setSettings(Settings.builder()
            .put(IndexMetaData.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetaData.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), replicas)));

        ensureGreen();

        final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        final IndexShardRoutingTable indexShardRoutingTable = clusterState.getRoutingTable().index(index).shard(0);
        final String primaryNodeId = indexShardRoutingTable.primaryShard().currentNodeId();
        final String primaryNodeName = clusterState.nodes().get(primaryNodeId).getName();

        List<ShardRouting> replicaShards = indexShardRoutingTable.replicaShards();
        assertThat(replicaShards.size(), is(replicas));

        // pick up any victim among replicas
        final String unluckyReplicaNodeId = replicaShards.get(randomInt(replicas)).currentNodeId();
        final String unluckyReplicaNodeName = clusterState.nodes().get(unluckyReplicaNodeId).getName();

        final ServiceDisruptionScheme serviceDisruptionScheme = new SlowClusterStateProcessing(unluckyReplicaNodeName, random());
        setDisruptionScheme(serviceDisruptionScheme);
        serviceDisruptionScheme.startDisrupting();

        final int threadCount = 10;
        final int docCount = randomIntBetween(20, 50);
        final CountDownLatch readyLatch = new CountDownLatch(threadCount);
        final CountDownLatch startLatch = new CountDownLatch(1);
        final AtomicInteger nextDocIndex = new AtomicInteger();
        final List<IndexResponse> indexResponses = new CopyOnWriteArrayList<>();

        final Client client = internalCluster().client(primaryNodeName);

        final Thread[] threads = new Thread[threadCount];
        for (int i = 0; i < threadCount; i++) {
            final int threadNumber = i;
            threads[i] = new Thread(() -> {
                readyLatch.countDown();
                try {
                    startLatch.await();
                    Thread.sleep(threadNumber * 10);
                } catch (InterruptedException ignored) {
                    // ignore
                }

                int docIndex;
                while ((docIndex = nextDocIndex.incrementAndGet()) < docCount) {
                    logger.info("--> thread {} indexing doc {}", threadNumber, docIndex);
                    IndexResponse indexResponse = client.prepareIndex(index, type, "doc" + docIndex)
                        .setSource("{ \"f\": \"normal\"}", XContentType.JSON).get();
                    logger.info("--> thread {} indexing doc {} done", threadNumber, docIndex);
                    indexResponses.add(indexResponse);
                }
            });
            threads[i].setName("thread-" + i);
        }

        Arrays.stream(threads).forEach(Thread::start);
        readyLatch.await();
        startLatch.countDown();

        for (final Thread thread : threads) {
            thread.join();
        }
        serviceDisruptionScheme.stopDisrupting();

        List<String> failureMessages = indexResponses.stream()
            .map(r -> r.getShardInfo().getFailures())
            .flatMap(Arrays::stream)
            .map(failure -> failure.nodeId() + ": " + failure.reason())
            .collect(Collectors.toList());
        assertThat(failureMessages.toString(), failureMessages, hasSize(0));
    }
}
