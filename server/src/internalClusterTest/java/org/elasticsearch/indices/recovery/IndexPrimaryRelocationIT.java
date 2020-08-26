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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndexPrimaryRelocationIT extends ESIntegTestCase {

    private static final int RELOCATION_COUNT = 15;

    public void testPrimaryRelocationWhileIndexing() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(randomIntBetween(2, 3));
        client().admin().indices().prepareCreate("test")
            .setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
            .setMapping("field", "type=text")
            .get();
        ensureGreen("test");
        AtomicInteger numAutoGenDocs = new AtomicInteger();
        final AtomicBoolean finished = new AtomicBoolean(false);
        Thread indexingThread = new Thread() {
            @Override
            public void run() {
                while (finished.get() == false && numAutoGenDocs.get() < 10_000) {
                    IndexResponse indexResponse = client().prepareIndex("test").setId("id").setSource("field", "value").get();
                    assertEquals(DocWriteResponse.Result.CREATED, indexResponse.getResult());
                    DeleteResponse deleteResponse = client().prepareDelete("test", "id").get();
                    assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());
                    client().prepareIndex("test").setSource("auto", true).get();
                    numAutoGenDocs.incrementAndGet();
                }
            }
        };
        indexingThread.start();

        ClusterState initialState = client().admin().cluster().prepareState().get().getState();
        DiscoveryNode[] dataNodes = initialState.getNodes().getDataNodes().values().toArray(DiscoveryNode.class);
        DiscoveryNode relocationSource = initialState.getNodes().getDataNodes().get(initialState.getRoutingTable()
            .shardRoutingTable("test", 0).primaryShard().currentNodeId());
        for (int i = 0; i < RELOCATION_COUNT; i++) {
            DiscoveryNode relocationTarget = randomFrom(dataNodes);
            while (relocationTarget.equals(relocationSource)) {
                relocationTarget = randomFrom(dataNodes);
            }
            logger.info("--> [iteration {}] relocating from {} to {} ", i, relocationSource.getName(), relocationTarget.getName());
            client().admin().cluster().prepareReroute()
                .add(new MoveAllocationCommand("test", 0, relocationSource.getId(), relocationTarget.getId()))
                .execute().actionGet();
            ClusterHealthResponse clusterHealthResponse = client().admin().cluster().prepareHealth()
                .setTimeout(TimeValue.timeValueSeconds(60))
                .setWaitForEvents(Priority.LANGUID).setWaitForNoRelocatingShards(true).execute().actionGet();
            if (clusterHealthResponse.isTimedOut()) {
                final String hotThreads = client().admin().cluster().prepareNodesHotThreads().setIgnoreIdleThreads(false).get().getNodes()
                    .stream().map(NodeHotThreads::getHotThreads).collect(Collectors.joining("\n"));
                final ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
                logger.info("timed out for waiting for relocation iteration [{}] \ncluster state {} \nhot threads {}",
                    i, clusterState, hotThreads);
                finished.set(true);
                indexingThread.join();
                throw new AssertionError("timed out waiting for relocation iteration [" + i + "] ");
            }
            logger.info("--> [iteration {}] relocation complete", i);
            relocationSource = relocationTarget;
            // indexing process aborted early, no need for more relocations as test has already failed
            if (indexingThread.isAlive() == false) {
                break;
            }
            if (i > 0  && i % 5 == 0) {
                logger.info("--> [iteration {}] flushing index", i);
                client().admin().indices().prepareFlush("test").get();
            }
        }
        finished.set(true);
        indexingThread.join();
        refresh("test");
        ElasticsearchAssertions.assertHitCount(client().prepareSearch("test").setTrackTotalHits(true).get(), numAutoGenDocs.get());
        ElasticsearchAssertions.assertHitCount(client().prepareSearch("test").setTrackTotalHits(true)// extra paranoia ;)
            .setQuery(QueryBuilders.termQuery("auto", true)).get(), numAutoGenDocs.get());
    }

}
