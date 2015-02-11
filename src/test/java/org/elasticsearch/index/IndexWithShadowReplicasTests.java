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

package org.elasticsearch.index;

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.store.MockFSDirectoryService;
import org.junit.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for indices that use shadow replicas and a shared filesystem
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class IndexWithShadowReplicasTests extends ElasticsearchIntegrationTest {

    @Test
    @TestLogging("_root:DEBUG,env:TRACE")
    public void testIndexWithFewDocuments() throws Exception {
        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                .build();

        String node1 = internalCluster().startNode(nodeSettings);
        String node2 = internalCluster().startNode(nodeSettings);
        String node3 = internalCluster().startNode(nodeSettings);

        final String IDX = "test";
        final Path dataPath = newTempDirPath();

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 2)
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .build();

        prepareCreate(IDX).setSettings(idxSettings).get();
        ensureGreen(IDX);

        // So basically, the primary should fail and the replica will need to
        // replay the translog, this is what this tests
        client().prepareIndex(IDX, "doc", "1").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "2").setSource("foo", "bar").get();
        flushAndRefresh(IDX);
        client().prepareIndex(IDX, "doc", "3").setSource("foo", "bar").get();
        client().prepareIndex(IDX, "doc", "4").setSource("foo", "bar").get();
        refresh();

        // Check that we can get doc 1 and 2
        GetResponse gResp1 = client().prepareGet(IDX, "doc", "1").setFields("foo").get();
        GetResponse gResp2 = client().prepareGet(IDX, "doc", "2").setFields("foo").get();
        assertThat(gResp1.getField("foo").getValue().toString(), equalTo("bar"));
        assertThat(gResp2.getField("foo").getValue().toString(), equalTo("bar"));

        logger.info("--> restarting both nodes");
        if (randomBoolean()) {
            logger.info("--> rolling restart");
            internalCluster().rollingRestart();
        } else {
            logger.info("--> full restart");
            internalCluster().fullRestart();
        }

        client().admin().cluster().prepareHealth().setWaitForNodes("3").get();
        ensureGreen(IDX);

        logger.info("--> performing query");
        SearchResponse resp = client().prepareSearch(IDX).setQuery(matchAllQuery()).get();
        assertHitCount(resp, 4);

        logger.info("--> deleting index");
        assertAcked(client().admin().indices().prepareDelete(IDX));
    }

    @Test
    @LuceneTestCase.Slow
    public void testChaosMonkeyWithShadowReplicas() throws Exception {
        final int initialNodeCount = scaledRandomIntBetween(3, 8);

        Settings nodeSettings = ImmutableSettings.builder()
                .put("node.add_id_to_custom_path", false)
                .put("node.enable_custom_paths", true)
                // We don't want closing a node to take forever, so disable this
                .put(MockFSDirectoryService.CHECK_INDEX_ON_CLOSE, false)
                .put("discovery.zen.minimum_master_nodes", (initialNodeCount / 2) + 1)
                .build();

        logger.info("--> starting up {} nodes...", initialNodeCount);
        final List<String> nodes = internalCluster().startNodesAsync(initialNodeCount, nodeSettings).get();

        // Start up a client node
        Settings clientNodeSettings = ImmutableSettings.builder()
                .put("node.client", true)
                .build();
        String clientNode = internalCluster().startNode(clientNodeSettings);
        final Client client = client(clientNode);

        final String IDX = "test";
        final String NORMAL = "normal"; // the normal index setting
        final Path dataPath = newTempDirPath();

        Settings idxSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, initialNodeCount))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, initialNodeCount - 1))
                .put(IndexMetaData.SETTING_DATA_PATH, dataPath.toAbsolutePath().toString())
                .put(IndexMetaData.SETTING_SHADOW_REPLICAS, true)
                .build();

        Settings normalSettings = ImmutableSettings.builder()
                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, initialNodeCount))
                .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, randomIntBetween(1, initialNodeCount - 1))
                .build();

        prepareCreate(IDX).setSettings(idxSettings).get();
        prepareCreate(NORMAL).setSettings(normalSettings).get();
        ensureGreen(TimeValue.timeValueMinutes(1), IDX, NORMAL);

        // Either we're going to fiddle with replicas, or we're going to
        // restart nodes, but not both
        final boolean fiddleWithReplicas = randomBoolean();

        // Flag to signle to the threads to stop doing things
        final AtomicBoolean running = new AtomicBoolean(true);

        final Runnable chaosRunner = new Runnable() {
            @Override
            public void run() {
                try {
                    while (running.get()) {
                        Thread.sleep(randomIntBetween(4000, 9000));
                        // Randomly either restart nodes and change replica count
                        if (fiddleWithReplicas) {
                            int newCount = randomIntBetween(1, initialNodeCount - 1);
                            logger.info("--> changing replica count to {}", newCount);
                            assertAcked(client.admin().indices().prepareUpdateSettings(IDX)
                                    .setSettings(ImmutableSettings.builder()
                                            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS,
                                                    newCount)));
                        } else {
                            logger.info("--> randomly restarting a data node");
                            internalCluster().restartRandomDataNode();
                        }
                    }
                } catch (Throwable t) {
                    logger.warn("exception in chaos monkey", t);
                }
            }
        };

        final LongAdder totalDocs = new LongAdder();
        final Runnable indexRunner = new Runnable() {
            @Override
            public void run() {
                while (running.get()) {
                    List<IndexRequestBuilder> indexRequests = newArrayList();
                    int batchSize = scaledRandomIntBetween(10, 100);
                    for (int i = 0; i < batchSize; i++) {
                        indexRequests.add(client.prepareIndex()
                                .setIndex(randomBoolean() ? IDX : NORMAL) // Randomly use different index
                                .setType("doc")
                                .setSource("body", randomRealisticUnicodeOfCodepointLengthBetween(10, 20))
                        );
                    }
                    try {
                        logger.info("--> indexing batch of {} documents...", batchSize);
                        indexRandom(true, true, true, indexRequests);
                        totalDocs.add(batchSize);
                        if (randomBoolean()) {
                            // Randomly flush the shadow index
                            flush(IDX);
                        }
                    } catch (Throwable t) {
                        logger.info("--> wasn't able to index that batch, we'll get 'em next time", t);
                    }
                }
            }
        };

        Thread chaosMonkey = new Thread(chaosRunner);
        Thread indexingThread = new Thread(indexRunner);

        logger.info("--> starting indexing thread and chaos monkey");
        indexingThread.start();
        chaosMonkey.start();

        try {
            // Give it 30 seconds to index and restart nodes randomly
            Thread.sleep(30 * 1000);

            logger.info("--> stopping indexing and chaos thread...");
            running.getAndSet(false);
            chaosMonkey.join(15 * 1000);
            indexingThread.join(15 * 1000);
        } catch (Throwable t) {
            fail("failed to go to sleep");
        }

        logger.info("--> waiting for cluster to recover");
        client.admin().cluster().prepareHealth().setWaitForNodes(initialNodeCount + 1 + "").get();
        logger.info("--> waiting for green");
        ensureGreen(IDX, NORMAL);

        logger.info("--> flushing indices...");
        flush(IDX, NORMAL);
        logger.info("--> refreshing indices...");
        refresh();

        logger.info("--> expecting [{}] total documents", totalDocs.longValue());

        SearchResponse resp = client.prepareSearch(IDX, NORMAL).setSearchType(SearchType.COUNT).setQuery(matchAllQuery()).get();
        assertThat("there should be " + totalDocs.longValue() + " documents that were indexed",
                resp.getHits().totalHits(),
                equalTo(totalDocs.longValue()));
    }
}
