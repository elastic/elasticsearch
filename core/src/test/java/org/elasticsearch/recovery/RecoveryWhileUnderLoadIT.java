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

package org.elasticsearch.recovery;

import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.index.translog.TranslogConfig;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;

public class RecoveryWhileUnderLoadIT extends ESIntegTestCase {

    private final ESLogger logger = Loggers.getLogger(RecoveryWhileUnderLoadIT.class);

    @Test
    public void recoverWhileUnderLoadAllocateReplicasTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test", 1, settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).put(TranslogConfig.INDEX_TRANSLOG_DURABILITY, Translog.Durabilty.ASYNC)));

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            client().admin().indices().prepareFlush().execute().actionGet();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);

            logger.info("--> allow 2 nodes for index [test] ...");
            // now start another node, while we index
            allowNodes("test", 2);

            logger.info("--> waiting for GREEN health status ...");
            // make sure the cluster state is green, and all has been recovered
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus());

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, indexer.totalIndexedDocs(), 10);
        }
    }

    @Test
    public void recoverWhileUnderLoadAllocateReplicasRelocatePrimariesTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test", 1, settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).put(TranslogConfig.INDEX_TRANSLOG_DURABILITY, Translog.Durabilty.ASYNC)));

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            client().admin().indices().prepareFlush().execute().actionGet();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 4 nodes for index [test] ...");
            allowNodes("test", 4);

            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus());


            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, indexer.totalIndexedDocs(), 10);
        }
    }

    @Test
    public void recoverWhileUnderLoadWithReducedAllowedNodes() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(prepareCreate("test", 2, settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, numberOfShards).put(SETTING_NUMBER_OF_REPLICAS, 1).put(TranslogConfig.INDEX_TRANSLOG_DURABILITY, Translog.Durabilty.ASYNC)));

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            client().admin().indices().prepareFlush().execute().actionGet();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            // now start more nodes, while we index
            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 4 nodes for index [test] ...");
            allowNodes("test", 4);

            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForGreenStatus().setWaitForRelocatingShards(0));

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();

            logger.info("--> {} docs indexed", totalNumDocs);
            // now, shutdown nodes
            logger.info("--> allow 3 nodes for index [test] ...");
            allowNodes("test", 3);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));

            logger.info("--> allow 2 nodes for index [test] ...");
            allowNodes("test", 2);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));

            logger.info("--> allow 1 nodes for index [test] ...");
            allowNodes("test", 1);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();
            logger.info("--> indexing threads stopped");

            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).setTimeout("5m").setWaitForRelocatingShards(0));

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, indexer.totalIndexedDocs(), 10);
        }
    }

    @Test
    public void recoverWhileRelocating() throws Exception {
        final int numShards = between(2, 10);
        final int numReplicas = 0;
        logger.info("--> creating test index ...");
        int allowNodes = 2;
        assertAcked(prepareCreate("test", 3, settingsBuilder().put(SETTING_NUMBER_OF_SHARDS, numShards).put(SETTING_NUMBER_OF_REPLICAS, numReplicas).put(TranslogConfig.INDEX_TRANSLOG_DURABILITY, Translog.Durabilty.ASYNC)));

        final int numDocs = scaledRandomIntBetween(200, 20000);

        try (BackgroundIndexer indexer = new BackgroundIndexer("test", "type", client(), numDocs)) {

            for (int i = 0; i < numDocs; i += scaledRandomIntBetween(100, Math.min(1000, numDocs))) {
                indexer.assertNoFailures();
                logger.info("--> waiting for {} docs to be indexed ...", i);
                waitForDocs(i, indexer);
                logger.info("--> {} docs indexed", i);
                allowNodes = 2 / allowNodes;
                allowNodes("test", allowNodes);
                logger.info("--> waiting for GREEN health status ...");
                ensureGreen(TimeValue.timeValueMinutes(5));
            }

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stop();

            logger.info("--> indexing threads stopped");
            logger.info("--> bump up number of replicas to 1 and allow all nodes to hold the index");
            allowNodes("test", 3);
            assertAcked(client().admin().indices().prepareUpdateSettings("test").setSettings(settingsBuilder().put("number_of_replicas", 1)).get());
            ensureGreen(TimeValue.timeValueMinutes(5));

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numShards, indexer.totalIndexedDocs(), 10);
        }
    }

    private void iterateAssertCount(final int numberOfShards, final long numberOfDocs, final int iterations) throws Exception {
        SearchResponse[] iterationResults = new SearchResponse[iterations];
        boolean error = false;
        for (int i = 0; i < iterations; i++) {
            SearchResponse searchResponse = client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get();
            logSearchResponse(numberOfShards, numberOfDocs, i, searchResponse);
            iterationResults[i] = searchResponse;
            if (searchResponse.getHits().totalHits() != numberOfDocs) {
                error = true;
            }
        }

        if (error) {
            //Printing out shards and their doc count
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats().get();
            for (ShardStats shardStats : indicesStatsResponse.getShards()) {
                DocsStats docsStats = shardStats.getStats().docs;
                logger.info("shard [{}] - count {}, primary {}", shardStats.getShardRouting().id(), docsStats.getCount(), shardStats.getShardRouting().primary());
            }

            //if there was an error we try to wait and see if at some point it'll get fixed
            logger.info("--> trying to wait");
            assertTrue(awaitBusy(() -> {
                                boolean errorOccurred = false;
                                for (int i = 0; i < iterations; i++) {
                                    SearchResponse searchResponse = client().prepareSearch().setSize(0).setQuery(matchAllQuery()).get();
                                    if (searchResponse.getHits().totalHits() != numberOfDocs) {
                                        errorOccurred = true;
                                    }
                                }
                                return !errorOccurred;
                            },
                            5,
                            TimeUnit.MINUTES
                    )
            );
        }

        //lets now make the test fail if it was supposed to fail
        for (int i = 0; i < iterations; i++) {
            assertHitCount(iterationResults[i], numberOfDocs);
        }
    }

    private void logSearchResponse(int numberOfShards, long numberOfDocs, int iteration, SearchResponse searchResponse) {
        logger.info("iteration [{}] - successful shards: {} (expected {})", iteration, searchResponse.getSuccessfulShards(), numberOfShards);
        logger.info("iteration [{}] - failed shards: {} (expected 0)", iteration, searchResponse.getFailedShards());
        if (searchResponse.getShardFailures() != null && searchResponse.getShardFailures().length > 0) {
            logger.info("iteration [{}] - shard failures: {}", iteration, Arrays.toString(searchResponse.getShardFailures()));
        }
        logger.info("iteration [{}] - returned documents: {} (expected {})", iteration, searchResponse.getHits().totalHits(), numberOfDocs);
    }

    private void refreshAndAssert() throws Exception {
        assertBusy(new Runnable() {
            @Override
            public void run() {
                RefreshResponse actionGet = client().admin().indices().prepareRefresh().get();
                assertAllSuccessful(actionGet);
            }
        }, 5, TimeUnit.MINUTES);
    }
}
