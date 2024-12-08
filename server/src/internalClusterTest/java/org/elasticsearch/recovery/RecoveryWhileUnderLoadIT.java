/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.recovery;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.DocsStats;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.BackgroundIndexer;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoTimeout;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

public class RecoveryWhileUnderLoadIT extends ESIntegTestCase {

    public static final class RetentionLeaseSyncIntervalSettingPlugin extends Plugin {

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING);
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), RetentionLeaseSyncIntervalSettingPlugin.class);
    }

    public void testRecoverWhileUnderLoadAllocateReplicasTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate(
                "test",
                1,
                indexSettings(numberOfShards, 1).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            )
        );

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            indicesAdmin().prepareFlush().get();

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
            assertNoTimeout(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setWaitForGreenStatus()
            );

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, 10, indexer.getIds());
        }
    }

    public void testRecoverWhileUnderLoadAllocateReplicasRelocatePrimariesTest() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate(
                "test",
                1,
                indexSettings(numberOfShards, 1).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            )
        );

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            indicesAdmin().prepareFlush().get();

            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs - waitFor;
            indexer.continueIndexing(extraDocs);
            logger.info("--> allow 4 nodes for index [test] ...");
            allowNodes("test", 4);

            logger.info("--> waiting for GREEN health status ...");
            assertNoTimeout(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setWaitForGreenStatus()
            );

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", totalNumDocs);

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, 10, indexer.getIds());
        }
    }

    public void testRecoverWhileUnderLoadWithReducedAllowedNodes() throws Exception {
        logger.info("--> creating test index ...");
        int numberOfShards = numberOfShards();
        assertAcked(
            prepareCreate(
                "test",
                2,
                indexSettings(numberOfShards, 1).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
            )
        );

        final int totalNumDocs = scaledRandomIntBetween(200, 10000);
        int waitFor = totalNumDocs / 10;
        int extraDocs = waitFor;
        try (BackgroundIndexer indexer = new BackgroundIndexer("test", client(), extraDocs)) {
            logger.info("--> waiting for {} docs to be indexed ...", waitFor);
            waitForDocs(waitFor, indexer);
            indexer.assertNoFailures();
            logger.info("--> {} docs indexed", waitFor);

            extraDocs = totalNumDocs / 10;
            waitFor += extraDocs;
            indexer.continueIndexing(extraDocs);
            logger.info("--> flushing the index ....");
            // now flush, just to make sure we have some data in the index, not just translog
            indicesAdmin().prepareFlush().get();

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
            assertNoTimeout(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setWaitForGreenStatus()
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> waiting for {} docs to be indexed ...", totalNumDocs);
            waitForDocs(totalNumDocs, indexer);
            indexer.assertNoFailures();

            logger.info("--> {} docs indexed", totalNumDocs);
            // now, shutdown nodes
            logger.info("--> allow 3 nodes for index [test] ...");
            allowNodes("test", 3);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> allow 2 nodes for index [test] ...");
            allowNodes("test", 2);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> allow 1 nodes for index [test] ...");
            allowNodes("test", 1);
            logger.info("--> waiting for relocations ...");
            assertNoTimeout(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> marking and waiting for indexing threads to stop ...");
            indexer.stopAndAwaitStopped();
            logger.info("--> indexing threads stopped");

            assertNoTimeout(
                clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT)
                    .setWaitForEvents(Priority.LANGUID)
                    .setTimeout(TimeValue.timeValueMinutes(5))
                    .setWaitForNoRelocatingShards(true)
            );

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numberOfShards, 10, indexer.getIds());
        }
    }

    public void testRecoverWhileRelocating() throws Exception {
        final int numShards = between(2, 5);
        final int numReplicas = 0;
        logger.info("--> creating test index ...");
        int allowNodes = 2;
        assertAcked(
            prepareCreate(
                "test",
                3,
                indexSettings(numShards, numReplicas).put(
                    IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(),
                    Translog.Durability.ASYNC
                ).put(IndexService.RETENTION_LEASE_SYNC_INTERVAL_SETTING.getKey(), randomFrom("100ms", "1s", "5s", "30s", "60s"))
            )
        );

        final int numDocs = scaledRandomIntBetween(200, 9999);

        try (BackgroundIndexer indexer = new BackgroundIndexer("test", client(), numDocs)) {

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
            indexer.stopAndAwaitStopped();

            logger.info("--> indexing threads stopped");
            logger.info("--> bump up number of replicas to 1 and allow all nodes to hold the index");
            allowNodes("test", 3);
            setReplicaCount(1, "test");
            ensureGreen(TimeValue.timeValueMinutes(5));

            logger.info("--> refreshing the index");
            refreshAndAssert();
            logger.info("--> verifying indexed content");
            iterateAssertCount(numShards, 10, indexer.getIds());
        }
    }

    private void iterateAssertCount(final int numberOfShards, final int iterations, final Set<String> ids) throws Exception {
        final long numberOfDocs = ids.size();
        long[] iterationHitCount = new long[iterations];
        boolean[] error = new boolean[1];
        for (int i = 0; i < iterations; i++) {
            final int finalI = i;
            assertResponse(
                prepareSearch().setSize((int) numberOfDocs).setQuery(matchAllQuery()).setTrackTotalHits(true).addSort("id", SortOrder.ASC),
                response -> {
                    logSearchResponse(numberOfShards, numberOfDocs, finalI, response);
                    iterationHitCount[finalI] = response.getHits().getTotalHits().value();
                    if (iterationHitCount[finalI] != numberOfDocs) {
                        error[0] = true;
                    }
                }
            );
        }

        if (error[0]) {
            // Printing out shards and their doc count
            IndicesStatsResponse indicesStatsResponse = indicesAdmin().prepareStats().get();
            for (ShardStats shardStats : indicesStatsResponse.getShards()) {
                DocsStats docsStats = shardStats.getStats().docs;
                logger.info(
                    "shard [{}] - count {}, primary {}",
                    shardStats.getShardRouting().id(),
                    docsStats.getCount(),
                    shardStats.getShardRouting().primary()
                );
            }

            ClusterService clusterService = clusterService();
            final ClusterState state = clusterService.state();
            for (int shard = 0; shard < numberOfShards; shard++) {
                for (String id : ids) {
                    ShardId docShard = clusterService.operationRouting().shardId(state, "test", id, null);
                    if (docShard.id() == shard) {
                        final IndexShardRoutingTable indexShardRoutingTable = state.routingTable().shardRoutingTable("test", shard);
                        for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                            ShardRouting shardRouting = indexShardRoutingTable.shard(copy);
                            GetResponse response = client().prepareGet("test", id)
                                .setPreference("_only_nodes:" + shardRouting.currentNodeId())
                                .get();
                            if (response.isExists()) {
                                logger.info("missing id [{}] on shard {}", id, shardRouting);
                            }
                        }
                    }
                }
            }

            // if there was an error we try to wait and see if at some point it'll get fixed
            logger.info("--> trying to wait");
            assertBusy(() -> {
                boolean[] errorOccurred = new boolean[1];
                for (int i = 0; i < iterations; i++) {
                    assertResponse(prepareSearch().setTrackTotalHits(true).setSize(0).setQuery(matchAllQuery()), response -> {
                        if (response.getHits().getTotalHits().value() != numberOfDocs) {
                            errorOccurred[0] = true;
                        }
                    });
                }
                assertFalse("An error occurred while waiting", errorOccurred[0]);
            }, 5, TimeUnit.MINUTES);
            assertEquals(numberOfDocs, ids.size());
        }

        // lets now make the test fail if it was supposed to fail
        for (int i = 0; i < iterations; i++) {
            assertEquals(iterationHitCount[i], numberOfDocs);
        }
    }

    private void logSearchResponse(int numberOfShards, long numberOfDocs, int iteration, SearchResponse searchResponse) {
        logger.info(
            "iteration [{}] - successful shards: {} (expected {})",
            iteration,
            searchResponse.getSuccessfulShards(),
            numberOfShards
        );
        logger.info("iteration [{}] - failed shards: {} (expected 0)", iteration, searchResponse.getFailedShards());
        if (CollectionUtils.isEmpty(searchResponse.getShardFailures()) == false) {
            logger.info("iteration [{}] - shard failures: {}", iteration, Arrays.toString(searchResponse.getShardFailures()));
        }
        logger.info(
            "iteration [{}] - returned documents: {} (expected {})",
            iteration,
            searchResponse.getHits().getTotalHits().value(),
            numberOfDocs
        );
    }

    private void refreshAndAssert() throws Exception {
        assertBusy(() -> {
            BroadcastResponse actionGet = indicesAdmin().prepareRefresh().get();
            assertAllSuccessful(actionGet);
        }, 5, TimeUnit.MINUTES);
    }
}
