/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilders;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests that resharding operations are resilient in presence of failures.
 */
public class StatelessReshardDisruptionIT extends StatelessReshardDisruptionBaseIT {
    public void testReshardWithDisruption() throws InterruptedException, ExecutionException {
        var masterNode = startMasterOnlyNode();

        int shards = randomIntBetween(1, 5);

        int indexNodes = randomIntBetween(1, shards * 2);
        startIndexNodes(indexNodes);
        int searchNodes = randomIntBetween(1, shards * 2);
        startSearchNodes(searchNodes);

        int clusterSize = indexNodes + searchNodes + 1;
        ensureStableCluster(clusterSize);

        final String indexName = randomIndexName();
        createIndex(indexName, indexSettings(shards, 1).build());
        ensureGreen(indexName);
        Index index = resolveIndex(indexName);

        checkNumberOfShardsSetting(indexName, shards);

        var multiple = 2;

        final int numDocs = randomIntBetween(10, 100);
        indexDocs(indexName, numDocs);

        try (var disruptionExecutorService = Executors.newSingleThreadExecutor()) {
            logger.info("--> start inducing failures");

            var failuresStarted = new CountDownLatch(1);
            var stop = new AtomicBoolean(false);
            var disruptionFuture = disruptionExecutorService.submit(() -> {
                failuresStarted.countDown();
                do {
                    Failure randomFailure = randomFrom(Failure.values());
                    try {
                        induceFailure(randomFailure, index, null);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                } while (stop.get() == false);
            });

            failuresStarted.await();

            try {
                logger.info("--> resharding an index [{}] under disruption", indexName);
                client(masterNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet(TEST_REQUEST_TIMEOUT);
                waitForReshardCompletion(index);
                logger.info("--> done resharding an index [{}]", indexName);
            } finally {
                stop.set(true);
                disruptionFuture.get();
            }

            checkNumberOfShardsSetting(indexName, shards * multiple);

            ensureGreen(indexName);

            // Explicit refresh currently needed because it is not done in scope of split.
            refresh(indexName);

            // All data movement is done properly.
            var search = prepareSearch(indexName).setAllowPartialSearchResults(false)
                .setQuery(QueryBuilders.matchAllQuery())
                .setTrackTotalHits(true)
                .setSize(numDocs * 2);
            assertResponse(search, r -> assertEquals(numDocs, r.getHits().getTotalHits().value()));
        }
    }

    @Override
    protected boolean addMockFsRepository() {
        // Use FS repository because it supports blob copy
        return false;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        // These tests are not performing writes and do not need the grace period.
        return super.nodeSettings().put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO);
    }

    private static void checkNumberOfShardsSetting(String indexName, int expected_shards) {
        assertThat(
            IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.get(
                client().admin()
                    .indices()
                    .prepareGetSettings(TEST_REQUEST_TIMEOUT, indexName)
                    .execute()
                    .actionGet()
                    .getIndexToSettings()
                    .get(indexName)
            ),
            equalTo(expected_shards)
        );
    }

    private void waitForReshardCompletion(Index index) {
        awaitClusterState((state) -> state.metadata().findIndex(index).get().getReshardingMetadata() == null);
    }
}
