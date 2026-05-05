/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.replication.TransportReplicationAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.search.SearchService.SLICE_SHARD_OPTIMIZATION_ENABLED;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.hamcrest.Matchers.equalTo;

public class StatelessReshardSliceIT extends AbstractStatelessPluginIntegTestCase {
    // test that a search of an index produces exactly the documents in the index,
    // when the search is composed of a full set of sliced scrolls and resharding
    // occurs between slice queries.
    public void testSliceConsistency() throws Exception {
        // start with small n shards
        final int numShards = randomIntBetween(1, 5);
        final int numDocs = randomIntBetween(10, 100);
        final int numSlices = randomIntBetween(2, numShards * 2);
        final String indexName = randomIndexName();

        final String indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        logger.info("--> creating {} docs in index [{}] with {} shards for {} slices", numDocs, indexName, numShards, numSlices);
        createIndex(indexName, numShards, 1);
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);

        indexDocsAndRefresh(indexName, numDocs);

        final int reshardBeforeSlice = randomIntBetween(1, numSlices - 1);
        final var totalHits = new AtomicInteger();
        for (int i = 0; i < numSlices; i++) {
            logger.info("--> slice {}", i);
            if (i == reshardBeforeSlice) {
                logger.info("--> resharding before slice {}", i);
                client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet(SAFE_AWAIT_TIMEOUT);
                awaitClusterState((state) -> state.metadata().indexMetadata(index).getReshardingMetadata() == null);
            }
            final var search = prepareSearch(indexName).setSize(10000).setTrackTotalHits(true).setScroll(SAFE_AWAIT_TIMEOUT);
            final var hits = getSliceHits(search, i, numSlices);
            totalHits.getAndAdd(hits);
        }

        assertEquals(numDocs, totalHits.get());
    }

    // On my laptop, this took ~800ms
    @SuppressForbidden(reason = "only for local development")
    @Ignore("only for local development")
    public void testSliceConsistencyWithShardOptimization() throws Exception {
        benchmarkSliceConsistency(true);
    }

    // and this took ~1600ms
    @SuppressForbidden(reason = "only for local development")
    @Ignore("only for local development")
    public void testSliceConsistencyWithoutShardOptimization() throws Exception {
        benchmarkSliceConsistency(false);
    }

    void benchmarkSliceConsistency(boolean shardOptimizationEnabled) throws Exception {
        // start with small n shards
        final int numShards = 10;
        final int numDocs = 100000;
        final int numSlices = numShards;
        final String indexName = randomIndexName();

        final Settings searchSettings = Settings.builder().put(SLICE_SHARD_OPTIMIZATION_ENABLED.getKey(), shardOptimizationEnabled).build();
        startMasterAndIndexNode();
        startSearchNode(searchSettings);
        ensureStableCluster(2);

        logger.info("--> creating {} docs in index [{}] with {} shards for {} slices", numDocs, indexName, numShards, numSlices);
        createIndex(indexName, numShards, 1);
        ensureGreen(indexName);

        indexDocsAndRefresh(indexName, numDocs);

        final var totalHits = new AtomicInteger();

        final var startTime = System.currentTimeMillis();
        for (int i = 0; i < numSlices; i++) {
            logger.info("--> slice {}", i);
            final var search = prepareSearch(indexName).setSize(10000).setTrackTotalHits(true).setScroll(SAFE_AWAIT_TIMEOUT);
            final var hits = getSliceHits(search, i, numSlices);
            totalHits.getAndAdd(hits);
        }
        final var endTime = System.currentTimeMillis();
        logger.info("--> took {} ms", endTime - startTime);

        assertEquals(numDocs, totalHits.get());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Test framework randomly sets this to 0, but we rely on retries to handle target shards still being in recovery
            // when we start re-splitting bulk requests.
            .put(TransportReplicationAction.REPLICATION_RETRY_TIMEOUT.getKey(), "60s")
            // These tests are carefully set up and do not hit the situations that the delete unowned grace period prevents.
            .put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO)
            // Resharding relies on not optimizing by shard. Tests are expected to fail if this is true.
            .put(SLICE_SHARD_OPTIMIZATION_ENABLED.getKey(), false);
    }

    private int getSliceHits(SearchRequestBuilder request, int slice, int numSlices) {
        int totalResults = 0;
        List<String> keys = new ArrayList<>();
        SliceBuilder sliceBuilder = new SliceBuilder(slice, numSlices);
        SearchResponse searchResponse = request.slice(sliceBuilder).get();
        try {
            totalResults += searchResponse.getHits().getHits().length;
            int expectedSliceResults = (int) searchResponse.getHits().getTotalHits().value();
            int numSliceResults = searchResponse.getHits().getHits().length;
            String scrollId = searchResponse.getScrollId();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertTrue(keys.add(hit.getId()));
            }
            while (searchResponse.getHits().getHits().length > 0) {
                searchResponse.decRef();
                searchResponse = client().prepareSearchScroll("test").setScrollId(scrollId).setScroll(TimeValue.timeValueSeconds(10)).get();
                scrollId = searchResponse.getScrollId();
                totalResults += searchResponse.getHits().getHits().length;
                numSliceResults += searchResponse.getHits().getHits().length;
                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertTrue(keys.add(hit.getId()));
                }
            }
            assertThat(numSliceResults, equalTo(expectedSliceResults));
            clearScroll(scrollId);
        } finally {
            searchResponse.decRef();
        }

        return totalResults;
    }
}
