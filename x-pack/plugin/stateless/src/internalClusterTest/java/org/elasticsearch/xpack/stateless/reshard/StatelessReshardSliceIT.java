/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.reshard;

import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportClosePointInTimeAction;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.reindex.ReindexRequestBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.junit.Ignore;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xpack.stateless.reshard.SplitSourceService.RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD;
import static org.hamcrest.Matchers.equalTo;

public class StatelessReshardSliceIT extends AbstractStatelessPluginIntegTestCase {
    // test that a search of an index produces exactly the documents in the index,
    // when the search is composed of a full set of sliced scrolls and resharding
    // occurs between slice queries.
    public void testSliceConsistency() throws Exception {
        final int numShards = randomIntBetween(1, 5);
        final int numDocs = randomIntBetween(10, 100);
        final int numSlices = randomIntBetween(2, numShards * 2);
        final String indexName = randomIndexName();
        final boolean usePit = randomBoolean();
        BytesReference pointInTimeId = null;

        final String indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        logger.info(
            "--> creating {} docs in index [{}] with {} shards for {} slices (PIT: {})",
            numDocs,
            indexName,
            numShards,
            numSlices,
            usePit
        );
        createIndex(indexName, numShards, 1);
        ensureGreen(indexName);

        final var index = resolveIndex(indexName);

        indexDocsAndRefresh(indexName, numDocs);

        if (usePit) {
            final var pointInTimeResponse = client().execute(
                TransportOpenPointInTimeAction.TYPE,
                new OpenPointInTimeRequest(indexName).keepAlive(SAFE_AWAIT_TIMEOUT)
            ).actionGet();
            assertThat(pointInTimeResponse.getSuccessfulShards(), equalTo(numShards));
            pointInTimeId = pointInTimeResponse.getPointInTimeId();
        }
        final int reshardBeforeSlice = randomIntBetween(1, numSlices - 1);
        final var keys = new HashSet<String>();
        for (int i = 0; i < numSlices; i++) {
            logger.info("--> slice {}", i);
            if (i == reshardBeforeSlice) {
                logger.info("--> resharding before slice {}", i);
                client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(indexName)).actionGet(SAFE_AWAIT_TIMEOUT);
                awaitClusterState((state) -> state.metadata().indexMetadata(index).getReshardingMetadata() == null);
            }
            final var search = prepareSearch().setSize(10000).setTrackTotalHits(true);
            if (usePit) {
                search.setPointInTime(new PointInTimeBuilder(pointInTimeId).setKeepAlive(SAFE_AWAIT_TIMEOUT))
                    .addSort(SortBuilders.fieldSort("_doc"));
                getPitSliceHits(search, i, numSlices, keys);
            } else {
                search.setIndices(indexName).setScroll(SAFE_AWAIT_TIMEOUT);
                getScrollSliceHits(search, i, numSlices, keys);
            }
        }

        if (usePit) {
            client().execute(TransportClosePointInTimeAction.TYPE, new ClosePointInTimeRequest(pointInTimeId)).actionGet();
        }

        assertEquals(numDocs, keys.size());
    }

    public void testReindexManualSliceConsistency() throws Exception {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);

        final int numShards = randomIntBetween(1, 5);
        final int numDocs = randomIntBetween(10, 100);
        final int numSlices = randomIntBetween(2, numShards * 2);
        final String sourceIndexName = randomIndexName();
        final String targetIndexName = randomIndexName();

        final String indexNode = startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        logger.info("--> creating {} docs in index [{}] with {} shards for {} slices", numDocs, sourceIndexName, numShards, numSlices);
        createIndex(sourceIndexName, numShards, 1);
        createIndex(targetIndexName, 1, 1);
        ensureGreen(sourceIndexName);
        ensureGreen(targetIndexName);

        final var sourceIndex = resolveIndex(sourceIndexName);

        indexDocsAndRefresh(sourceIndexName, numDocs);

        final int reshardBeforeSlice = randomIntBetween(1, numSlices - 1);
        for (int i = 0; i < numSlices; i++) {
            logger.info("--> slice {}", i);
            if (i == reshardBeforeSlice) {
                logger.info("--> resharding before slice {}", i);
                client(indexNode).execute(TransportReshardAction.TYPE, new ReshardIndexRequest(sourceIndexName))
                    .actionGet(SAFE_AWAIT_TIMEOUT);
                awaitClusterState((state) -> state.metadata().indexMetadata(sourceIndex).getReshardingMetadata() == null);
            }
            ReindexRequestBuilder request = new ReindexRequestBuilder(client()).source(sourceIndexName).destination(targetIndexName);
            request.source().slice(new SliceBuilder(i, numSlices));
            request.get(SAFE_AWAIT_TIMEOUT);
        }

        refresh(targetIndexName);
        assertHitCount(client().prepareSearch(targetIndexName).setSize(0), numDocs);
    }

    // On my laptop, this took ~800ms
    @SuppressForbidden(reason = "only for local development")
    @Ignore("only for local development")
    public void testSliceConsistencyWithShardOptimization() throws Exception {
        benchmarkSliceConsistency(true);
    }

    // and this took ~1600ms - 2500ms
    @SuppressForbidden(reason = "only for local development")
    @Ignore("only for local development")
    public void testSliceConsistencyWithoutShardOptimization() throws Exception {
        benchmarkSliceConsistency(false);
    }

    void benchmarkSliceConsistency(boolean shardOptimizationEnabled) throws Exception {
        final int numShards = 10;
        final int numDocs = 100000;
        final int numSlices = numShards;
        final String indexName = randomIndexName();

        final var indexVersion = shardOptimizationEnabled
            ? IndexVersionUtils.getPreviousVersion(IndexVersions.SHARD_OBLIVIOUS_SLICING)
            : IndexVersions.SHARD_OBLIVIOUS_SLICING;

        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(2);

        logger.info("--> creating {} docs in index [{}] with {} shards for {} slices", numDocs, indexName, numShards, numSlices);
        createIndex(indexName, indexSettings(indexVersion, numShards, 1).build());
        ensureGreen(indexName);

        indexDocsAndRefresh(indexName, numDocs);

        final var startTime = System.currentTimeMillis();
        final var keys = new HashSet<String>();
        for (int i = 0; i < numSlices; i++) {
            logger.info("--> slice {}", i);
            final var search = prepareSearch(indexName).setSize(10000).setTrackTotalHits(true).setScroll(SAFE_AWAIT_TIMEOUT);
            getScrollSliceHits(search, i, numSlices, keys);
        }
        final var endTime = System.currentTimeMillis();
        logger.info("--> took {} ms", endTime - startTime);

        assertEquals(numDocs, keys.size());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // These tests are carefully set up and do not hit the situations that the delete unowned grace period prevents.
            .put(RESHARD_SPLIT_DELETE_UNOWNED_GRACE_PERIOD.getKey(), TimeValue.ZERO);
    }

    // for overriding index version
    @Override
    protected boolean forbidPrivateIndexSettings() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(ReindexPlugin.class);
        return plugins;
    }

    // adds keys of all docs in the given slice of the request to keys, asserting they are unique
    private void getScrollSliceHits(SearchRequestBuilder request, int slice, int numSlices, Set<String> keys) {
        SliceBuilder sliceBuilder = new SliceBuilder(slice, numSlices);
        SearchResponse searchResponse = request.slice(sliceBuilder).get();
        try {
            int expectedSliceResults = (int) searchResponse.getHits().getTotalHits().value();
            int numSliceResults = searchResponse.getHits().getHits().length;
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                assertTrue(keys.add(hit.getId()));
            }

            String scrollId = searchResponse.getScrollId();
            while (searchResponse.getHits().getHits().length > 0) {
                searchResponse.decRef();
                searchResponse = client().prepareSearchScroll("test")
                    .setScrollId(scrollId)
                    .setScroll(TimeValue.timeValueSeconds(SAFE_AWAIT_TIMEOUT.seconds()))
                    .get();
                scrollId = searchResponse.getScrollId();
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
    }

    // adds keys of all docs in the given slice of the request to keys, asserting they are unique
    private void getPitSliceHits(SearchRequestBuilder request, int slice, int numSlices, Set<String> keys) {
        int numSliceResults = 0;

        SliceBuilder sliceBuilder = new SliceBuilder(slice, numSlices);
        SearchResponse searchResponse = request.slice(sliceBuilder).get();
        try {
            int expectedSliceResults = (int) searchResponse.getHits().getTotalHits().value();

            while (true) {
                int numHits = searchResponse.getHits().getHits().length;
                if (numHits == 0) {
                    break;
                }

                numSliceResults += numHits;

                for (SearchHit hit : searchResponse.getHits().getHits()) {
                    assertTrue(keys.add(hit.getId()));
                }

                Object[] sortValues = searchResponse.getHits().getHits()[numHits - 1].getSortValues();
                searchResponse.decRef();
                searchResponse = request.searchAfter(sortValues).get();
            }
            assertThat(numSliceResults, equalTo(expectedSliceResults));
        } finally {
            searchResponse.decRef();
        }
    }
}
