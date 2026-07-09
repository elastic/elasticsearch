/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.fetch;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.lessThan;

/**
 * Verifies that {@code size_in_bytes} still truncates the fetch phase when the chunked/streaming fetch path is
 * used, not just the traditional synchronous fetch path.
 * <p>
 * The two cases below exercise genuinely different code paths: a single-shard search runs query and fetch in the
 * same {@link org.elasticsearch.search.internal.SearchContext} (the "combined" optimization in
 * {@code SearchService#executeQueryPhase}), so it never goes through the chunked-fetch transport round-trip
 * regardless of {@link SearchService#FETCH_PHASE_CHUNKED_ENABLED}. Only a multi-shard search triggers a standalone
 * {@code ShardFetchRequest} per shard, which is what chunked fetch actually applies to.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0)
public class ChunkedFetchPhaseSizeInBytesIT extends ESIntegTestCase {

    private static final String INDEX_NAME = "chunked_size_in_bytes_idx";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), true)
            .build();
    }

    /**
     * Multi-shard search: fetch runs as its own round-trip per shard against a {@code ResultsType.FETCH} context
     * that never gets a {@code QuerySearchResult}, so {@code terminated_early} can't be reported here (see the
     * comment in {@code FetchPhaseDocsIterator#doIterate}) — but the hits must still be truncated, and the
     * request must not fail.
     */
    public void testSizeInBytesTruncatesWithChunkedFetchMultiShard() throws Exception {
        internalCluster().startNode();
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        int docCount = 20;
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            )
        );
        indexDocs(docCount);
        ensureGreen(INDEX_NAME);

        var searchRequestBuilder = internalCluster().client(coordinatorNode)
            .prepareSearch(INDEX_NAME)
            .setQuery(matchAllQuery())
            .setSize(docCount);
        searchRequestBuilder.request().source().sizeInBytes(ByteSizeValue.of(1, ByteSizeUnit.BYTES));

        SearchResponse response = searchRequestBuilder.get();
        try {
            assertThat(
                "chunked fetch should still truncate hits per size_in_bytes",
                response.getHits().getHits().length,
                lessThan(docCount)
            );
        } finally {
            response.decRef();
        }
    }

    /**
     * Single-shard search: query and fetch run in the same context (the "combined" optimization), so
     * {@code terminated_early} is reportable here even with chunked fetch enabled, exercising the code path
     * unchanged from the traditional (non-chunked) case.
     */
    public void testSizeInBytesReportsTerminatedEarlySingleShard() throws Exception {
        internalCluster().startNode();

        int docCount = 20;
        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            )
        );
        indexDocs(docCount);
        ensureGreen(INDEX_NAME);

        var searchRequestBuilder = internalCluster().client().prepareSearch(INDEX_NAME).setQuery(matchAllQuery()).setSize(docCount);
        searchRequestBuilder.request().source().sizeInBytes(ByteSizeValue.of(1, ByteSizeUnit.BYTES));

        SearchResponse response = searchRequestBuilder.get();
        try {
            assertThat(response.getHits().getHits().length, lessThan(docCount));
            assertEquals(Boolean.TRUE, response.isTerminatedEarly());
        } finally {
            response.decRef();
        }
    }

    private void indexDocs(int docCount) {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            builders.add(prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i));
        }
        indexRandom(true, builders);
    }
}
