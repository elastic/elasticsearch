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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThan;

/**
 * Verifies that {@code size_in_bytes} still triggers early termination of the fetch phase when the
 * chunked/streaming fetch path is used, not just the traditional synchronous fetch path.
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

    public void testSizeInBytesTerminatesEarlyWithChunkedFetch() throws Exception {
        internalCluster().startNode();
        String coordinatorNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);

        assertAcked(
            prepareCreate(INDEX_NAME).setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).build()
            )
        );

        List<IndexRequestBuilder> builders = new ArrayList<>();
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            builders.add(prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource("field", "value" + i));
        }
        indexRandom(true, builders);
        ensureGreen(INDEX_NAME);

        var searchRequestBuilder = internalCluster().client(coordinatorNode)
            .prepareSearch(INDEX_NAME)
            .setQuery(matchAllQuery())
            .setSize(docCount);
        searchRequestBuilder.request().source().sizeInBytes(ByteSizeValue.of(1, ByteSizeUnit.BYTES));

        var response = searchRequestBuilder.get();
        try {
            assertThat("chunked fetch should still honor size_in_bytes", response.getHits().getHits().length, lessThan(docCount));
            assertThat(response.isTerminatedEarly(), equalTo(true));
        } finally {
            response.decRef();
        }
    }
}
