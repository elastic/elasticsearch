/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class BytesReadParallelCollectionIT extends ESIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(SearchService.MINIMUM_DOCS_PER_SLICE.getKey(), 1)
            .build();
    }

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testParallelCollectionCapturesWorkerThreadBytes() throws Exception {
        final String indexName = randomIndexName();
        createIndex(
            indexName,
            Settings.builder()
                .put("index.number_of_shards", 1)
                .put("index.number_of_replicas", 0)
                .put("index.requests.cache.enable", false)
                .build()
        );

        int batches = randomIntBetween(5, 10);
        for (int batch = 0; batch < batches; batch++) {
            List<IndexRequestBuilder> builders = new ArrayList<>();
            int numDocs = randomIntBetween(10, 50);
            for (int i = 0; i < numDocs; i++) {
                int docId = batch * numDocs + i;
                builders.add(prepareIndex(indexName).setSource("keyword_field", "term_" + docId, "numeric_field", docId));
            }
            indexRandom(true, false, false, builders);
        }
        flushAndRefresh(indexName);

        SearchSourceBuilder source = new SearchSourceBuilder().query(QueryBuilders.matchAllQuery())
            .aggregation(AggregationBuilders.terms("agg").field("keyword_field.keyword").size(500))
            .size(0);

        SearchRequest request = new SearchRequest(indexName).source(source).requestCache(false);

        // parallel search runs first (cold caches); if worker bytes are captured, this is the full count
        long parallelBytesRead = getBytesReadHeader(request);
        assertThat("parallel search must report bytes read", parallelBytesRead, greaterThan(0L));

        // sequential baseline runs second (warm caches); should read fewer or equal bytes
        updateClusterSettings(Settings.builder().put(SearchService.QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey(), false));
        long sequentialBytesRead = getBytesReadHeader(request);
        updateClusterSettings(Settings.builder().putNull(SearchService.QUERY_PHASE_PARALLEL_COLLECTION_ENABLED.getKey()));

        assertThat(sequentialBytesRead, greaterThan(0L));
        assertThat(
            "parallel search must report at least as many bytes as sequential",
            parallelBytesRead,
            greaterThanOrEqualTo(sequentialBytesRead)
        );
    }

    private long getBytesReadHeader(SearchRequest searchRequest) throws InterruptedException {
        SetOnce<Long> bytesRead = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        final Client client = client();

        client.search(searchRequest, new LatchedActionListener<>(ActionListener.assertOnce(ActionListener.wrap(searchResponse -> {
            Map<String, List<String>> responseHeaders = client.threadPool().getThreadContext().getResponseHeaders();
            assertThat(responseHeaders, hasKey(SearchService.BYTES_READ_RESPONSE_HEADER));
            List<String> values = responseHeaders.get(SearchService.BYTES_READ_RESPONSE_HEADER);
            assertThat(values, hasSize(1));
            assertThat("expected a single accumulated header value", values.size(), equalTo(1));
            bytesRead.set(Long.parseLong(values.get(0)));
        }, e -> { fail("no error expected"); })), latch));
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertThat(bytesRead.get(), notNullValue());
        return bytesRead.get();
    }
}
