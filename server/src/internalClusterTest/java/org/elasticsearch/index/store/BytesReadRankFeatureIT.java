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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.FieldBasedRerankerIT;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests that the bytes-read response header is set when the rank feature phase is exercised.
 */
public class BytesReadRankFeatureIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), FieldBasedRerankerIT.FieldBasedRerankerPlugin.class);
    }

    @Before
    public void ensureDirectoryMetricsEnabled() {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());
    }

    public void testRankFeaturePhaseSetsBytesReadHeader() throws InterruptedException {
        final String indexName = randomIndexName();
        assertAcked(
            prepareCreate(indexName).setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping("rankFeatureField", "type=float", "searchField", "type=text")
        );
        indexRandom(
            true,
            prepareIndex(indexName).setId("1").setSource("rankFeatureField", 0.1, "searchField", "A"),
            prepareIndex(indexName).setId("2").setSource("rankFeatureField", 0.2, "searchField", "B"),
            prepareIndex(indexName).setId("3").setSource("rankFeatureField", 0.3, "searchField", "C"),
            prepareIndex(indexName).setId("4").setSource("rankFeatureField", 0.4, "searchField", "D"),
            prepareIndex(indexName).setId("5").setSource("rankFeatureField", 0.5, "searchField", "E")
        );

        SearchRequest request = new SearchRequest(indexName).source(
            new SearchSourceBuilder().query(
                QueryBuilders.boolQuery()
                    .should(QueryBuilders.matchQuery("searchField", "A"))
                    .should(QueryBuilders.matchQuery("searchField", "B"))
                    .should(QueryBuilders.matchQuery("searchField", "C"))
                    .should(QueryBuilders.matchQuery("searchField", "D"))
                    .should(QueryBuilders.matchQuery("searchField", "E"))
            ).rankBuilder(new FieldBasedRerankerIT.FieldBasedRankBuilder(10, "rankFeatureField")).size(5)
        );

        SetOnce<Long> bytesRead = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);

        final Client client = client();
        client.search(request, ActionListener.assertOnce(new ActionListener<>() {
            @Override
            public void onResponse(SearchResponse searchResponse) {
                try {
                    Map<String, List<String>> responseHeaders = client.threadPool().getThreadContext().getResponseHeaders();
                    assertThat(responseHeaders, hasKey(StoreMetrics.BYTES_READ_RESPONSE_HEADER));
                    List<String> values = responseHeaders.get(StoreMetrics.BYTES_READ_RESPONSE_HEADER);
                    assertThat("expected a single accumulated header value", values.size(), equalTo(1));
                    long total = Long.parseLong(values.get(0));
                    assertThat(total, greaterThan(0L));
                    bytesRead.set(total);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception e) {
                latch.countDown();
                fail("no error expected");
            }
        }));
        assertTrue("search did not complete in time", latch.await(30, TimeUnit.SECONDS));
        assertThat(bytesRead.get(), notNullValue());
    }
}
