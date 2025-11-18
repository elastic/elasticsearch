/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.basic;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.discovery.AbstractDisruptionTestCase;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.test.disruption.NetworkDisruption;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;

@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS") // we sometimes have >2048 open files
public class SearchWithRandomDisconnectsIT extends AbstractDisruptionTestCase {

    public void testSearchWithRandomDisconnects() throws InterruptedException, ExecutionException {
        // make sure we have a couple data nodes
        int minDataNodes = randomIntBetween(3, 7);
        internalCluster().ensureAtLeastNumDataNodes(minDataNodes);
        final int indexCount = randomIntBetween(minDataNodes, 10 * minDataNodes);
        final String[] indexNames = IntStream.range(0, indexCount).mapToObj(i -> "test-" + i).toArray(String[]::new);
        final Settings indexSettings = indexSettings(1, 0).put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), false)
            .put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), false)
            .build();
        for (String indexName : indexNames) {
            createIndex(indexName, indexSettings);
        }
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        for (String indexName : indexNames) {
            for (int i = 0; i < randomIntBetween(1, 10); i++) {
                bulkRequestBuilder = bulkRequestBuilder.add(prepareIndex(indexName).setCreate(false).setSource("foo", "bar-" + i));
            }
        }
        assertFalse(bulkRequestBuilder.get().hasFailures());
        final AtomicBoolean done = new AtomicBoolean();
        final int concurrentSearches = randomIntBetween(2, 5);
        final List<PlainActionFuture<Void>> futures = new ArrayList<>(concurrentSearches);
        for (int i = 0; i < concurrentSearches; i++) {
            final PlainActionFuture<Void> finishFuture = new PlainActionFuture<>();
            futures.add(finishFuture);
            prepareRandomSearch().execute(new ActionListener<>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    runMoreSearches();
                }

                @Override
                public void onFailure(Exception e) {
                    runMoreSearches();
                }

                private void runMoreSearches() {
                    while (done.get() == false) {
                        final ListenableFuture<SearchResponse> f = new ListenableFuture<>();
                        prepareRandomSearch().execute(f);
                        if (f.isDone() == false) {
                            f.addListener(this);
                            return;
                        }
                    }
                    finishFuture.onResponse(null);
                }
            });
        }
        for (int i = 0, n = randomIntBetween(50, 100); i < n; i++) {
            NetworkDisruption networkDisruption = new NetworkDisruption(
                isolateNode(internalCluster().getRandomNodeName()),
                NetworkDisruption.DISCONNECT
            );
            setDisruptionScheme(networkDisruption);
            networkDisruption.startDisrupting();
            networkDisruption.stopDisrupting();
            internalCluster().clearDisruptionScheme();
            ensureFullyConnectedCluster();
        }
        done.set(true);
        for (PlainActionFuture<Void> future : futures) {
            future.get();
        }
        ensureGreen(DISRUPTION_HEALING_OVERHEAD, indexNames);
        assertAcked(indicesAdmin().prepareDelete(indexNames));
    }

    private static SearchRequestBuilder prepareRandomSearch() {
        return prepareSearch("*").setQuery(new MatchAllQueryBuilder())
            .setSize(9999)
            .setFetchSource(true)
            .setAllowPartialSearchResults(randomBoolean());
    }
}
