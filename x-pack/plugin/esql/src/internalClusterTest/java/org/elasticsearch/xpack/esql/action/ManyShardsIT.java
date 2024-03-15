/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Make sures that we can run many concurrent requests with large number of shards with any data_partitioning.
 */
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class ManyShardsIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        var plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(MockSearchService.TestPlugin.class);
        return plugins;
    }

    @Before
    public void setupIndices() {
        int numIndices = between(10, 20);
        for (int i = 0; i < numIndices; i++) {
            String index = "test-" + i;
            client().admin()
                .indices()
                .prepareCreate(index)
                .setSettings(
                    Settings.builder()
                        .put("index.shard.check_on_startup", "false")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("user", "type=keyword", "tags", "type=keyword")
                .get();
            BulkRequestBuilder bulk = client().prepareBulk(index).setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            int numDocs = between(5, 10);
            for (int d = 0; d < numDocs; d++) {
                String user = randomFrom("u1", "u2", "u3");
                String tag = randomFrom("java", "elasticsearch", "lucene");
                bulk.add(new IndexRequest().source(Map.of("user", user, "tags", tag)));
            }
            bulk.get();
        }
    }

    public void testConcurrentQueries() throws Exception {
        int numQueries = between(10, 20);
        Thread[] threads = new Thread[numQueries];
        CountDownLatch latch = new CountDownLatch(1);
        for (int q = 0; q < numQueries; q++) {
            threads[q] = new Thread(() -> {
                try {
                    assertTrue(latch.await(1, TimeUnit.MINUTES));
                } catch (InterruptedException e) {
                    throw new AssertionError(e);
                }
                final var pragmas = Settings.builder();
                if (randomBoolean() && canUseQueryPragmas()) {
                    pragmas.put(randomPragmas().getSettings())
                        .put("task_concurrency", between(1, 2))
                        .put("exchange_concurrent_clients", between(1, 2));
                }
                run("from test-* | stats count(user) by tags", new QueryPragmas(pragmas.build())).close();
            });
        }
        for (Thread thread : threads) {
            thread.start();
        }
        latch.countDown();
        for (Thread thread : threads) {
            thread.join();
        }
    }

    static class SearchContextCounter {
        private final int maxAllowed;
        private final AtomicInteger current = new AtomicInteger();

        SearchContextCounter(int maxAllowed) {
            this.maxAllowed = maxAllowed;
        }

        void onNewContext() {
            int total = current.incrementAndGet();
            assertThat("opening more shards than the limit", total, Matchers.lessThanOrEqualTo(maxAllowed));
        }

        void onContextReleased() {
            int total = current.decrementAndGet();
            assertThat(total, Matchers.greaterThanOrEqualTo(0));
        }
    }

    public void testLimitConcurrentShards() {
        Iterable<SearchService> searchServices = internalCluster().getInstances(SearchService.class);
        try {
            var queries = List.of(
                "from test-* | stats count(user) by tags",
                "from test-* | stats count(user) by tags | LIMIT 0",
                "from test-* | stats count(user) by tags | LIMIT 1",
                "from test-* | stats count(user) by tags | LIMIT 1000",
                "from test-* | LIMIT 0",
                "from test-* | LIMIT 1",
                "from test-* | LIMIT 1000",
                "from test-* | SORT tags | LIMIT 0",
                "from test-* | SORT tags | LIMIT 1",
                "from test-* | SORT tags | LIMIT 1000"
            );
            for (String q : queries) {
                QueryPragmas pragmas = randomPragmas();
                for (SearchService searchService : searchServices) {
                    SearchContextCounter counter = new SearchContextCounter(pragmas.maxConcurrentShardsPerNode());
                    var mockSearchService = (MockSearchService) searchService;
                    mockSearchService.setOnPutContext(r -> counter.onNewContext());
                    mockSearchService.setOnRemoveContext(r -> counter.onContextReleased());
                }
                run(q, pragmas).close();
            }
        } finally {
            for (SearchService searchService : searchServices) {
                var mockSearchService = (MockSearchService) searchService;
                mockSearchService.setOnPutContext(r -> {});
                mockSearchService.setOnRemoveContext(r -> {});
            }
        }
    }
}
