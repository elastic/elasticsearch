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
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Make sures that we can run many concurrent requests with large number of shards with any data_partitioning.
 */
@LuceneTestCase.SuppressFileSystems(value = "HandleLimitFS")
public class ManyShardsIT extends AbstractEsqlIntegTestCase {

    public void testConcurrentQueries() throws Exception {
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
                if (canUseQueryPragmas()) {
                    pragmas.put(randomPragmas().getSettings()).put("exchange_concurrent_clients", between(1, 2));
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
}
