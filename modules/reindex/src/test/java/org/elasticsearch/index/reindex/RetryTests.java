/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.reindex;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.ListenableActionFuture;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.bulk.Retry;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.MockSearchService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.IntFunction;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Integration test for retry behavior. Useful because retrying relies on the way that the rest of Elasticsearch throws exceptions and unit
 * tests won't verify that.
 */
@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/18456")
public class RetryTests extends ReindexTestCase {
    /**
     * The number of concurrent requests to test.
     */
    private static final int CONCURRENT = 12;
    /**
     * Enough docs that the requests will likely step on each other.
     */
    private static final int DOC_COUNT = 200;

    /**
     * Lower the queue sizes to be small enough that both bulk and searches will time out and have to be retried.
     */
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put("threadpool.bulk.queue_size", 1);
        settings.put("threadpool.bulk.size", 1);
        settings.put("threadpool.search.queue_size", 1);
        settings.put("threadpool.search.size", 1);
        return settings.build();
    }

    /**
     * Disable search context leak detection because we expect leaks when there is an {@link EsRejectedExecutionException} queueing the
     * reduce phase.
     */
    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        List<Class<? extends Plugin>> mockPlugins = new ArrayList<>();
        for (Class<? extends Plugin> plugin: super.getMockPlugins()) {
            if (plugin.equals(MockSearchService.TestPlugin.class)) {
                continue;
            }
            mockPlugins.add(plugin);
        }
        return mockPlugins;
    }

    public void testReindex() throws Exception {
        setupSourceIndex("source");
        testCase(true, i -> reindex().source("source").destination("dest" + i));
    }

    public void testUpdateByQuery() throws Exception {
        for (int i = 0; i < CONCURRENT; i++) {
            setupSourceIndex("source" + i);
        }
        testCase(false, i -> updateByQuery().source("source" + i));
    }

    private void testCase(boolean expectCreated, IntFunction<AbstractBulkIndexByScrollRequestBuilder<?, ?>> requestBuilder)
            throws Exception {
        List<ListenableActionFuture<BulkIndexByScrollResponse>> futures = new ArrayList<>(CONCURRENT);
        for (int i = 0; i < CONCURRENT; i++) {
            AbstractBulkIndexByScrollRequestBuilder<?, ?> request = requestBuilder.apply(i);
            // Make sure we use more than one batch so we get the full reindex behavior
            request.source().setSize(DOC_COUNT / randomIntBetween(2, 10));
            // Use a low, random initial wait so we are unlikely collide with others retrying.
            request.setRetryBackoffInitialTime(timeValueMillis(randomIntBetween(10, 300)));
            futures.add(request.execute());
        }

        // Finish all the requests
        List<BulkIndexByScrollResponse> responses = new ArrayList<>(CONCURRENT);
        for (ListenableActionFuture<BulkIndexByScrollResponse> future : futures) {
            responses.add(future.get());
        }

        // Now check them
        long bulkRetries = 0;
        long searchRetries = 0;
        BulkIndexByScrollResponseMatcher matcher = matcher();
        if (expectCreated) {
            matcher.created(DOC_COUNT);
        } else {
            matcher.updated(DOC_COUNT);
        }
        for (BulkIndexByScrollResponse response : responses) {
            assertThat(response, matcher);
            bulkRetries += response.getBulkRetries();
            searchRetries += response.getSearchRetries();
        }

        // We expect at least one retry or this test isn't very useful
        assertThat(bulkRetries, greaterThan(0L));
        assertThat(searchRetries, greaterThan(0L));
    }

    private void setupSourceIndex(String name) {
        try {
            // Build the test index with a single shard so we can be sure that a search request *can* complete with the one thread
            assertAcked(client().admin().indices().prepareCreate(name).setSettings(
                    "index.number_of_shards", 1,
                    "index.number_of_replicas", 0).get());
            waitForRelocation(ClusterHealthStatus.GREEN);
            // Build the test data. Don't use indexRandom because that won't work consistently with such small thread pools.
            BulkRequestBuilder bulk = client().prepareBulk();
            for (int i = 0; i < DOC_COUNT; i++) {
                bulk.add(client().prepareIndex(name, "test").setSource("foo", "bar " + i));
            }
            Retry retry = Retry.on(EsRejectedExecutionException.class).policy(BackoffPolicy.exponentialBackoff());
            BulkResponse response = retry.withSyncBackoff(client(), bulk.request());
            assertFalse(response.buildFailureMessage(), response.hasFailures());
            refresh(name);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
