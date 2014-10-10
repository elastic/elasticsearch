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

package org.elasticsearch.benchmark;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.List;

import static com.google.common.collect.Lists.newArrayList;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

/**
 * Benchmarks for different implementations of the circuit breaker
 */
@ElasticsearchIntegrationTest.ClusterScope(scope= ElasticsearchIntegrationTest.Scope.SUITE, numDataNodes =0)
public class CircuitBreakerBenchmark extends ElasticsearchIntegrationTest {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                // Comment/uncomment as needed to use a regular circuit breaker
                // (Hierarchy), or a noop breaker (None)
                .put(CircuitBreakerModule.IMPL, "org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService")
                //.put(CircuitBreakerModule.IMPL, "org.elasticsearch.indices.breaker.NoneCircuitBreakerService")
                .build();
    }

    @Test
    public void testSingleThreadPerformance() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(
                settingsBuilder().put("number_of_shards", 5).put("number_of_replicas", 0)).get();
        ensureGreen("test");

        int NUM_DOCS = 20_000;
        List<IndexRequestBuilder> builders = newArrayList();
        for (int i = 0; i < NUM_DOCS; i++) {
            builders.add(client().prepareIndex("test", "doc").setSource("num", i));
        }
        logger.info("--> indexing docs...");
        indexRandom(true, false, false, builders);

        int queries = 100;
        int warmup = (int) (5 * Math.log10(queries));

        logger.info("--> warming up with {} queries...", warmup);
        for (int i = 0; i < warmup; i++) {
            client().prepareSearch("test").setQuery(matchAllQuery())
                    .addAggregation(
                            terms("myterms")
                                    .size(NUM_DOCS)
                                    .field("num")
                    ).setSearchType(SearchType.COUNT).get();
        }

        long times[] = new long[queries];
        for (int i = 0; i < queries; i++) {
            if (i % 10 == 0) {
                logger.info("--> query # {}", i);
            }
            SearchResponse resp = client().prepareSearch("test").setQuery(matchAllQuery())
                    .addAggregation(
                            terms("myterms")
                                    .size(NUM_DOCS)
                                    .field("num")
                    ).setSearchType(SearchType.COUNT).get();
            Terms terms = resp.getAggregations().get("myterms");
            assertHitCount(resp, NUM_DOCS);
            assertNotNull("term aggs were calculated", terms);
            times[i] = resp.getTookInMillis();
        }

        long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, total = 0, avg;
        for (int i = 0; i < queries; i++) {
            long time = times[i];
            min = Math.min(min, time);
            max = Math.max(max, time);
            total = total + time;
        }
        avg = total / queries;
        logger.info("--> min: {}ms, max: {}ms, total: {}ms, average: {}ms", min, max, total, avg);
    }

    @Test
    public void testMultiThreadPerformance() throws Exception {
        client().admin().indices().prepareCreate("test").setSettings(
                settingsBuilder().put("number_of_shards", 5).put("number_of_replicas", 0)).get();
        ensureGreen("test");

        final int NUM_DOCS = 20_000;
        List<IndexRequestBuilder> builders = newArrayList();
        for (int i = 0; i < NUM_DOCS; i++) {
            builders.add(client().prepareIndex("test", "doc").setSource("num", i));
        }
        logger.info("--> indexing docs...");
        indexRandom(true, false, false, builders);

        final int queries = 100;
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        int warmup = (int) (5 * Math.log10(queries));

        logger.info("--> warming up with {} queries...", warmup);
        for (int i = 0; i < warmup; i++) {
            client().prepareSearch("test").setQuery(matchAllQuery())
                    .addAggregation(
                            terms("myterms")
                                    .size(NUM_DOCS)
                                    .field("num")
                    ).setSearchType(SearchType.COUNT).get();
        }

        for (int threadNum = 0; threadNum < threadCount; threadNum++) {
            threads[threadNum] = new Thread(new Runnable() {
                @Override
                public void run() {
                    long tid = Thread.currentThread().getId();
                    long times[] = new long[queries];
                    for (int i = 0; i < queries; i++) {
                        if (i % 10 == 0) {
                            logger.info("--> [{}] query # {}", tid, i);
                        }
                        SearchResponse resp = client().prepareSearch("test").setQuery(matchAllQuery())
                                .addAggregation(
                                        terms("myterms")
                                                .size(NUM_DOCS)
                                                .field("num")
                                ).setSearchType(SearchType.COUNT).get();
                        Terms terms = resp.getAggregations().get("myterms");
                        assertHitCount(resp, NUM_DOCS);
                        assertNotNull("term aggs were calculated", terms);
                        times[i] = resp.getTookInMillis();
                    }

                    long min = Integer.MAX_VALUE, max = Integer.MIN_VALUE, total = 0, avg;
                    for (int i = 0; i < queries; i++) {
                        long time = times[i];
                        min = Math.min(min, time);
                        max = Math.max(max, time);
                        total = total + time;
                    }
                    avg = total / queries;
                    logger.info("--> [{}] min: {}ms, max: {}ms, total: {}ms, average: {}ms", tid, min, max, total, avg);
                }
            });
        }

        for (Thread t : threads) {
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }
}
