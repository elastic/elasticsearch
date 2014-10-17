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

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.IndexAlreadyExistsException;
import org.elasticsearch.indices.breaker.CircuitBreakerModule;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static junit.framework.Assert.assertNotNull;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.search.aggregations.AggregationBuilders.terms;

/**
 * Benchmarks for different implementations of the circuit breaker
 */
public class CircuitBreakerBenchmark {

    private static Settings nodeSettings() {
        return ImmutableSettings.builder()
                // Comment/uncomment as needed to use a regular circuit breaker
                // (Hierarchy), or a noop breaker (None)
                .put(CircuitBreakerModule.IMPL, "org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService")
                //.put(CircuitBreakerModule.IMPL, "org.elasticsearch.indices.breaker.NoneCircuitBreakerService")
                .build();
    }

    private static final String INDEX = UUID.randomUUID().toString();

    public static void main(String args[]) throws Exception {
        Node node = NodeBuilder.nodeBuilder().settings(
                ImmutableSettings.settingsBuilder().put(nodeSettings()).put("node.master", true)
        ).node();
        final Client client = node.client();
        try {
            try {
                client.admin().indices().prepareDelete(INDEX).get();
            } catch (Exception e) {
                // Ignore
            }
            try {
                client.admin().indices().prepareCreate(INDEX).setSettings(
                        settingsBuilder().put("number_of_shards", 2).put("number_of_replicas", 0)).get();
            } catch (IndexAlreadyExistsException e) {}
            client.admin().cluster().prepareHealth().setWaitForYellowStatus().execute().actionGet();

            final int BULK_SIZE = 100;
            final int NUM_DOCS = 500_000;
            final int AGG_SIZE = 50;
            System.out.println("--> indexing: " + NUM_DOCS + " documents...");
            BulkRequestBuilder bulkBuilder = client.prepareBulk();
            for (int i = 0; i < NUM_DOCS; i++) {
                bulkBuilder.add(client.prepareIndex(INDEX, "doc").setSource("num", i));
                if (i % BULK_SIZE == 0) {
                    // Send off bulk request
                    bulkBuilder.get();
                    // Create a new holder
                    bulkBuilder = client.prepareBulk();
                }
            }
            bulkBuilder.get();
            client.admin().indices().prepareRefresh(INDEX).get();
            SearchResponse countResp = client.prepareSearch(INDEX).setQuery(matchAllQuery()).setSearchType(SearchType.COUNT).get();
            assert countResp.getHits().getTotalHits() == NUM_DOCS : "all docs should be indexed";

            final int WARMUPS = 25;
            for (int i = 0; i < WARMUPS; i++) {
                if (i % 10 == 0) {
                    System.out.println("--> warmup #" + i);
                }
                SearchResponse resp = client.prepareSearch(INDEX).setQuery(matchAllQuery())
                        .addAggregation(
                                terms("myterms")
                                        .size(AGG_SIZE)
                                        .field("num")
                        ).setSearchType(SearchType.COUNT).get();
                Terms terms = resp.getAggregations().get("myterms");
                assertNotNull("term aggs were calculated", terms);
            }

            final int QUERIES = 100;

            long totalTime = 0;
            for (int i = 0; i < QUERIES; i++) {
                if (i % 10 == 0) {
                    System.out.println("--> query #" + i);
                }
                SearchResponse resp = client.prepareSearch(INDEX).setQuery(matchAllQuery())
                        .addAggregation(
                                terms("myterms")
                                        .size(AGG_SIZE)
                                        .field("num")
                        ).setSearchType(SearchType.COUNT).get();
                Terms terms = resp.getAggregations().get("myterms");
                assertNotNull("term aggs were calculated", terms);
                totalTime += resp.getTookInMillis();
            }

            System.out.println("--> single threaded average time: " + (totalTime / QUERIES) + "ms");

            final AtomicLong totalThreadedTime = new AtomicLong(0);
            int THREADS = 5;
            Thread threads[] = new Thread[THREADS];
            for (int i = 0; i < THREADS; i++) {
                threads[i] = new Thread(new Runnable() {
                    @Override
                    public void run() {
                        long tid = Thread.currentThread().getId();
                        for (int i = 0; i < QUERIES; i++) {
                            if (i % 10 == 0) {
                                System.out.println("--> [" + tid + "] query # "+ i);
                            }
                            SearchResponse resp = client.prepareSearch(INDEX).setQuery(matchAllQuery())
                                    .addAggregation(
                                            terms("myterms")
                                                    .size(AGG_SIZE)
                                                    .field("num")
                                    ).setSearchType(SearchType.COUNT).get();
                            Terms terms = resp.getAggregations().get("myterms");
                            assertNotNull("term aggs were calculated", terms);
                            totalThreadedTime.addAndGet(resp.getTookInMillis());
                        }
                    }
                });
            }

            System.out.println("--> starting " + THREADS + " threads for parallel aggregating");
            for (Thread t : threads) {
                t.start();
            }

            for (Thread t : threads) {
                t.join();
            }

            System.out.println("--> threaded average time: " + (totalThreadedTime.get() / (THREADS * QUERIES)) + "ms");

        } finally {
            client.close();
            node.stop();
        }
    }
}
