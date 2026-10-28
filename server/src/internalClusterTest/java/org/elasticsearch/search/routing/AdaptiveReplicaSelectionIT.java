/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.routing;

import org.apache.lucene.tests.util.English;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

/**
 * Here we test the behavior of the "adaptive replica selection" (ARS) system in Elasticsearch.
 * By using a cluster with 4 nodes and indices with 3 replicas, we ensure there's a copy of each shard on each node.
 * That means ARS can choose to route each shard request to any of the 4 nodes.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 4)
public class AdaptiveReplicaSelectionIT extends ESIntegTestCase {

    private static final int CONCURRENCY = 8;

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), true)
            .build();
    }

    /**
     * Under uniform conditions, ARS should distribute requests equally across all nodes.
     * No node should be permanently starved or monopolize traffic.
     * This test asserts that each node handles between 0% and 50% of traffic (these are, for the most part, overly safe bounds, chosen to
     * minimize the chance of transient failures).
     */
    public void testFairDistributionUnderUniformLoad() throws Exception {
        int numSearches = 500;
        assertAcked(
            prepareCreate("test").setSettings(indexSettings(randomIntBetween(6, 12), 3))
                .setMapping("text", "type=text", "num", "type=integer")
        );
        ensureGreen();
        indexDocs("test", 1000);

        // Warm up ARS stats...
        runConcurrentSearches("test", 50);
        // Then capture counts for requests handled by each node for a batch of search requests
        Map<String, Integer> nodeCounts = runConcurrentSearches("test", numSearches);

        int total = nodeCounts.values().stream().mapToInt(Integer::intValue).sum();
        nodeCounts.forEach(
            (nodeId, count) -> logger.info(
                "fairness: node [{}] handled {}/{} = {}%",
                nodeId,
                count,
                total,
                String.format(java.util.Locale.ROOT, "%.1f", 100.0 * count / total)
            )
        );

        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        for (String nodeId : state.nodes().getDataNodes().keySet()) {
            int count = nodeCounts.getOrDefault(nodeId, 0);
            assertThat(
                "Node [" + nodeId + "] was starved: " + count + "/" + numSearches + ". Distribution: " + nodeCounts,
                count,
                greaterThan(0)
            );
            assertThat(
                "Node [" + nodeId + "] handled too much traffic: " + count + "/" + numSearches + ". Distribution: " + nodeCounts,
                count,
                lessThan((int) (numSearches * 0.50))
            );
        }
    }

    private void indexDocs(String indexName, int numDocs) {
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            builders[i] = prepareIndex(indexName).setSource("text", English.intToEnglish(i), "num", i);
        }
        indexRandom(true, builders);
    }

    private Map<String, Integer> runConcurrentSearches(String indexName, int numSearches) throws InterruptedException {
        Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        for (int i = 0; i < numSearches; i++) {
            executor.execute(() -> {
                SearchResponse response = internalCluster().client()
                    .prepareSearch(indexName)
                    .setQuery(termQuery("num", between(0, 999)))
                    .get();
                try {
                    String nodeId = response.getHits().getAt(0).getShard().getNodeId();
                    counts.computeIfAbsent(nodeId, k -> new AtomicInteger()).incrementAndGet();
                } finally {
                    response.decRef();
                }
            });
        }
        executor.shutdown();
        assertTrue("Searches did not complete in time", executor.awaitTermination(120, TimeUnit.SECONDS));

        Map<String, Integer> nodeCounts = new HashMap<>();
        counts.forEach((k, v) -> nodeCounts.put(k, v.get()));
        return nodeCounts;
    }

}
