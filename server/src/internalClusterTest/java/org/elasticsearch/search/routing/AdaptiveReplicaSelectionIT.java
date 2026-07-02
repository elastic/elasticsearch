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
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.shard.SearchOperationListener;
import org.elasticsearch.node.Node;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LongSummaryStatistics;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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

    /**
     * Inflates the executor's task-execution EWMA, which is what ARS reads as {@code serviceTimeEWMA} in the query response.
     * Adding the node's name to {@link #slowNodeNames} enables the delay for that node; removing it disables it.
     */
    public static class SlowSearchPlugin extends Plugin {
        static final Set<String> slowNodeNames = ConcurrentHashMap.newKeySet();
        private final String nodeName;

        public SlowSearchPlugin(Settings settings) {
            this.nodeName = Node.NODE_NAME_SETTING.get(settings);
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            indexModule.addSearchOperationListener(new SearchOperationListener() {
                @Override
                public void onPreQueryPhase(SearchContext context) {
                    if (slowNodeNames.contains(nodeName)) {
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), SlowSearchPlugin.class);
    }

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
     * This test asserts that each node handles >0% and <50% of traffic (these are, for the most part, overly safe bounds, chosen to
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
        SearchStats stats = runConcurrentSearches("test", numSearches);
        Map<String, Integer> nodeCounts = stats.nodeCounts();

        int total = nodeCounts.values().stream().mapToInt(Integer::intValue).sum();
        nodeCounts.forEach(
            (nodeId, count) -> logger.info(
                "fairness: node [{}] handled {}/{} = {}%  (min={}ms avg={}ms max={}ms)",
                nodeId,
                count,
                total,
                String.format(java.util.Locale.ROOT, "%.1f", 100.0 * count / total),
                stats.nodeTimings().get(nodeId).getMin(),
                (long) stats.nodeTimings().get(nodeId).getAverage(),
                stats.nodeTimings().get(nodeId).getMax()
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

    /**
     * When one node has degraded service time (slow query execution on the search thread pool), ARS should route most traffic away from it.
     * The slow node should handle less than 30% of traffic (this is an overly safe bound chosen to minimize the chance of transient
     * failures).
     */
    public void testDegradedNodeAvoidance() throws Exception {
        int numSearches = 200;
        assertAcked(
            prepareCreate("test").setSettings(indexSettings(randomIntBetween(6, 12), 3))
                .setMapping("text", "type=text", "num", "type=integer")
        );
        ensureGreen();
        indexDocs("test", 1000);

        ClusterState clusterState = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexShardRoutingTable shardRoutingTable = clusterState.routingTable(ProjectId.DEFAULT).index("test").shard(0);
        Set<String> shardNodes = new HashSet<>();
        for (int i = 0; i < shardRoutingTable.size(); i++) {
            if (shardRoutingTable.shard(i).currentNodeId() != null) {
                shardNodes.add(shardRoutingTable.shard(i).currentNodeId());
            }
        }
        String slowNodeId = shardNodes.iterator().next();
        String slowNodeName = nodeIdsToNames().get(slowNodeId);

        SlowSearchPlugin.slowNodeNames.add(slowNodeName);
        try {
            // Warm up ARS stats so the executor EWMA on the slow node converges to reflect the injected delay...
            runConcurrentSearches("test", 50);
            // Then capture counts for requests handled by each node for a batch of search requests
            SearchStats stats = runConcurrentSearches("test", numSearches);
            Map<String, Integer> nodeCounts = stats.nodeCounts();
            int slowNodeCount = nodeCounts.getOrDefault(slowNodeId, 0);
            int total = nodeCounts.values().stream().mapToInt(Integer::intValue).sum();
            nodeCounts.forEach(
                (nodeId, count) -> logger.info(
                    "degraded: node [{}]{} handled {}/{} = {}%  (min={}ms avg={}ms max={}ms)",
                    nodeId,
                    nodeId.equals(slowNodeId) ? " [SLOW]" : "",
                    count,
                    total,
                    String.format(java.util.Locale.ROOT, "%.1f", 100.0 * count / total),
                    stats.nodeTimings().get(nodeId).getMin(),
                    (long) stats.nodeTimings().get(nodeId).getAverage(),
                    stats.nodeTimings().get(nodeId).getMax()
                )
            );

            assertThat(
                "Slow node [" + slowNodeId + "] got " + slowNodeCount + "/" + numSearches + ". Distribution: " + nodeCounts,
                slowNodeCount,
                lessThan((int) (numSearches * 0.30))
            );
        } finally {
            SlowSearchPlugin.slowNodeNames.remove(slowNodeName);
        }
    }

    private void indexDocs(String indexName, int numDocs) {
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            builders[i] = prepareIndex(indexName).setSource("text", English.intToEnglish(i), "num", i);
        }
        indexRandom(true, builders);
    }

    private record SearchStats(Map<String, Integer> nodeCounts, Map<String, LongSummaryStatistics> nodeTimings) {}

    /**
     * Returns per-node search counts and response time statistics for {@code numSearches} concurrent searches.
     */
    private SearchStats runConcurrentSearches(String indexName, int numSearches) throws InterruptedException {
        Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        Map<String, Queue<Long>> timings = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        for (int i = 0; i < numSearches; i++) {
            executor.execute(() -> {
                long start = System.nanoTime();
                // termQuery hits exactly one doc (num is unique 0..numDocs-1): O(1) term lookup with
                // no aggregation. Keeping per-search latency in single-digit milliseconds ensures any
                // injected delay (service-time or network) remains the dominant signal in ARS's EWMAs.
                SearchResponse response = internalCluster().client()
                    .prepareSearch(indexName)
                    .setQuery(termQuery("num", between(0, 999)))
                    .get();
                long elapsedMs = (System.nanoTime() - start) / 1_000_000;
                try {
                    String nodeId = response.getHits().getAt(0).getShard().getNodeId();
                    counts.computeIfAbsent(nodeId, k -> new AtomicInteger()).incrementAndGet();
                    timings.computeIfAbsent(nodeId, k -> new ConcurrentLinkedQueue<>()).add(elapsedMs);
                } finally {
                    response.decRef();
                }
            });
        }
        executor.shutdown();
        assertTrue("Searches did not complete in time", executor.awaitTermination(120, TimeUnit.SECONDS));

        Map<String, Integer> nodeCounts = new HashMap<>();
        counts.forEach((k, v) -> nodeCounts.put(k, v.get()));

        Map<String, LongSummaryStatistics> nodeTimings = new HashMap<>();
        timings.forEach((k, v) -> nodeTimings.put(k, v.stream().mapToLong(Long::longValue).summaryStatistics()));

        return new SearchStats(nodeCounts, nodeTimings);
    }

}
