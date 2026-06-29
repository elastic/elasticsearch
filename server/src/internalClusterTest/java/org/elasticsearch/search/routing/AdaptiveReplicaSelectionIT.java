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
import org.elasticsearch.action.search.SearchTransportService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * Exercise adaptive replica selection (ARS) under sustained search load on a multi-node cluster and assert some
 * behavioral invariants.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 4)
public class AdaptiveReplicaSelectionIT extends ESIntegTestCase {

    private static final int CONCURRENCY = 8;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(OperationRouting.USE_ADAPTIVE_REPLICA_SELECTION_SETTING.getKey(), true)
            .build();
    }

    /**
     * Under uniform conditions, ARS should distribute requests across all nodes.
     * No node should be permanently starved or monopolize traffic.
     */
    public void testConvergenceToFairDistributionUnderUniformLoad() throws Exception {
        int numSearches = 500;
        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 3)).setMapping("text", "type=text", "num", "type=integer"));
        ensureGreen();
        indexDocs("test", 1000);

        // Warm up ARS stats...
        runConcurrentSearches("test", 50);
        // Then capture nodes used for a batch of searches
        Map<String, Integer> nodeCounts = runConcurrentSearches("test", numSearches);

        Set<String> shardNodes = getShardHoldingNodeIds("test", 0);
        // Fair share of traffic for each node is 25%
        // Assert each node handled between 0% and 50% of traffic (to make this test durable)
        for (String nodeId : shardNodes) {
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
     * When one node has degraded performance (high latency inflating service time), ARS should route most traffic away from it.
     */
    public void testDegradedNodeAvoidance() throws Exception {
        int numSearches = 500;
        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 3)).setMapping("text", "type=text", "num", "type=integer"));
        ensureGreen();
        indexDocs("test", 1000);

        Set<String> shardNodes = getShardHoldingNodeIds("test", 0);
        String slowNodeId = shardNodes.iterator().next();
        String slowNodeName = nodeIdsToNames().get(slowNodeId);

        // Inject delay and run searches
        MockTransportService slowTransport = MockTransportService.getInstance(slowNodeName);
        try {
            slowTransport.addRequestHandlingBehavior(SearchTransportService.QUERY_ACTION_NAME, (handler, request, channel, task) -> {
                Thread.sleep(100);
                handler.messageReceived(request, channel, task);
            });

            // Warm up ARS stats...
            runConcurrentSearches("test", 50);
            // Then capture nodes used for a batch of searches
            Map<String, Integer> nodeCounts = runConcurrentSearches("test", numSearches);
            int slowNodeCount = nodeCounts.getOrDefault(slowNodeId, 0);

            // Fair share of traffic would be 25%, but this node is degraded
            // Assert the slow node handled <25% of traffic
            assertThat(
                "Slow node [" + slowNodeId + "] got " + slowNodeCount + "/" + numSearches + ". Distribution: " + nodeCounts,
                slowNodeCount,
                lessThan((int) (numSearches * 0.25))
            );
        } finally {
            slowTransport.clearAllRules();
        }
    }

    /**
     * After a slow node recovers, ARS should gradually route traffic back to it via the stats adjustment mechanism.
     */
    public void testRecoveryAfterDegradation() throws Exception {
        int numRecoverySearches = 500;
        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 3)).setMapping("text", "type=text", "num", "type=integer"));
        ensureGreen();
        indexDocs("test", 1000);

        Set<String> shardNodes = getShardHoldingNodeIds("test", 0);
        String slowNodeId = shardNodes.iterator().next();
        String slowNodeName = nodeIdsToNames().get(slowNodeId);

        // Inject delay and run searches
        MockTransportService slowTransport = MockTransportService.getInstance(slowNodeName);
        try {
            slowTransport.addRequestHandlingBehavior(SearchTransportService.QUERY_ACTION_NAME, (handler, request, channel, task) -> {
                Thread.sleep(200);
                handler.messageReceived(request, channel, task);
            });
            runConcurrentSearches("test", 100);

            // Remove delay and run more searches, capturing nodes used
            slowTransport.clearAllRules();
            Map<String, Integer> nodeCounts = runConcurrentSearches("test", numRecoverySearches);
            int recoveredSlowNodeCount = nodeCounts.getOrDefault(slowNodeId, 0);

            // Fair share of traffic for each node is 25%
            // Similar to the first test, assert the recovered node handled between 0% and 50% of traffic (to make this test durable)
            assertThat(
                "Recovered node ["
                    + slowNodeId
                    + "] was starved: "
                    + recoveredSlowNodeCount
                    + "/"
                    + numRecoverySearches
                    + ". Distribution: "
                    + nodeCounts,
                recoveredSlowNodeCount,
                greaterThan(0)
            );
            assertThat(
                "Recovered node ["
                    + slowNodeId
                    + "] handled too much traffic: "
                    + recoveredSlowNodeCount
                    + "/"
                    + numRecoverySearches
                    + ". Distribution: "
                    + nodeCounts,
                recoveredSlowNodeCount,
                lessThan((int) (numRecoverySearches * 0.50))
            );
        } finally {
            slowTransport.clearAllRules();
        }
    }

    /**
     * Even without the response time component in the ranking formula, ARS still ends up avoiding nodes with high network overhead
     * due to outstanding request tracking (qHatS).
     */
    public void testOutstandingRequestTrackingAvoidsNetworkSlowNode() throws Exception {
        int numSearches = 500;

        assertAcked(prepareCreate("test").setSettings(indexSettings(1, 3)).setMapping("text", "type=text", "num", "type=integer"));
        ensureGreen();
        indexDocs("test", 1000);

        Set<String> shardNodes = getShardHoldingNodeIds("test", 0);
        String slowNodeId = shardNodes.iterator().next();
        String slowNodeName = nodeIdsToNames().get(slowNodeId);
        MockTransportService slowNodeTransport = MockTransportService.getInstance(slowNodeName);

        // Inject network delay on every node's outbound path to the slow node
        for (String nodeName : internalCluster().getNodeNames()) {
            if (nodeName.equals(slowNodeName) == false) {
                MockTransportService senderTransport = MockTransportService.getInstance(nodeName);
                senderTransport.addSendBehavior(slowNodeTransport, (connection, requestId, action, request, options) -> {
                    if (action.equals(SearchTransportService.QUERY_ACTION_NAME)) {
                        try {
                            Thread.sleep(200);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
            }
        }

        try {
            // Warm up ARS stats...
            runConcurrentSearches("test", 50);
            // Then capture nodes used for a batch of searches
            Map<String, Integer> counts = runConcurrentSearches("test", numSearches);
            int slowCount = counts.getOrDefault(slowNodeId, 0);

            // Fair share is 25%
            // Similar to the degraded service time test, assert the slow node handles <=25% of traffic
            assertThat(
                "Network-slow node [" + slowNodeId + "] got " + slowCount + "/" + numSearches + ". Distribution: " + counts,
                slowCount,
                lessThanOrEqualTo((int) (numSearches * 0.25))
            );
            // The slow node should still receive some traffic — it's not fully avoided, just deprioritized.
            assertThat("Network-slow node [" + slowNodeId + "] got zero traffic: " + counts, slowCount, greaterThan(0));
        } finally {
            for (String nodeName : internalCluster().getNodeNames()) {
                MockTransportService.getInstance(nodeName).clearAllRules();
            }
        }
    }

    private void indexDocs(String indexName, int numDocs) {
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            builders[i] = prepareIndex(indexName).setSource("text", English.intToEnglish(i), "num", i);
        }
        indexRandom(true, builders);
    }

    /**
     * Returns a map of nodeId to the number of times that node served the query.
     */
    private Map<String, Integer> runConcurrentSearches(String indexName, int numSearches) throws InterruptedException {
        Map<String, AtomicInteger> counts = new ConcurrentHashMap<>();
        ExecutorService executor = Executors.newFixedThreadPool(CONCURRENCY);
        for (int i = 0; i < numSearches; i++) {
            // internalCluster().client() picks a random node each call, without RandomizingClient wrapping
            // (which sometimes sets preferences that bypass ARS).
            executor.execute(() -> {
                SearchResponse response = internalCluster().client()
                    .prepareSearch(indexName)
                    .setQuery(
                        boolQuery().must(matchAllQuery())
                            .should(matchQuery("text", English.intToEnglish(between(0, 1000))))
                            .filter(rangeQuery("num").gte(between(0, 500)).lte(between(500, 1000)))
                    )
                    .addAggregation(AggregationBuilders.avg("avg_num").field("num"))
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
        assertTrue("Searches did not complete in time", executor.awaitTermination(60, TimeUnit.SECONDS));

        Map<String, Integer> result = new HashMap<>();
        counts.forEach((k, v) -> result.put(k, v.get()));
        return result;
    }

    /**
     * Returns the set of node IDs that hold copies (primary or replica) of the given shard.
     */
    private Set<String> getShardHoldingNodeIds(String indexName, int shardId) {
        ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        IndexRoutingTable indexRoutingTable = state.routingTable(ProjectId.DEFAULT).index(indexName);
        IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(shardId);
        Set<String> nodeIds = new HashSet<>();
        for (int i = 0; i < shardRoutingTable.size(); i++) {
            ShardRouting shardRouting = shardRoutingTable.shard(i);
            if (shardRouting.currentNodeId() != null) {
                nodeIds.add(shardRouting.currentNodeId());
            }
        }
        return nodeIds;
    }
}
