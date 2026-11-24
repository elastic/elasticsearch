/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.routing.allocation.RecoveryDirectCancellationService;
import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.indices.recovery.RecoveryGateMonitor;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.cluster.stateless.local.DefaultStatelessLocalConfigProvider.node;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/// Runs a recovery-heavy workload against a multi-node stateless cluster.
/// This test suite creates many indices, indexes in the background, and repeatedly triggers concurrent recoveries
/// (relocations, replica changes, close/reopen, node restarts). It checks that the cluster always returns to green and
/// no acknowledged write is lost.
///
/// Each scenario runs once with the default (master-side) throttling and once with DNRT enabled with tight limits.
///
/// All requests go through the master node, which is never restarted: restarted nodes come back on a different HTTP port.
public class StatelessManyShardsRecoveryRestIT extends ESRestTestCase {

    private static final String USER = "admin-user";
    private static final String PASSWORD = "x-pack-test-password";

    private static final int MASTER_NODE = 0;
    private static final List<String> INDEX_NODE_NAMES = List.of("index-1", "index-2");
    private static final List<String> SEARCH_NODE_NAMES = List.of("search-1", "search-2");
    private static final int NUM_NODES = 1 + INDEX_NODE_NAMES.size() + SEARCH_NODE_NAMES.size();

    private static final TimeValue HEALTH_TIMEOUT = TimeValue.timeValueMinutes(1);

    @ClassRule
    public static final StatelessElasticsearchCluster cluster = StatelessElasticsearchCluster.localEmptyNode()
        .withNode(masterNode -> masterNode.name("master").setting("node.roles", "[master]"))
        .withNode(node(INDEX_NODE_NAMES.get(0), "[remote_cluster_client,ingest,index]"))
        .withNode(node(INDEX_NODE_NAMES.get(1), "[remote_cluster_client,ingest,index]"))
        .withNode(node(SEARCH_NODE_NAMES.get(0), "[remote_cluster_client,search]"))
        .withNode(node(SEARCH_NODE_NAMES.get(1), "[remote_cluster_client,search]"))
        .module("stateless")
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.watcher.enabled", "false")
        .user(USER, PASSWORD)
        .build();

    /// Each disruption causes a burst of recoveries.
    private enum Disruption {
        // Moves all shards off one index node, then lets them rebalance back.
        RELOCATE_OFF_INDEX_NODE,
        // Moves all shards off one search node, then lets them rebalance back.
        RELOCATE_OFF_SEARCH_NODE,
        // Drops the search replicas of some indices, then adds them back.
        DROP_AND_RESTORE_REPLICAS,
        // Closes some indices, then reopens them.
        CLOSE_AND_REOPEN,
        // Restarts an index or search node in place, possibly forcibly.
        RESTART_DATA_NODE
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddress(MASTER_NODE);
    }

    @Override
    protected Settings restClientSettings() {
        final var token = basicAuthHeaderValue(USER, new SecureString(PASSWORD.toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .put(CLIENT_SOCKET_TIMEOUT, HEALTH_TIMEOUT.millis() + 30_000 + "ms")
            .build();
    }

    public void testWithDefaultRecoveryThrottling() throws Exception {
        runDisruptionWorkload(Settings.EMPTY);
    }

    public void testWithTightDataNodeRecoveryThrottling() throws Exception {
        runDisruptionWorkload(tightDataNodeRecoveryThrottlingSettings());
    }

    private void runDisruptionWorkload(Settings throttlingSettings) throws Exception {
        if (throttlingSettings.isEmpty() == false) {
            logger.info("--> running with recovery throttling settings {}", throttlingSettings);
            updateClusterSettings(throttlingSettings);
        }

        final var indices = createIndices();
        waitForGreenHealth();

        final var indexer = new Indexer(indices, new Random(randomLong()));
        final var disruptionsDone = new AtomicBoolean();
        final var testThread = Thread.currentThread();
        runInParallel(() -> indexer.indexUntil(disruptionsDone), () -> {
            assert Thread.currentThread() == testThread : "disruptions must run on the test thread to be reproducible from the seed";
            try {
                runDisruptionRounds(indices);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                disruptionsDone.set(true);
            }
        });
        verifyAckedDocs(indexer);
    }

    private void runDisruptionRounds(List<String> indices) throws IOException {
        final int rounds = between(3, 6);
        for (int round = 0; round < rounds; round++) {
            final var disruption = randomFrom(Disruption.values());
            logger.info("--> round [{}/{}]: [{}]", round + 1, rounds, disruption);
            triggerDisruption(disruption, indices);
            waitForGreenHealth();
        }
    }

    private List<String> createIndices() throws IOException {
        final int numIndices = between(20, 40);
        final List<String> indices = new ArrayList<>(numIndices);
        int totalShards = 0;
        for (int i = 0; i < numIndices; i++) {
            final var indexName = randomIndexName();
            final int numShards = between(1, 3);
            createIndex(indexName, indexSettings(numShards, 1).build());
            indices.add(indexName);
            totalShards += numShards;
        }
        logger.info("--> created [{}] indices with [{}] primary shards in total", numIndices, totalShards);
        return indices;
    }

    private void triggerDisruption(Disruption disruption, List<String> indices) throws IOException {
        switch (disruption) {
            case RELOCATE_OFF_INDEX_NODE -> relocateOffNode(randomFrom(INDEX_NODE_NAMES));
            case RELOCATE_OFF_SEARCH_NODE -> relocateOffNode(randomFrom(SEARCH_NODE_NAMES));
            case DROP_AND_RESTORE_REPLICAS -> {
                final var subset = randomSubsetOf(between(1, Math.min(10, indices.size())), indices);
                final var indexPattern = String.join(",", subset);
                updateIndexSettings(indexPattern, Settings.builder().put("index.number_of_replicas", 0));
                waitForGreenHealth();
                updateIndexSettings(indexPattern, Settings.builder().put("index.number_of_replicas", 1));
            }
            case CLOSE_AND_REOPEN -> {
                final var subset = randomSubsetOf(between(1, Math.min(5, indices.size())), indices);
                final var indexPattern = String.join(",", subset);
                closeIndex(indexPattern);
                openIndex(indexPattern);
            }
            case RESTART_DATA_NODE -> {
                // Don't restart the master
                final int nodeIndex = between(1, NUM_NODES - 1);
                final boolean forcibly = randomBoolean();
                logger.info("--> restarting node [{}], forcibly [{}]", nodeIndex, forcibly);
                cluster.restartNodeInPlace(nodeIndex, forcibly);
            }
        }
    }

    private void relocateOffNode(String nodeName) throws IOException {
        logger.info("--> relocating all shards off node [{}]", nodeName);
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", nodeName).build());
        waitForGreenHealth();
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._name").build());
    }

    private static void waitForGreenHealth() throws IOException {
        final var request = new Request("GET", "/_cluster/health");
        request.addParameter("wait_for_status", "green");
        request.addParameter("wait_for_nodes", Integer.toString(NUM_NODES));
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("wait_for_no_initializing_shards", "true");
        request.addParameter("wait_for_events", "languid");
        request.addParameter("timeout", HEALTH_TIMEOUT.getStringRep());
        try {
            client().performRequest(request);
        } catch (ResponseException e) {
            throw new AssertionError("cluster did not converge to green within " + HEALTH_TIMEOUT + ": " + recoveriesInProgress(), e);
        }
    }

    private static String recoveriesInProgress() {
        try {
            final var request = new Request("GET", "/_cat/recovery");
            request.addParameter("active_only", "true");
            request.addParameter("v", "true");
            return new String(client().performRequest(request).getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "failed to get recoveries: " + e;
        }
    }

    private void verifyAckedDocs(Indexer indexer) throws IOException {
        refresh("*");
        long totalAcked = 0;
        for (final var indexName : indexer.indices) {
            final long acked = indexer.acked.getOrDefault(indexName, 0L);
            final long attempted = indexer.attempted.getOrDefault(indexName, 0L);
            final var response = client().performRequest(new Request("GET", "/" + indexName + "/_count"));
            final long count = ((Number) ObjectPath.createFromResponse(response).evaluate("count")).longValue();
            // every acked write must be visible, and requests that failed may or may not have been applied
            assertThat(
                "unexpected doc count for [" + indexName + "]",
                count,
                allOf(greaterThanOrEqualTo(acked), lessThanOrEqualTo(attempted))
            );
            totalAcked += acked;
        }
        assertThat("background indexer did not get any write acked", totalAcked, greaterThan(0L));
    }

    private static Settings tightDataNodeRecoveryThrottlingSettings() {
        return Settings.builder()
            // unthrottle the master-side deciders
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), Integer.MAX_VALUE)
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(),
                Integer.MAX_VALUE
            )
            .put("cluster.routing.allocation.min_heap_required_for_concurrent_primary_recoveries", "0b")
            .put("cluster.routing.allocation.concurrent_primary_recoveries_per_heap_gb", Integer.MAX_VALUE)
            .put(ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING.getKey(), -1)
            // and throttle on the data nodes instead
            .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), between(1, 16))
            .put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(),
                between(10, 100) + "%"
            )
            .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), between(1, 16))
            .put(RecoveryGateMonitor.ENABLE_RECOVERY_GATES_SETTING.getKey(), randomBoolean())
            .put(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(), randomBoolean())
            .build();
    }

    /// Indexes small bulks of documents with unique ids into random indices, and tracks per index how many writes were
    /// acknowledged and how many were attempted.
    private class Indexer {
        private final List<String> indices;
        private final Random random;
        private final Map<String, Long> acked = new HashMap<>();
        private final Map<String, Long> attempted = new HashMap<>();

        Indexer(List<String> indices, Random random) {
            this.indices = indices;
            this.random = random;
        }

        void indexUntil(AtomicBoolean done) {
            long docId = 0;
            while (done.get() == false) {
                final var indexName = indices.get(random.nextInt(indices.size()));
                final int numDocs = 1 + random.nextInt(20);
                final var bulk = new StringBuilder();
                for (int i = 0; i < numDocs; i++) {
                    bulk.append(Strings.format("""
                        {"index":{"_index":"%s","_id":"%d"}}
                        {"field":%d}
                        """, indexName, docId++, random.nextInt()));
                }
                final var request = new Request("POST", "/_bulk");
                request.setJsonEntity(bulk.toString());
                attempted.merge(indexName, (long) numDocs, Long::sum);
                // A request that fails outright (e.g. during a restart) may or may not have been applied.
                try {
                    final var response = client().performRequest(request);
                    final List<?> items = ObjectPath.createFromResponse(response).evaluate("items");
                    for (final var item : items) {
                        final int status = (Integer) ((Map<?, ?>) ((Map<?, ?>) item).get("index")).get("status");
                        if (status == 200 || status == 201) {
                            acked.merge(indexName, 1L, Long::sum);
                        }
                    }
                } catch (IOException e) {
                    logger.debug("bulk request to [{}] failed", indexName, e);
                }
                safeSleep(random.nextInt(50));
            }
        }
    }
}
