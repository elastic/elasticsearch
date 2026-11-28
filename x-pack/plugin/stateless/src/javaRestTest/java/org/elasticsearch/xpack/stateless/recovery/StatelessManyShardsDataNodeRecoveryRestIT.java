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
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.test.cluster.stateless.StatelessElasticsearchCluster;
import org.elasticsearch.test.cluster.stateless.local.DefaultStatelessSettingsProvider;
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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.test.cluster.stateless.local.DefaultStatelessLocalConfigProvider.node;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/// Runs a recovery-heavy workload against a multi-node stateless cluster.
/// This test suite runs with data node recovery throttling (DNRT) enabled with tight limits. It creates many indices,
/// indexes in the background, and repeatedly triggers concurrent recoveries (relocations, replica changes, close/reopen,
/// node restarts). It checks that the cluster always returns to green and no acknowledged write is lost.
///
/// All requests go through the master node, which is never restarted: restarted nodes come back on a different HTTP port.
public class StatelessManyShardsDataNodeRecoveryRestIT extends ESRestTestCase {

    private static final String USER = "admin-user";
    private static final String PASSWORD = "x-pack-test-password";

    private static final List<String> INDEX_NODE_NAMES = List.of("index-1", "index-2");
    private static final List<String> SEARCH_NODE_NAMES = List.of("search-1", "search-2");
    /// Data node names in the order they are added to the cluster. See [#buildCluster()].
    private static final List<String> DATA_NODE_NAMES = Stream.concat(INDEX_NODE_NAMES.stream(), SEARCH_NODE_NAMES.stream()).toList();
    /// The master is added after all the data nodes.
    private static final int MASTER_NODE = DATA_NODE_NAMES.size();
    private static final int NUM_NODES = 1 + DATA_NODE_NAMES.size();

    /// Generous because with tight limits, recoveries queue up and complete one at a time.
    private static final TimeValue AWAIT_TIMEOUT = TimeValue.timeValueMinutes(3);

    @ClassRule
    public static final StatelessElasticsearchCluster cluster = buildCluster();

    /// Adds the data nodes in [#DATA_NODE_NAMES] order, then the master at index [#MASTER_NODE].
    private static StatelessElasticsearchCluster buildCluster() {
        final var builder = StatelessElasticsearchCluster.localEmptyNode();
        for (final var nodeName : DATA_NODE_NAMES) {
            final var roles = INDEX_NODE_NAMES.contains(nodeName)
                ? "[remote_cluster_client,ingest,index]"
                : "[remote_cluster_client,search]";
            builder.withNode(node(nodeName, roles));
        }
        return builder.withNode(masterNode -> masterNode.name("master").setting("node.roles", "[master]"))
            .module("stateless")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.watcher.enabled", "false")
            .user(USER, PASSWORD)
            .build();
    }

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
            .put(CLIENT_SOCKET_TIMEOUT, AWAIT_TIMEOUT.millis() + 30_000 + "ms")
            .build();
    }

    public void testWithTightDataNodeRecoveryThrottling() throws Exception {
        final var throttlingSettings = tightDataNodeRecoveryThrottlingSettings();
        logger.info("--> running with recovery throttling settings {}", throttlingSettings);
        updateClusterSettings(throttlingSettings);

        final var indices = createIndices();
        waitForGreenAndBalanced();

        final Map<String, Long> acked = new ConcurrentHashMap<>();
        final Map<String, Long> attempted = new ConcurrentHashMap<>();
        final var indexer = new Indexer(indices, acked, attempted, new Random(randomLong()));
        final var searcher = new Searcher(indices, acked, attempted, new Random(randomLong()));
        final var disruptionsDone = new AtomicBoolean();
        final var testThread = Thread.currentThread();
        runInParallel(() -> indexer.indexUntil(disruptionsDone), () -> searcher.searchUntil(disruptionsDone), () -> {
            assert Thread.currentThread() == testThread : "disruptions must run on the test thread to be reproducible from the seed";
            try {
                runDisruptionRounds(indices);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            } finally {
                disruptionsDone.set(true);
            }
        });
        // runInParallel has joined the indexer and searcher threads, so their results are final.
        verifyAckedDocs(indices, indexer.ackedIds, attempted);
        assertThat("background searcher did not complete any search on all shards", searcher.verifiedSearches, greaterThan(0L));
    }

    private void runDisruptionRounds(List<String> indices) throws IOException {
        final int rounds = between(3, 6);
        for (int round = 0; round < rounds; round++) {
            // The first round recovers all the shards of a node at once (relocating them off it, or restarting it), which
            // always exceeds the data node recovery limits, so the test leverages data node recovery queueing.
            final var disruption = round == 0
                ? randomFrom(Disruption.RELOCATE_OFF_INDEX_NODE, Disruption.RELOCATE_OFF_SEARCH_NODE, Disruption.RESTART_DATA_NODE)
                : randomFrom(Disruption.values());
            logger.info("--> round [{}/{}]: [{}]", round + 1, rounds, disruption);
            triggerDisruption(disruption, indices);
            waitForGreenAndBalanced();
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
                closeIndices(subset);
                openIndices(subset);
            }
            case RESTART_DATA_NODE -> {
                final var nodeName = randomFrom(DATA_NODE_NAMES);
                final boolean forcibly = randomBoolean();
                logger.info("--> restarting node [{}], forcibly [{}]", nodeName, forcibly);
                restartNodeInPlace(nodeName, forcibly);
            }
        }
    }

    private static void restartNodeInPlace(String nodeName, boolean forcibly) {
        try {
            cluster.restartNodeInPlace(DATA_NODE_NAMES.indexOf(nodeName), forcibly);
        } catch (RuntimeException e) {
            throw new AssertionError("cluster did not become ready after restarting node [" + nodeName + "]" + clusterDiagnostics(), e);
        }
    }

    private static void closeIndices(List<String> indices) throws IOException {
        assertAcknowledged(client().performRequest(new Request("POST", "/" + String.join(",", indices) + "/_close")));
    }

    private static void openIndices(List<String> indices) throws IOException {
        assertAcknowledged(client().performRequest(new Request("POST", "/" + String.join(",", indices) + "/_open")));
    }

    private void relocateOffNode(String nodeSpecName) throws IOException {
        final var nodeName = resolveNodeName(nodeSpecName);
        logger.info("--> relocating [{}] shards off node [{}]", countShardsOnNode(nodeName), nodeName);
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.exclude._name", nodeName).build());
        waitForGreenAndBalanced();
        assertThat("shards left on excluded node [" + nodeName + "]", countShardsOnNode(nodeName), equalTo(0));
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.exclude._name").build());
    }

    private static String resolveNodeName(String nodeSpecName) throws IOException {
        final var request = new Request("GET", "/_cat/nodes");
        request.addParameter("h", "name");
        request.addParameter("format", "json");
        final List<String> matchingNames = new ArrayList<>();
        for (final var node : entityAsList(client().performRequest(request))) {
            final String name = ObjectPath.evaluate(node, "name");
            if (name.startsWith(nodeSpecName + "-")) {
                matchingNames.add(name);
            }
        }
        assertThat("nodes named after [" + nodeSpecName + "]", matchingNames, hasSize(1));
        return matchingNames.getFirst();
    }

    private static int countShardsOnNode(String nodeName) throws IOException {
        final var request = new Request("GET", "/_cat/shards");
        request.addParameter("h", "node");
        request.addParameter("format", "json");
        int count = 0;
        for (final var shard : entityAsList(client().performRequest(request))) {
            if (nodeName.equals(ObjectPath.evaluate(shard, "node"))) {
                count++;
            }
        }
        return count;
    }

    /// Waits for the cluster to be green with all the relocations the balancer wants done.
    ///
    /// The health API alone is not enough after a node joins. The balancer computes asynchronously where shards should
    /// go, so health can report green before the rebalancing towards the new node has started. The reroute API only
    /// responds once the balancer has converged and the resulting relocations have started, so the second health check
    /// waits for them to finish.
    private static void waitForGreenAndBalanced() throws IOException {
        waitForGreenHealth();
        client().performRequest(new Request("POST", "/_cluster/reroute"));
        waitForGreenHealth();
    }

    private static void waitForGreenHealth() throws IOException {
        final var request = new Request("GET", "/_cluster/health");
        request.addParameter("wait_for_status", "green");
        request.addParameter("wait_for_nodes", Integer.toString(NUM_NODES));
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("wait_for_no_initializing_shards", "true");
        request.addParameter("wait_for_events", "languid");
        request.addParameter("timeout", AWAIT_TIMEOUT.getStringRep());
        try {
            client().performRequest(request);
        } catch (ResponseException e) {
            throw new AssertionError("cluster did not converge to green within " + AWAIT_TIMEOUT + clusterDiagnostics(), e);
        }
    }

    private static String clusterDiagnostics() {
        final var shards = new Request("GET", "/_cat/shards");
        shards.addParameter("v", "true");
        shards.addParameter("h", "index,shard,prirep,state,node,unassigned.reason");
        final var recoveries = new Request("GET", "/_cat/recovery");
        recoveries.addParameter("active_only", "true");
        recoveries.addParameter("v", "true");
        return "\nshards:\n" + catOutput(shards) + "\nactive recoveries:\n" + catOutput(recoveries);
    }

    private static String catOutput(Request request) {
        try {
            return new String(client().performRequest(request).getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            return "failed to get [" + request.getEndpoint() + "]: " + e;
        }
    }

    /// Checks that every acknowledged document is searchable, by id, and that no index holds more documents than were attempted.
    private void verifyAckedDocs(List<String> indices, Map<String, List<String>> ackedIds, Map<String, Long> attempted) throws IOException {
        final var refresh = ObjectPath.createFromResponse(
            client().performRequest(new Request("POST", "/" + String.join(",", indices) + "/_refresh"))
        );
        assertAllShardsSucceeded("refresh", refresh);
        long totalAcked = 0;
        for (final var indexName : indices) {
            final List<String> indexAckedIds = ackedIds.getOrDefault(indexName, List.of());
            final long attemptedDocs = attempted.getOrDefault(indexName, 0L);
            assertThat(
                "acked documents in [" + indexName + "]",
                countDocsWithIds(indexName, indexAckedIds),
                equalTo((long) indexAckedIds.size())
            );
            assertThat("doc count for [" + indexName + "]", countDocs(indexName, "{}"), lessThanOrEqualTo(attemptedDocs));
            totalAcked += indexAckedIds.size();
        }
        assertThat("background indexer did not get any write acked", totalAcked, greaterThan(0L));
    }

    /// Counts the documents with the given ids.
    private static long countDocsWithIds(String indexName, List<String> ids) throws IOException {
        final int batchSize = 1_000;
        long count = 0;
        for (int from = 0; from < ids.size(); from += batchSize) {
            final var batch = ids.subList(from, Math.min(from + batchSize, ids.size()));
            count += countDocs(
                indexName,
                "{\"query\":{\"ids\":{\"values\":[" + batch.stream().map(id -> '"' + id + '"').collect(Collectors.joining(",")) + "]}}}"
            );
        }
        return count;
    }

    private static long countDocs(String indexName, String body) throws IOException {
        final var request = new Request("GET", "/" + indexName + "/_count");
        request.setJsonEntity(body);
        final var response = ObjectPath.createFromResponse(client().performRequest(request));
        assertAllShardsSucceeded("count on [" + indexName + "]", response);
        return ((Number) response.evaluate("count")).longValue();
    }

    private static boolean allShardsSucceeded(ObjectPath response) throws IOException {
        final int total = response.evaluate("_shards.total");
        final int successful = response.evaluate("_shards.successful");
        final int failed = response.evaluate("_shards.failed");
        return successful == total && failed == 0;
    }

    private static void assertAllShardsSucceeded(String description, ObjectPath response) throws IOException {
        assertTrue(description + " did not succeed on all shards: " + response.evaluate("_shards"), allShardsSucceeded(response));
    }

    /// Lifts master-side throttling and caps the data nodes to tight concurrency limits.
    private static Settings tightDataNodeRecoveryThrottlingSettings() {
        return Settings.builder()
            // unthrottle the master-side deciders
            .loadFromMap(DefaultStatelessSettingsProvider.UNTHROTTLED_MASTER_RECOVERY_SETTINGS)
            // and throttle on the data nodes instead
            .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), between(1, 3))
            .put(
                DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(),
                between(30, 100) + "%"
            )
            .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(), between(1, 3))
            .put(RecoveryDirectCancellationService.ENABLE_DIRECT_RECOVERY_CANCELLATIONS_SETTING.getKey(), randomBoolean())
            .build();
    }

    /// Indexes small bulks of documents with unique ids into random indices, and tracks per index how many writes were
    /// acknowledged and how many were attempted, as well as the ids of the acknowledged documents.
    private class Indexer {
        private final List<String> indices;
        private final Map<String, Long> acked;
        private final Map<String, Long> attempted;
        private final Random random;

        private final Map<String, List<String>> ackedIds = new HashMap<>();
        private final Map<Integer, Long> failedItemsByStatus = new HashMap<>();
        private long failedBulks;

        Indexer(List<String> indices, Map<String, Long> acked, Map<String, Long> attempted, Random random) {
            this.indices = indices;
            this.acked = acked;
            this.attempted = attempted;
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
                    final var response = ObjectPath.createFromResponse(client().performRequest(request));
                    final int numItems = response.evaluateArraySize("items");
                    int failedItems = 0;
                    Object firstError = null;
                    for (int i = 0; i < numItems; i++) {
                        final int status = response.evaluate("items." + i + ".index.status");
                        if (status == 200 || status == 201) {
                            ackedIds.computeIfAbsent(indexName, k -> new ArrayList<>()).add(response.evaluate("items." + i + ".index._id"));
                            acked.merge(indexName, 1L, Long::sum);
                        } else {
                            failedItemsByStatus.merge(status, 1L, Long::sum);
                            if (failedItems++ == 0) {
                                firstError = response.evaluate("items." + i + ".index.error");
                            }
                        }
                    }
                    if (failedItems > 0) {
                        logger.info("--> [{}/{}] bulk items to [{}] failed, first error: {}", failedItems, numItems, indexName, firstError);
                    }
                } catch (ResponseException e) {
                    failedBulks++;
                    logger.info("--> bulk request to [{}] failed: {}", indexName, e.getMessage());
                } catch (IOException e) {
                    failedBulks++;
                    logger.info("--> bulk request to [{}] failed", indexName, e);
                }
                safeSleep(random.nextInt(50));
            }
            logger.info(
                "--> indexer done: acked {}, attempted {}, failed bulk requests [{}], failed items by status {}",
                acked,
                attempted,
                failedBulks,
                failedItemsByStatus
            );
        }
    }

    /// Refreshes and then searches random indices.
    /// Each successful search request must see every write acked before the refresh.
    private class Searcher {
        private final List<String> indices;
        private final Map<String, Long> acked;
        private final Map<String, Long> attempted;
        private final Random random;

        /// Searches whose hit count was checked.
        private long verifiedSearches;
        private long incompleteSearches;
        private long failedSearches;

        Searcher(List<String> indices, Map<String, Long> acked, Map<String, Long> attempted, Random random) {
            this.indices = indices;
            this.acked = acked;
            this.attempted = attempted;
            this.random = random;
        }

        void searchUntil(AtomicBoolean done) {
            while (done.get() == false) {
                final var indexName = indices.get(random.nextInt(indices.size()));
                final long ackedBeforeRefresh = acked.getOrDefault(indexName, 0L);
                try {
                    final var refresh = ObjectPath.createFromResponse(
                        client().performRequest(new Request("POST", "/" + indexName + "/_refresh"))
                    );
                    final var search = new Request("GET", "/" + indexName + "/_search");
                    search.setJsonEntity("""
                        {"size":0,"track_total_hits":true}
                        """);
                    final var response = ObjectPath.createFromResponse(client().performRequest(search));
                    if (allShardsSucceeded(refresh) && allShardsSucceeded(response)) {
                        final long hits = ((Number) response.evaluate("hits.total.value")).longValue();
                        final long attemptedAfterSearch = attempted.getOrDefault(indexName, 0L);
                        assertThat(
                            "unexpected hit count for [" + indexName + "]",
                            hits,
                            allOf(greaterThanOrEqualTo(ackedBeforeRefresh), lessThanOrEqualTo(attemptedAfterSearch))
                        );
                        verifiedSearches++;
                    } else {
                        incompleteSearches++;
                        logger.info(
                            "--> refresh or search on [{}] did not succeed on all shards: refresh {}, search {}",
                            indexName,
                            refresh.evaluate("_shards"),
                            response.evaluate("_shards")
                        );
                    }
                } catch (ResponseException e) {
                    failedSearches++;
                    logger.info("--> refresh or search on [{}] failed: {}", indexName, e.getMessage());
                } catch (IOException e) {
                    failedSearches++;
                    logger.info("--> refresh or search on [{}] failed", indexName, e);
                }
                safeSleep(random.nextInt(50));
            }
            logger.info(
                "--> searcher done: verified searches [{}], incomplete searches [{}], failed searches [{}]",
                verifiedSearches,
                incompleteSearches,
                failedSearches
            );
        }
    }
}
