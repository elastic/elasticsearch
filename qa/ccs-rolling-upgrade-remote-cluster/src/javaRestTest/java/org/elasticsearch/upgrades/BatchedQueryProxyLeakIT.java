/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/// Reproduces the Netty buffer leak from incident-management#3360 with real mixed-version clusters.
///
/// The local cluster connects to the remote cluster in proxy mode, like cross-project search in serverless does, so a query sent to any
/// remote node other than the proxy address goes through that node acting as a [org.elasticsearch.transport.TransportActionProxy].
/// When the coordinator-to-proxy and proxy-to-target connections use different transport versions, which happens during
/// most upgrades, the proxy deserializes the batched query phase response instead of passing its bytes through, and never
/// releases the network buffer that backs its serialized aggregations.
public class BatchedQueryProxyLeakIT extends AbstractCcsRollingUpgradeTestCase {

    private static final String CLUSTER_ALIAS = "remote_cluster";
    private static final String REMOTE_INDEX = "batched_query_proxy_leak";
    private static final int SEARCHES = 50;
    private static final int SEARCHES_AFTER_GC = 5;

    public BatchedQueryProxyLeakIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testProxiedBatchedQueryDoesNotLeak() throws Exception {
        assumeTrue(
            "the batched query phase needs every node on 9.1.0 or later",
            Version.fromString(getOldClusterVersion()).onOrAfter(Version.V_9_1_0)
        );
        try (RestClient remoteClient = newRemoteClient()) {
            final List<SearchStatesIT.Node> remoteNodes = AggregationsIT.getNodes(remoteClient);
            final String targetNode = remoteNodes.stream()
                .filter(n -> n.attributes().containsKey("gateway") == false)
                .map(SearchStatesIT.Node::name)
                .findFirst()
                .orElseThrow();
            // The node the local cluster connects to and that proxies the queries for the target node. Which one sees mismatched
            // versions depends on the upgrade phase, so run the searches through each of them.
            final List<String> proxyAddresses = shuffledList(
                remoteNodes.stream().filter(n -> n.attributes().containsKey("gateway")).map(SearchStatesIT.Node::transportAddress).toList()
            );
            // A coordinator only batches the query phase for a node holding at least two shards.
            createIndex(
                remoteClient,
                REMOTE_INDEX,
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(2, 5))
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.routing.allocation.include._name", targetNode)
                    .build(),
                """
                    "properties": { "ts": { "type": "long" }, "keyword": { "type": "keyword" } }
                    """
            );
            // Remote nodes keep their log across phases, only look at what this phase writes
            final long[] logLinesBefore = new long[REMOTE_NODE_NUM];
            for (int node = 0; node < REMOTE_NODE_NUM; node++) {
                try (BufferedReader reader = remoteNodeLogReader(node)) {
                    logLinesBefore[node] = reader.lines().count();
                }
            }
            try {
                indexDocs(remoteClient, between(100, 500));
                // Hit them all, so we are certain to hit at least one proxy whose two hops (coordinator->proxy and
                // proxy->target) use different transport versions
                for (String proxyAddress : proxyAddresses) {
                    configureRemoteCluster(proxyAddress);
                    for (int i = 0; i < SEARCHES; i++) {
                        search();
                    }
                    // Netty only notices a leaked buffer once the garbage collector has reclaimed it, and only reports
                    // it on a later allocation of a tracked buffer, so we do extra searches after the GC.
                    forceGcOnRemoteNodes();
                    for (int i = 0; i < SEARCHES_AFTER_GC; i++) {
                        search();
                    }
                }
                final List<String> leaks = findLeakReports(logLinesBefore);
                assertTrue("Netty buffers leaked on the remote cluster:\n" + String.join("\n", leaks), leaks.isEmpty());
            } finally {
                deleteIndex(remoteClient, REMOTE_INDEX);
            }
        }
    }

    private static void indexDocs(RestClient remoteClient, int numDocs) throws IOException {
        final StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < numDocs; i++) {
            bulk.append(Strings.format("""
                { "index": {} }
                { "ts": %d, "keyword": "k%d" }
                """, i, i % 50));
        }
        final Request request = new Request("POST", "/" + REMOTE_INDEX + "/_bulk");
        request.addParameter("refresh", "true");
        request.setJsonEntity(bulk.toString());
        assertOK(remoteClient.performRequest(request));
    }

    /// Same shape as the searches that leaked in the original instance: a `terms` aggregation plus `min` and `max`.
    private void search() throws IOException {
        try (RestClient localClient = newLocalClient()) {
            final Request request = new Request("POST", "/" + CLUSTER_ALIAS + ":" + REMOTE_INDEX + "/_search");
            request.addParameter("ccs_minimize_roundtrips", "false");
            request.setJsonEntity("""
                {
                  "size": 0,
                  "aggs": {
                    "keywords": { "terms": { "field": "keyword", "size": 100 } },
                    "min_ts": { "min": { "field": "ts" } },
                    "max_ts": { "max": { "field": "ts" } }
                  }
                }
                """);
            final ObjectPath response = ObjectPath.createFromResponse(localClient.performRequest(request));
            assertEquals(0, (int) response.evaluate("_shards.failed"));
        }
    }

    /// Runs `jcmd <pid> GC.run` on every remote node.
    private static void forceGcOnRemoteNodes() throws IOException, InterruptedException {
        final String jcmd = Path.of(System.getProperty("java.home"), "bin", "jcmd").toString();
        for (int node = 0; node < REMOTE_NODE_NUM; node++) {
            final ProcessHandle launcher = ProcessHandle.of(getRemoteNodePid(node)).orElseThrow();
            final List<ProcessHandle> jvms = Stream.concat(Stream.of(launcher), launcher.descendants())
                .filter(p -> p.info().command().map(c -> c.endsWith("java")).orElse(false))
                .toList();
            assertFalse("no JVM found for remote node " + node, jvms.isEmpty());
            for (ProcessHandle jvm : jvms) {
                final Process gc = new ProcessBuilder(jcmd, Long.toString(jvm.pid()), "GC.run").redirectErrorStream(true).start();
                final String output = new String(gc.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
                assertEquals("jcmd GC.run failed on remote node " + node + ": " + output, 0, gc.waitFor());
            }
        }
    }

    private static List<String> findLeakReports(long[] logLinesBefore) throws IOException {
        final List<String> leaks = new ArrayList<>();
        for (int node = 0; node < REMOTE_NODE_NUM; node++) {
            try (BufferedReader reader = remoteNodeLogReader(node)) {
                final int nodeIndex = node;
                reader.lines()
                    .skip(logLinesBefore[node])
                    .filter(line -> line.contains("LEAK:"))
                    .forEach(line -> leaks.add("remote node " + nodeIndex + ": " + line));
            }
        }
        return leaks;
    }

    private static BufferedReader remoteNodeLogReader(int node) {
        return new BufferedReader(new InputStreamReader(getRemoteNodeLog(node), StandardCharsets.UTF_8));
    }

    /// Connects to the remote cluster in proxy mode through `proxyAddress` only. The proxy mode strategy ignores the
    /// `cluster.remote.node.attr` that the base class sets on the local cluster.
    private void configureRemoteCluster(String proxyAddress) throws Exception {
        try (RestClient localClient = newLocalClient()) {
            updateClusterSettings(
                localClient,
                Settings.builder()
                    .putNull("cluster.remote." + CLUSTER_ALIAS + ".seeds")
                    .put("cluster.remote." + CLUSTER_ALIAS + ".mode", "proxy")
                    .put("cluster.remote." + CLUSTER_ALIAS + ".proxy_address", proxyAddress)
                    .build()
            );
            assertBusy(() -> {
                final ObjectPath remoteInfo = ObjectPath.createFromResponse(
                    localClient.performRequest(new Request("GET", "/_remote/info"))
                );
                assertEquals(proxyAddress, remoteInfo.evaluate(CLUSTER_ALIAS + ".proxy_address"));
                assertTrue(remoteInfo.evaluate(CLUSTER_ALIAS + ".connected"));
            }, 30, TimeUnit.SECONDS);
        }
    }

    private RestClient newLocalClient() {
        return RestClient.builder(randomFrom(SearchStatesIT.parseHosts(getTestRestCluster()))).build();
    }

    private RestClient newRemoteClient() {
        return RestClient.builder(randomFrom(SearchStatesIT.parseHosts(getRemoteClusterRestAddresses()))).build();
    }
}
