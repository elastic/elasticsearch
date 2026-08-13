/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.compute.operator.exchange.ExchangeService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.plugin.ComputeService;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class WarningsIT extends AbstractEsqlIntegTestCase {

    private static final String OLD_NODE_WARNING = "warning from a data node without esql_driver_warnings support";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    public void testCollectWarnings() throws Exception {
        final String node1, node2;
        if (randomBoolean()) {
            internalCluster().ensureAtLeastNumDataNodes(2);
            node1 = randomDataNode().getName();
            node2 = randomValueOtherThan(node1, () -> randomDataNode().getName());
        } else {
            node1 = randomDataNode().getName();
            node2 = randomDataNode().getName();
        }

        int numDocs1 = randomIntBetween(1, 15);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index-1")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", node1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                )
                .setMapping("host", "type=keyword")
        );
        for (int i = 0; i < numDocs1; i++) {
            client().prepareIndex("index-1").setSource("host", "192." + i).get();
        }
        int numDocs2 = randomIntBetween(1, 15);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("index-2")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", node2)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                )
                .setMapping("host", "type=keyword")
        );
        for (int i = 0; i < numDocs2; i++) {
            client().prepareIndex("index-2").setSource("host", "10." + i).get();
        }

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        client().admin().indices().prepareRefresh("index-1", "index-2").get();

        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(
            "FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100"
        ).pragmas(randomPragmas());
        CountDownLatch latch = new CountDownLatch(1);
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
            try {
                var threadpool = internalCluster().getInstance(TransportService.class, coordinator.getName()).getThreadPool();
                Map<String, List<String>> responseHeaders = threadpool.getThreadContext().getResponseHeaders();
                List<String> warnings = responseHeaders.getOrDefault("Warning", List.of())
                    .stream()
                    .filter(w -> w.contains("is not an IP string literal"))
                    .toList();
                int expectedWarnings = Math.min(20, numDocs1 + numDocs2);
                // we cap the number of warnings per node
                assertThat(warnings.size(), greaterThanOrEqualTo(expectedWarnings));
            } finally {
                latch.countDown();
            }
        }));
        latch.await(30, TimeUnit.SECONDS);
    }

    /**
     * Regression test for warnings emitted by the reduction driver being lost.
     * <p>
     * When node-level reduction is enabled and the data node and coordinator are on different nodes,
     * the reduction driver's completion listener may fire on a transport thread that does not carry
     * the thread context accumulated during the reduction driver's execution. Before the fix, this
     * caused any warnings emitted by the reduction driver (e.g. SUM long overflow) to be silently
     * dropped instead of propagated to the response.
     * <p>
     * Exchange fetch requests from the coordinator to the data node are delayed so that the
     * reduction driver always finishes and registers its completion listener before the coordinator's
     * {@code fetchPageAsync(sourceFinished=true)} arrives. Without the delay the completion future
     * can already be done when {@code addCompletionListener} is called, firing it synchronously on
     * the search-executor thread and masking the bug.
     */
    public void testReductionDriverWarningsAreNotLost() throws Exception {
        assumeTrue("pragmas only enabled on snapshot builds", canUseQueryPragmas());
        internalCluster().ensureAtLeastNumDataNodes(2);

        String dataNodeName = randomDataNode().getName();
        // coordinator must be on a different node to trigger the node-level reduction path
        String coordinatorName = randomValueOtherThan(dataNodeName, () -> randomDataNode().getName());

        // Long.MAX_VALUE / 2 + 1: each shard's per-batch partial sum will not overflow on its own,
        // but when the reduction driver combines two shards' partial sums via addIntermediateInput
        // it overflows, generating a SUM overflow warning on the reduction driver thread.
        long largeValue = Long.MAX_VALUE / 2 + 1;

        for (String index : new String[] { "reduction-warning-a", "reduction-warning-b" }) {
            assertAcked(
                client().admin()
                    .indices()
                    .prepareCreate(index)
                    .setSettings(
                        Settings.builder()
                            .put("index.routing.allocation.require._name", dataNodeName)
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    )
                    .setMapping("value", "type=long")
            );
            client().prepareIndex(index).setSource("value", largeValue).get();
        }
        client().admin().indices().prepareRefresh("reduction-warning-a", "reduction-warning-b").get();

        // Delay all exchange fetch requests so the reduction driver always completes and registers
        // its completion listener before the coordinator's final fetch arrives on the transport thread.
        var coordinatorTs = (MockTransportService) internalCluster().getInstance(TransportService.class, coordinatorName);
        var dataNodeTs = internalCluster().getInstance(TransportService.class, dataNodeName);
        coordinatorTs.addSendBehavior(dataNodeTs, (connection, requestId, action, request, options) -> {
            if (ExchangeService.EXCHANGE_ACTION_NAME.equals(action)) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            connection.sendRequest(requestId, action, request, options);
        });

        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest(
            "FROM reduction-warning-a,reduction-warning-b | STATS total = SUM(value)"
        )
            .pragmas(
                new QueryPragmas(
                    Settings.builder()
                        .put("node_level_reduction", true)
                        // one shard per batch so each shard produces a separate intermediate page;
                        // combining two pages in the reduction driver then triggers SUM overflow
                        .put("max_concurrent_shards_per_node", 1)
                        .build()
                )
            );

        CountDownLatch latch = new CountDownLatch(1);
        try {
            client(coordinatorName).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
                try {
                    var threadpool = internalCluster().getInstance(TransportService.class, coordinatorName).getThreadPool();
                    List<String> warnings = threadpool.getThreadContext()
                        .getResponseHeaders()
                        .getOrDefault("Warning", List.of())
                        .stream()
                        .filter(w -> w.contains("long overflow"))
                        .toList();
                    assertThat(
                        "SUM overflow warning emitted by the reduction driver should reach the response",
                        warnings.size(),
                        greaterThanOrEqualTo(1)
                    );
                } finally {
                    latch.countDown();
                }
            }));
            latch.await(30, TimeUnit.SECONDS);
        } finally {
            coordinatorTs.clearAllRules();
        }
    }

    /**
     * Regression test for warnings raised by a data node too old to report them in
     * {@link org.elasticsearch.compute.operator.DriverCompletionInfo#warnings()}.
     * <p>
     * Such a node predates the {@code esql_driver_warnings} wire field, so it raises warnings through
     * {@link HeaderWarning} instead. Those only reach the coordinator as transport response headers, serialised
     * from the thread context of whichever thread sends the data node response. The coordinator must then carry
     * them from the thread that handles that response to the thread that renders the final response.
     * <p>
     * The old node is simulated by adding a warning to the data node's thread context immediately before its
     * channel serialises the response, which is exactly where an old node's warnings would already be sitting.
     * The warning deliberately never exists in the completion info, so it can only reach the client if the
     * coordinator handles header-only warnings.
     */
    public void testWarningsArrivingOnlyAsResponseHeaders() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);

        String dataNodeName = randomDataNode().getName();
        // the coordinator must be a different node, so the data node's response crosses the transport layer
        String coordinatorName = randomValueOtherThan(dataNodeName, () -> randomDataNode().getName());

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("legacy-warnings")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", dataNodeName)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                )
                .setMapping("value", "type=long")
        );
        client().prepareIndex("legacy-warnings").setSource("value", 1).get();
        client().admin().indices().prepareRefresh("legacy-warnings").get();

        AtomicInteger injected = new AtomicInteger();
        var dataNodeTs = asInstanceOf(MockTransportService.class, internalCluster().getInstance(TransportService.class, dataNodeName));
        // deliberately not HeaderWarning.addWarning, which writes to every registered thread context: in a single JVM
        // that includes the coordinator's, which would let the warning reach the response without being serialised
        var dataNodeThreadContext = dataNodeTs.getThreadPool().getThreadContext();
        dataNodeTs.addRequestHandlingBehavior(
            ComputeService.DATA_ACTION_NAME,
            (handler, request, channel, task) -> handler.messageReceived(request, new TransportChannel() {
                @Override
                public String getProfileName() {
                    return channel.getProfileName();
                }

                @Override
                public void sendResponse(TransportResponse response) {
                    // where an old node's warnings already sit when its channel serialises the response
                    dataNodeThreadContext.addResponseHeader("Warning", HeaderWarning.formatWarning(OLD_NODE_WARNING));
                    injected.incrementAndGet();
                    channel.sendResponse(response);
                }

                @Override
                public void sendResponse(Exception exception) {
                    channel.sendResponse(exception);
                }
            }, task)
        );

        // captured inside the listener because the response headers only live on the thread that rendered the response
        AtomicReference<List<String>> responseWarnings = new AtomicReference<>(List.of());
        CountDownLatch latch = new CountDownLatch(1);
        try {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest("FROM legacy-warnings | LIMIT 10");
            client(coordinatorName).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
                try {
                    var threadPool = internalCluster().getInstance(TransportService.class, coordinatorName).getThreadPool();
                    responseWarnings.set(threadPool.getThreadContext().getResponseHeaders().getOrDefault("Warning", List.of()));
                } finally {
                    latch.countDown();
                }
            }));
            assertTrue("query did not complete", latch.await(30, TimeUnit.SECONDS));
        } finally {
            dataNodeTs.clearAllRules();
        }

        // guards against the query being planned without a remote data node request, which would make the test vacuous
        assertThat("the data node never responded, so no warning was injected", injected.get(), greaterThanOrEqualTo(1));
        assertThat(
            "a warning that exists only as a response header on the data node response should reach the client",
            responseWarnings.get().stream().filter(w -> w.contains(OLD_NODE_WARNING)).toList(),
            hasSize(greaterThanOrEqualTo(1))
        );
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
