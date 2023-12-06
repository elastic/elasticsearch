/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

@TestLogging(value = "org.elasticsearch.xpack.esql:TRACE", reason = "debug")
public class WarningsIT extends AbstractEsqlIntegTestCase {

    public void testCollectWarnings() {
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
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node1))
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
                .setSettings(Settings.builder().put("index.routing.allocation.require._name", node2))
                .setMapping("host", "type=keyword")
        );
        for (int i = 0; i < numDocs2; i++) {
            client().prepareIndex("index-2").setSource("host", "10." + i).get();
        }

        DiscoveryNode coordinator = randomFrom(clusterService().state().nodes().stream().toList());
        client().admin().indices().prepareRefresh("index-1", "index-2").get();

        EsqlQueryRequest request = new EsqlQueryRequest();
        request.query("FROM index-* | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip | LIMIT 100");
        request.pragmas(randomPragmas());
        PlainActionFuture<EsqlQueryResponse> future = new PlainActionFuture<>();
        client(coordinator.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.runBefore(future, () -> {
            var threadpool = internalCluster().getInstance(TransportService.class, coordinator.getName()).getThreadPool();
            Map<String, List<String>> responseHeaders = threadpool.getThreadContext().getResponseHeaders();
            List<String> warnings = responseHeaders.getOrDefault("Warning", List.of())
                .stream()
                .filter(w -> w.contains("is not an IP string literal"))
                .toList();
            int expectedWarnings = Math.min(20, numDocs1 + numDocs2);
            // we cap the number of warnings per node
            assertThat(warnings.size(), greaterThanOrEqualTo(expectedWarnings));
        }));
        future.actionGet(30, TimeUnit.SECONDS).close();
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
