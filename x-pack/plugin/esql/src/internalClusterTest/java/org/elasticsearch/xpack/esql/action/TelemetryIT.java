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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.esql.stats.Metrics;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TelemetryIT extends AbstractEsqlIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    public void testMetrics() throws Exception {
        DiscoveryNode dataNode = randomDataNode();
        final String nodeName = dataNode.getName();

        int numDocs = randomIntBetween(1, 15);
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", nodeName)
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1, 5))
                )
                .setMapping("host", "type=keyword")
        );
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("idx").setSource("host", "192." + i).get();
        }

        client().admin().indices().prepareRefresh("idx").get();

        EsqlQueryRequest request = AbstractEsqlIntegTestCase.syncRequestOnLatestVersion();
        request.query("FROM idx | EVAL ip = to_ip(host) | STATS s = COUNT(*) by ip | KEEP ip");
        request.pragmas(randomPragmas());
        CountDownLatch latch = new CountDownLatch(1);

        final var plugins = internalCluster().getInstance(PluginsService.class, nodeName).filterPlugins(TestTelemetryPlugin.class).toList();
        assertThat(plugins, hasSize(1));
        TestTelemetryPlugin plugin = plugins.get(0);

        client(dataNode.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
            try {
                final List<Measurement> metrics = Measurement.combine(plugin.getLongCounterMeasurement(Metrics.FEATURE_METRICS));
                Set<String> featuresFound = metrics.stream()
                    .map(x -> x.attributes().get(Metrics.FEATURE_NAME))
                    .map(String.class::cast)
                    .collect(Collectors.toSet());
                assertThat(featuresFound, is(Set.of("from", "eval", "stats", "keep")));
                for (Measurement metric : metrics) {
                    assertThat(metric.value(), is(1L));
                }
            } finally {
                latch.countDown();
            }
        }));
        latch.await(30, TimeUnit.SECONDS);
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
