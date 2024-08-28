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
import org.elasticsearch.xpack.esql.stats.PlanningMetricsManager;

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

        int successIterations = randomInt(10);
        for (int i = 0; i < successIterations; i++) {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            request.query(
                "FROM idx | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host) "
                    + "| STATS s = COUNT(*) by ip | KEEP ip | EVAL a = 10"
            );
            request.pragmas(randomPragmas());
            CountDownLatch latch = new CountDownLatch(1);

            final var plugins = internalCluster().getInstance(PluginsService.class, nodeName)
                .filterPlugins(TestTelemetryPlugin.class)
                .toList();
            assertThat(plugins, hasSize(1));
            TestTelemetryPlugin plugin = plugins.get(0);

            final long iteration = i + 1;
            client(dataNode.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
                try {
                    // test total commands used
                    final List<Measurement> metricsAll = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FEATURE_METRICS_ALL)
                    );
                    Set<String> featuresFound = metricsAll.stream()
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(featuresFound, is(Set.of("FROM", "EVAL", "STATS", "KEEP")));
                    for (Measurement metric : metricsAll) {
                        assertThat(metric.attributes().get(PlanningMetricsManager.SUCCESS), is(true));
                        if ("EVAL".equalsIgnoreCase((String) metric.attributes().get(PlanningMetricsManager.FEATURE_NAME))) {
                            assertThat(metric.getLong(), is(iteration * 2L));
                        } else {
                            assertThat(metric.getLong(), is(iteration));
                        }
                    }

                    // test num of queries using a command
                    final List<Measurement> metrics = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FEATURE_METRICS)
                    );
                    featuresFound = metrics.stream()
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(featuresFound, is(Set.of("FROM", "EVAL", "STATS", "KEEP")));
                    for (Measurement metric : metrics) {
                        assertThat(metric.attributes().get(PlanningMetricsManager.SUCCESS), is(true));
                        assertThat(metric.getLong(), is(iteration));
                    }

                    // test total functions used
                    final List<Measurement> funcitonMeasurementsAll = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FUNCTION_METRICS_ALL)
                    );
                    Set<String> functionNames = funcitonMeasurementsAll.stream()
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(functionNames, is(Set.of("TO_STRING", "TO_IP", "COUNT")));
                    for (Measurement measurement : funcitonMeasurementsAll) {
                        assertThat(measurement.attributes().get(PlanningMetricsManager.SUCCESS), is(true));
                        if ("TO_STRING".equalsIgnoreCase((String) measurement.attributes().get(PlanningMetricsManager.FEATURE_NAME))) {
                            assertThat(measurement.getLong(), is(iteration * 2L));
                        } else {
                            assertThat(measurement.getLong(), is(iteration));
                        }
                    }

                    // test number of queries using a function
                    final List<Measurement> funcitonMeasurements = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FUNCTION_METRICS)
                    );
                    functionNames = funcitonMeasurements.stream()
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(functionNames, is(Set.of("TO_STRING", "TO_IP", "COUNT")));
                    for (Measurement measurement : funcitonMeasurements) {
                        assertThat(measurement.attributes().get(PlanningMetricsManager.SUCCESS), is(true));
                        assertThat(measurement.getLong(), is(iteration));
                    }
                } finally {
                    latch.countDown();
                }
            }));
            latch.await(30, TimeUnit.SECONDS);
        }

        // ----------- failures -------------

        int failureIterations = randomInt(10);
        for (int i = 0; i < failureIterations; i++) {
            EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
            // make it fail with a non-existing attribute
            request.query(
                "FROM idx | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host) "
                    + "| STATS s = COUNT(*) by ip | KEEP ip | EVAL a = non_existing"

            );
            request.pragmas(randomPragmas());
            CountDownLatch latch = new CountDownLatch(1);

            final var plugins = internalCluster().getInstance(PluginsService.class, nodeName)
                .filterPlugins(TestTelemetryPlugin.class)
                .toList();
            assertThat(plugins, hasSize(1));
            TestTelemetryPlugin plugin = plugins.get(0);

            final long iteration = i + 1;
            client(dataNode.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
                try {
                    // test total commands used
                    final List<Measurement> metricsAll = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FEATURE_METRICS_ALL)
                    );
                    Set<String> featuresFound = metricsAll.stream()
                        .filter(x -> Boolean.FALSE.equals(x.attributes().get(PlanningMetricsManager.SUCCESS)))
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(featuresFound, is(Set.of("FROM", "EVAL", "STATS", "KEEP")));
                    for (Measurement metric : metricsAll) {
                        assertThat(metric.attributes().get(PlanningMetricsManager.SUCCESS), is(false));
                        if ("EVAL".equalsIgnoreCase((String) metric.attributes().get(PlanningMetricsManager.FEATURE_NAME))) {
                            assertThat(metric.getLong(), is(successIterations + iteration * 2L));
                        } else {
                            assertThat(metric.getLong(), is(successIterations + iteration));
                        }
                    }

                    // test num of queries using a command
                    final List<Measurement> metrics = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FEATURE_METRICS)
                    );
                    featuresFound = metrics.stream()
                        .filter(x -> Boolean.FALSE.equals(x.attributes().get(PlanningMetricsManager.SUCCESS)))
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(featuresFound, is(Set.of("FROM", "EVAL", "STATS", "KEEP")));
                    for (Measurement metric : metrics) {
                        assertThat(metric.attributes().get(PlanningMetricsManager.SUCCESS), is(false));
                        assertThat(metric.getLong(), is(successIterations + iteration));
                    }

                    // test total functions used
                    final List<Measurement> funcitonMeasurementsAll = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FUNCTION_METRICS_ALL)
                    );
                    Set<String> functionNames = funcitonMeasurementsAll.stream()
                        .filter(x -> Boolean.FALSE.equals(x.attributes().get(PlanningMetricsManager.SUCCESS)))
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(functionNames, is(Set.of("TO_STRING", "TO_IP", "COUNT")));
                    for (Measurement measurement : funcitonMeasurementsAll) {
                        assertThat(measurement.attributes().get(PlanningMetricsManager.SUCCESS), is(false));
                        if ("TO_STRING".equalsIgnoreCase((String) measurement.attributes().get(PlanningMetricsManager.FEATURE_NAME))) {
                            assertThat(measurement.getLong(), is(successIterations + iteration * 2L));
                        } else {
                            assertThat(measurement.getLong(), is(successIterations + iteration));
                        }
                    }

                    // test number of queries using a function
                    final List<Measurement> funcitonMeasurements = Measurement.combine(
                        plugin.getLongCounterMeasurement(PlanningMetricsManager.FUNCTION_METRICS)
                    );
                    functionNames = funcitonMeasurements.stream()
                        .filter(x -> Boolean.FALSE.equals(x.attributes().get(PlanningMetricsManager.SUCCESS)))
                        .map(x -> x.attributes().get(PlanningMetricsManager.FEATURE_NAME))
                        .map(String.class::cast)
                        .collect(Collectors.toSet());
                    assertThat(functionNames, is(Set.of("TO_STRING", "TO_IP", "COUNT")));
                    for (Measurement measurement : funcitonMeasurements) {
                        assertThat(measurement.attributes().get(PlanningMetricsManager.SUCCESS), is(false));
                        assertThat(measurement.getLong(), is(successIterations + iteration));
                    }
                } finally {
                    latch.countDown();
                }
            }));
            latch.await(30, TimeUnit.SECONDS);
        }
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }
}
