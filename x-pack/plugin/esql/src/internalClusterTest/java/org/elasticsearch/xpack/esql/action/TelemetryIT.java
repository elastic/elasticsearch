/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.Build;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager;
import org.junit.Before;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TelemetryIT extends AbstractEsqlIntegTestCase {

    record Test(String query, Map<String, Integer> expectedCommands, Map<String, Integer> expectedFunctions, boolean success) {}

    private final Test testCase;

    public TelemetryIT(@Name("TestCase") Test test) {
        this.testCase = test;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            new Object[] {
                new Test(
                    """
                        FROM idx
                        | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                        | STATS s = COUNT(*) by ip
                        | KEEP ip
                        | EVAL a = 10""",
                    Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 2), Map.entry("STATS", 1), Map.entry("KEEP", 1)),
                    Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2), Map.entry("COUNT", 1)),
                    true
                ) },
            new Object[] {
                new Test(
                    "FROM idx | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host) "
                        + "| STATS s = COUNT(*) by ip | KEEP ip | EVAL a = non_existing",
                    Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 2), Map.entry("STATS", 1), Map.entry("KEEP", 1)),
                    Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2), Map.entry("COUNT", 1)),
                    false
                ) },
            new Object[] {
                new Test(
                    """
                        FROM idx
                        | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                        | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                        | STATS s = COUNT(*) by ip | KEEP ip | EVAL a = 10
                        """,
                    Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 3), Map.entry("STATS", 1), Map.entry("KEEP", 1)),
                    Map.ofEntries(Map.entry("TO_IP", 2), Map.entry("TO_STRING", 4), Map.entry("COUNT", 1)),
                    true
                ) },
            new Object[] {
                new Test(
                    """
                        FROM idx | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                        | WHERE id is not null AND id > 100 AND host RLIKE \".*foo\"
                        | eval a = 10
                        | drop host
                        | rename a as foo
                        | DROP foo
                        """, // lowercase on purpose
                    Map.ofEntries(
                        Map.entry("FROM", 1),
                        Map.entry("EVAL", 2),
                        Map.entry("WHERE", 1),
                        Map.entry("DROP", 2),
                        Map.entry("RENAME", 1)
                    ),
                    Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2)),
                    true
                ) },
            new Object[] {
                new Test(
                    """
                        FROM idx
                        | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                        | GROK host "%{WORD:name} %{WORD}"
                        | DISSECT host "%{surname}"
                        """,
                    Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 1), Map.entry("GROK", 1), Map.entry("DISSECT", 1)),
                    Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2)),
                    true
                ) },
            new Object[] {
                new Test(
                    // Using the `::` cast operator and a function alias
                    """
                        ROW host = "1.1.1.1"
                        | EVAL ip = host::ip::string, y = to_str(host)
                        """,
                    Map.ofEntries(Map.entry("ROW", 1), Map.entry("EVAL", 1)),
                    Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2)),
                    true
                ) },
            new Object[] {
                new Test(
                    // Using the `::` cast operator and a function alias
                    """
                        FROM idx
                        | EVAL ip = host::ip::string, y = to_str(host)
                        """,
                    Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 1)),
                    Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2)),
                    true
                ) },
            new Object[] {
                new Test(
                    """
                        FROM idx
                        | EVAL y = to_str(host)
                        | LOOKUP JOIN lookup_idx ON host
                        """,
                    Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 1), Map.entry("LOOKUP JOIN", 1)),
                    Map.ofEntries(Map.entry("TO_STRING", 1)),
                    true
                ) },
            new Object[] {
                new Test(
                    "TS time_series_idx | LIMIT 10",
                    Build.current().isSnapshot() ? Map.ofEntries(Map.entry("TS", 1), Map.entry("LIMIT", 1)) : Collections.emptyMap(),
                    Map.ofEntries(),
                    Build.current().isSnapshot()
                ) },
            new Object[] {
                new Test(
                    "TS time_series_idx | STATS max(id) BY host | LIMIT 10",
                    Build.current().isSnapshot()
                        ? Map.ofEntries(Map.entry("TS", 1), Map.entry("STATS", 1), Map.entry("LIMIT", 1))
                        : Collections.emptyMap(),
                    Build.current().isSnapshot() ? Map.ofEntries(Map.entry("MAX", 1)) : Collections.emptyMap(),
                    Build.current().isSnapshot()
                ) }
            // awaits fix for https://github.com/elastic/elasticsearch/issues/116003
            // ,
            // new Object[] {
            // new Test(
            // """
            // FROM idx
            // | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
            // | INLINESTATS max(id)
            // """,
            // Build.current().isSnapshot() ? Map.of("FROM", 1, "EVAL", 1, "INLINESTATS", 1) : Collections.emptyMap(),
            // Build.current().isSnapshot()
            // ? Map.ofEntries(Map.entry("MAX", 1), Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2))
            // : Collections.emptyMap(),
            // Build.current().isSnapshot()
            // ) }
        );
    }

    @Before
    public void init() {
        DiscoveryNode dataNode = randomDataNode();
        final String nodeName = dataNode.getName();
        loadData(nodeName);
    }

    public void testMetrics() throws Exception {
        DiscoveryNode dataNode = randomDataNode();
        testQuery(dataNode, testCase);
    }

    private static void testQuery(DiscoveryNode dataNode, Test test) throws InterruptedException {
        testQuery(dataNode, test.query, test.success, test.expectedCommands, test.expectedFunctions);
    }

    private static void testQuery(
        DiscoveryNode dataNode,
        String query,
        Boolean success,
        Map<String, Integer> expectedCommands,
        Map<String, Integer> expectedFunctions
    ) throws InterruptedException {
        final var plugins = internalCluster().getInstance(PluginsService.class, dataNode.getName())
            .filterPlugins(TestTelemetryPlugin.class)
            .toList();
        assertThat(plugins, hasSize(1));
        TestTelemetryPlugin plugin = plugins.get(0);

        try {
            int successIterations = randomInt(10);
            for (int i = 0; i < successIterations; i++) {
                EsqlQueryRequest request = executeQuery(query);
                CountDownLatch latch = new CountDownLatch(1);

                final long iteration = i + 1;
                client(dataNode.getName()).execute(EsqlQueryAction.INSTANCE, request, ActionListener.running(() -> {
                    try {
                        // test total commands used
                        final List<Measurement> commandMeasurementsAll = measurements(plugin, PlanTelemetryManager.FEATURE_METRICS_ALL);
                        assertAllUsages(expectedCommands, commandMeasurementsAll, iteration, success);

                        // test num of queries using a command
                        final List<Measurement> commandMeasurements = measurements(plugin, PlanTelemetryManager.FEATURE_METRICS);
                        assertUsageInQuery(expectedCommands, commandMeasurements, iteration, success);

                        // test total functions used
                        final List<Measurement> functionMeasurementsAll = measurements(plugin, PlanTelemetryManager.FUNCTION_METRICS_ALL);
                        assertAllUsages(expectedFunctions, functionMeasurementsAll, iteration, success);

                        // test number of queries using a function
                        final List<Measurement> functionMeasurements = measurements(plugin, PlanTelemetryManager.FUNCTION_METRICS);
                        assertUsageInQuery(expectedFunctions, functionMeasurements, iteration, success);
                    } finally {
                        latch.countDown();
                    }
                }));
                latch.await(30, TimeUnit.SECONDS);
            }
        } finally {
            plugin.resetMeter();
        }

    }

    private static void assertAllUsages(Map<String, Integer> expected, List<Measurement> metrics, long iteration, Boolean success) {
        Set<String> found = featureNames(metrics);
        assertThat(found, is(expected.keySet()));
        for (Measurement metric : metrics) {
            assertThat(metric.attributes().get(PlanTelemetryManager.SUCCESS), is(success));
            String featureName = (String) metric.attributes().get(PlanTelemetryManager.FEATURE_NAME);
            assertThat(metric.getLong(), is(iteration * expected.get(featureName)));
        }
    }

    private static void assertUsageInQuery(Map<String, Integer> expected, List<Measurement> found, long iteration, Boolean success) {
        Set<String> functionsFound;
        functionsFound = featureNames(found);
        assertThat(functionsFound, is(expected.keySet()));
        for (Measurement measurement : found) {
            assertThat(measurement.attributes().get(PlanTelemetryManager.SUCCESS), is(success));
            assertThat(measurement.getLong(), is(iteration));
        }
    }

    private static List<Measurement> measurements(TestTelemetryPlugin plugin, String metricKey) {
        return Measurement.combine(plugin.getLongCounterMeasurement(metricKey));
    }

    private static Set<String> featureNames(List<Measurement> functionMeasurements) {
        return functionMeasurements.stream()
            .map(x -> x.attributes().get(PlanTelemetryManager.FEATURE_NAME))
            .map(String.class::cast)
            .collect(Collectors.toSet());
    }

    private static EsqlQueryRequest executeQuery(String query) {
        EsqlQueryRequest request = EsqlQueryRequest.syncEsqlQueryRequest();
        request.query(query);
        request.pragmas(randomPragmas());
        return request;
    }

    private static void loadData(String nodeName) {
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
                .setMapping("host", "type=keyword", "id", "type=long")
        );
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("idx").setSource("host", "192." + i, "id", i).get();
        }

        client().admin().indices().prepareRefresh("idx").get();

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("lookup_idx")
                .setSettings(
                    Settings.builder()
                        .put("index.routing.allocation.require._name", nodeName)
                        .put("index.mode", "lookup")
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                )
                .setMapping("ip", "type=ip", "host", "type=keyword")
        );
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("time_series_idx")
                .setSettings(Settings.builder().put("mode", "time_series").putList("routing_path", List.of("host")).build())
                .setMapping(
                    "@timestamp",
                    "type=date",
                    "id",
                    "type=keyword",
                    "host",
                    "type=keyword,time_series_dimension=true",
                    "cpu",
                    "type=long,time_series_metric=gauge"
                )
        );
    }

    private DiscoveryNode randomDataNode() {
        return randomFrom(clusterService().state().nodes().getDataNodes().values());
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

}
