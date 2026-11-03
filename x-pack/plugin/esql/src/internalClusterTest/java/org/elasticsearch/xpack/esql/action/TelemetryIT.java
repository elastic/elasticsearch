/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

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
import org.elasticsearch.test.ESIntegTestCase.SuiteScopeTestCase;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.telemetry.PlanTelemetryManager;

import java.time.ZoneId;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.esql.action.EsqlQueryRequest.syncEsqlQueryRequest;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@SuiteScopeTestCase
public class TelemetryIT extends AbstractEsqlIntegTestCase {

    private final String query;
    private final Map<QuerySettingDef<?>, ?> requestSettings;
    private final Map<String, Integer> expectedCommands;
    private final Map<String, Integer> expectedFunctions;
    private final Map<String, Integer> expectedSettings;
    private final Map<String, String> expectedNonDefaultResolvedSettings;
    private final boolean success;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return List.of(
            testCase(
                """
                    FROM idx
                    | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                    | STATS s = COUNT(*) by ip
                    | KEEP ip
                    | EVAL a = 10""",
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 2), Map.entry("STATS", 1), Map.entry("KEEP", 1)),
                Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2), Map.entry("COUNT", 1)),
                true
            ),
            testCase(
                "FROM idx | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host) "
                    + "| STATS s = COUNT(*) by ip | KEEP ip | EVAL a = non_existing",
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 2), Map.entry("STATS", 1), Map.entry("KEEP", 1)),
                Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2), Map.entry("COUNT", 1)),
                false
            ),
            testCase(
                """
                    FROM idx
                    | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                    | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                    | STATS s = COUNT(*) by ip | KEEP ip | EVAL a = 10
                    """,
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 3), Map.entry("STATS", 1), Map.entry("KEEP", 1)),
                Map.ofEntries(Map.entry("TO_IP", 2), Map.entry("TO_STRING", 4), Map.entry("COUNT", 1)),
                true
            ),
            testCase(
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
            ),
            testCase(
                """
                    FROM idx
                    | EVAL ip = to_ip(host), x = to_string(host), y = to_string(host)
                    | GROK host "%{WORD:name} %{WORD}"
                    | DISSECT host "%{surname}"
                    """,
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 1), Map.entry("GROK", 1), Map.entry("DISSECT", 1)),
                Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2)),
                true
            ),
            testCase(
                // Using the `::` cast operator and a function alias
                """
                    ROW host = "1.1.1.1"
                    | EVAL ip = host::ip::string, y = to_str(host)
                    """,
                Map.ofEntries(Map.entry("ROW", 1), Map.entry("EVAL", 1)),
                Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2)),
                true
            ),
            testCase(
                // Using the `::` cast operator and a function alias
                """
                    FROM idx
                    | EVAL ip = host::ip::string, y = to_str(host)
                    """,
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 1)),
                Map.ofEntries(Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2)),
                true
            ),
            testCase(
                """
                    FROM idx
                    | EVAL y = to_str(host)
                    | LOOKUP JOIN lookup_idx ON host
                    """,
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("EVAL", 1), Map.entry("LOOKUP JOIN", 1)),
                Map.ofEntries(Map.entry("TO_STRING", 1)),
                true
            ),
            testCase("""
                FROM idx
                | LOOKUP JOIN _coordinator:lookup_idx ON host
                """, Map.ofEntries(Map.entry("FROM", 1), Map.entry("COORDINATOR LOOKUP JOIN", 1)), Map.ofEntries(), true),
            testCase("""
                FROM idx
                | LOOKUP JOIN _coordinator:lookup_idx ON host
                | LOOKUP JOIN _coordinator:lookup_idx ON host
                """, Map.ofEntries(Map.entry("FROM", 1), Map.entry("COORDINATOR LOOKUP JOIN", 2)), Map.ofEntries(), true),
            testCase(
                """
                    FROM idx
                    | LOOKUP JOIN lookup_idx ON host
                    | LOOKUP JOIN _coordinator:lookup_idx ON host
                    """,
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("LOOKUP JOIN", 1), Map.entry("COORDINATOR LOOKUP JOIN", 1)),
                Map.ofEntries(),
                true
            ),
            testCase(
                """
                    FROM idx
                    | LOOKUP JOIN _coordinator:lookup_idx ON host
                    | LOOKUP JOIN lookup_idx ON host
                    """,
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("LOOKUP JOIN", 1), Map.entry("COORDINATOR LOOKUP JOIN", 1)),
                Map.ofEntries(),
                true
            ),
            testCase(
                """
                    FROM idx
                    | RENAME host as host_left
                    | LOOKUP JOIN _coordinator:lookup_idx ON host_left == host
                    """,
                Map.ofEntries(Map.entry("FROM", 1), Map.entry("RENAME", 1), Map.entry("COORDINATOR LOOKUP JOIN ON EXPRESSION", 1)),
                Map.ofEntries(),
                true
            ),
            testCase(
                """
                    FROM idx
                    | EVAL y = to_str(host)
                    | RENAME host as host_left
                    | LOOKUP JOIN lookup_idx ON host_left == host
                    """,
                Map.ofEntries(
                    Map.entry("RENAME", 1),
                    Map.entry("FROM", 1),
                    Map.entry("EVAL", 1),
                    Map.entry("LOOKUP JOIN ON EXPRESSION", 1)
                ),
                Map.ofEntries(Map.entry("TO_STRING", 1)),
                true
            ),
            testCase("TS time_series_idx | LIMIT 10", Map.ofEntries(Map.entry("TS", 1), Map.entry("LIMIT", 1)), Map.ofEntries(), true),
            testCase("""
                FROM idx
                | LIMIT 3 BY host
                """, Map.ofEntries(Map.entry("FROM", 1), Map.entry("LIMIT BY", 1)), Map.ofEntries(), true),
            testCase("""
                FROM idx
                | SORT id
                | LIMIT 3 BY host
                """, Map.ofEntries(Map.entry("FROM", 1), Map.entry("SORT", 1), Map.entry("LIMIT BY", 1)), Map.ofEntries(), true),
            testCase(
                "TS time_series_idx | STATS max(cpu) BY host | LIMIT 10",
                Map.ofEntries(Map.entry("TS", 1), Map.entry("STATS", 1), Map.entry("LIMIT", 1)),
                Map.ofEntries(Map.entry("MAX", 1)),
                true
            ),
            testCase(
                """
                    FROM idx
                    | EVAL ip = TO_IP(host), x = TO_STRING(host), y = TO_STRING(host)
                    | INLINE STATS MAX(id)
                    """,
                EsqlCapabilities.Cap.INLINE_STATS.isEnabled() ? Map.of("FROM", 1, "EVAL", 1, "INLINE STATS", 1) : Collections.emptyMap(),
                EsqlCapabilities.Cap.INLINE_STATS.isEnabled()
                    ? Map.ofEntries(Map.entry("MAX", 1), Map.entry("TO_IP", 1), Map.entry("TO_STRING", 2))
                    : Collections.emptyMap(),
                EsqlCapabilities.Cap.INLINE_STATS.isEnabled()
            ),
            testCase(
                """
                    FROM idx, (FROM idx | WHERE host =="127.0.0.1")
                    | WHERE id > 10
                    """,
                EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled()
                    ? Map.of("FROM", 2, "WHERE", 2, "SUBQUERY", 1)
                    : Collections.emptyMap(),
                Collections.emptyMap(),
                EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled()
            ),
            // Implicit cast shouldn't add extra metrics
            testCase("""
                FROM idx
                | EVAL x = DATE_DIFF("hours", "2021-01-02T00:00:00", "2021-01-02T00:00:00Z")
                """, Map.of("FROM", 1, "EVAL", 1), Map.of("DATE_DIFF", 1), true),
            // Test with settings
            testCase("""
                SET time_zone = "UTC";
                FROM idx
                | EVAL ip = to_ip(host)
                """, Map.of("FROM", 1, "EVAL", 1), Map.of("TO_IP", 1), Map.of("TIME_ZONE", 1), Map.of("TIME_ZONE", "default"), true),
            // Test with multiple settings
            testCase(
                """
                    SET time_zone = "UTC";
                    SET unmapped_fields = "NULLIFY";
                    FROM idx
                    | KEEP host
                    """,
                Map.of("FROM", 1, "KEEP", 1),
                Map.of(),
                Map.of("TIME_ZONE", 1, "UNMAPPED_FIELDS", 1),
                Map.of("TIME_ZONE", "default", "UNMAPPED_FIELDS", "nullify"),
                true
            ),
            // Test with duplicate settings (both should be counted)
            testCase("""
                SET time_zone = "UTC";
                SET time_zone = "America/New_York";
                FROM idx
                | LIMIT 10
                """, Map.of("FROM", 1, "LIMIT", 1), Map.of(), Map.of("TIME_ZONE", 2), Map.of("TIME_ZONE", "set"), true),
            // Test with settings supplied in the request body rather than the query.
            // The resolved values reflect them.
            // The per-setting usage counters only see in-query SET, so none are expected here.
            // (see also: https://github.com/elastic/elasticsearch/issues/160257)
            testCase(
                "FROM idx | KEEP host",
                Map.of(QuerySettings.TIME_ZONE, ZoneId.of("America/New_York"), QuerySettings.COLUMN_METADATA, true),
                Map.of("FROM", 1, "KEEP", 1),
                Map.of(),
                Map.of(),
                Map.of("TIME_ZONE", "set", "COLUMN_METADATA", "true"),
                true
            ),
            // Test with settings from both the request body and the query.
            testCase(
                """
                    SET unmapped_fields = "NULLIFY";
                    FROM idx
                    | KEEP host
                    """,
                Map.of(QuerySettings.TIME_ZONE, ZoneId.of("America/New_York"), QuerySettings.UNMAPPED_FIELDS, "load"),
                Map.of("FROM", 1, "KEEP", 1),
                Map.of(),
                Map.of("UNMAPPED_FIELDS", 1),
                Map.of("TIME_ZONE", "set", "UNMAPPED_FIELDS", "nullify"),
                true
            ),
            // Test without settings: each one reports the value it resolved to, which is its default
            testCase(
                "FROM idx | LIMIT 10",
                Map.of("FROM", 1, "LIMIT", 1),
                Map.of(),
                Map.of(),
                Map.of("TIME_ZONE", "default", "UNMAPPED_FIELDS", "default", "COLUMN_METADATA", "false", "APPROXIMATION", "false"),
                true
            )
        );
    }

    private static Object[] testCase(
        String query,
        Map<String, Integer> expectedCommands,
        Map<String, Integer> expectedFunctions,
        boolean success
    ) {
        return testCase(query, expectedCommands, expectedFunctions, Map.of(), success);
    }

    private static Object[] testCase(
        String query,
        Map<String, Integer> expectedCommands,
        Map<String, Integer> expectedFunctions,
        Map<String, Integer> expectedSettings,
        boolean success
    ) {
        return testCase(query, expectedCommands, expectedFunctions, expectedSettings, Map.of(), success);
    }

    private static Object[] testCase(
        String query,
        Map<String, Integer> expectedCommands,
        Map<String, Integer> expectedFunctions,
        Map<String, Integer> expectedSettings,
        Map<String, String> expectedSettingValues,
        boolean success
    ) {
        return testCase(query, Map.of(), expectedCommands, expectedFunctions, expectedSettings, expectedSettingValues, success);
    }

    private static Object[] testCase(
        String query,
        Map<QuerySettingDef<?>, ?> requestSettings,
        Map<String, Integer> expectedCommands,
        Map<String, Integer> expectedFunctions,
        Map<String, Integer> expectedSettings,
        Map<String, String> expectedSettingValues,
        boolean success
    ) {
        return new Object[] {
            query,
            requestSettings,
            expectedCommands,
            expectedFunctions,
            expectedSettings,
            expectedSettingValues,
            success };
    }

    public TelemetryIT(
        String query,
        Map<QuerySettingDef<?>, ?> requestSettings,
        Map<String, Integer> expectedCommands,
        Map<String, Integer> expectedFunctions,
        Map<String, Integer> expectedSettings,
        Map<String, String> expectedNonDefaultResolvedSettings,
        boolean success
    ) {
        this.query = query;
        this.requestSettings = requestSettings;
        this.expectedCommands = expectedCommands;
        this.expectedFunctions = expectedFunctions;
        this.expectedSettings = expectedSettings;
        this.expectedNonDefaultResolvedSettings = expectedNonDefaultResolvedSettings;
        this.success = success;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestTelemetryPlugin.class);
    }

    @Override
    protected void setupSuiteScopeCluster() {
        loadData(randomDataNode().getName());
    }

    public void testMetrics() throws Exception {
        if (query.contains("LOOKUP JOIN lookup_idx ON host_left == host")) {
            assumeTrue(
                "requires LOOKUP JOIN ON boolean expression capability",
                EsqlCapabilities.Cap.LOOKUP_JOIN_ON_BOOLEAN_EXPRESSION.isEnabled()
            );
        }

        DiscoveryNode dataNode = randomDataNode();
        final var plugins = internalCluster().getInstance(PluginsService.class, dataNode.getName())
            .filterPlugins(TestTelemetryPlugin.class)
            .toList();
        assertThat(plugins, hasSize(1));
        TestTelemetryPlugin plugin = plugins.getFirst();

        // Every query that gets past parsing reports every applicable resolved setting.
        // The expected value is in `expectedNonDefaultResolvedSettings` or the default if not.
        Map<String, String> expectedResolvedSettings = new HashMap<>();
        if (expectedCommands.isEmpty() == false) {
            for (QuerySettingDef<?> def : QuerySettings.applicableIn(Build.current().isSnapshot(), false)) {
                String name = def.name().toUpperCase(Locale.ROOT);
                expectedResolvedSettings.put(name, defaultLabel(def));
            }
            expectedResolvedSettings.putAll(expectedNonDefaultResolvedSettings);
        }

        try {
            int successIterations = randomInt(10);
            for (int i = 0; i < successIterations; i++) {
                EsqlQueryRequest request = executeQuery(query, requestSettings);
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

                        // test total settings used
                        final List<Measurement> settingMeasurementsAll = measurements(plugin, PlanTelemetryManager.SETTING_METRICS_ALL);
                        assertAllUsages(expectedSettings, settingMeasurementsAll, iteration, success);

                        // test number of queries using a setting
                        final List<Measurement> settingMeasurements = measurements(plugin, PlanTelemetryManager.SETTING_METRICS);
                        assertUsageInQuery(expectedSettings, settingMeasurements, iteration, success);

                        // test number of queries in which a setting resolved to a value
                        // this covers every setting and not only the ones explicitly in the query
                        final var resolvedSettingsMeasurements = measurements(plugin, PlanTelemetryManager.RESOLVED_SETTINGS_METRICS);
                        assertUsageInQuery(expectedResolvedSettings, resolvedSettingsMeasurements, iteration, success);
                        assertResolvedSettings(expectedResolvedSettings, resolvedSettingsMeasurements);
                    } finally {
                        latch.countDown();
                    }
                }));
                assertTrue(latch.await(30, TimeUnit.SECONDS));
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

    private static void assertUsageInQuery(Map<String, ?> expected, List<Measurement> found, long iteration, Boolean success) {
        Set<String> functionsFound;
        functionsFound = featureNames(found);
        assertThat(functionsFound, is(expected.keySet()));
        for (Measurement measurement : found) {
            assertThat(measurement.attributes().get(PlanTelemetryManager.SUCCESS), is(success));
            assertThat(measurement.getLong(), is(iteration));
        }
    }

    private static void assertResolvedSettings(Map<String, String> expected, List<Measurement> metrics) {
        Map<String, String> found = metrics.stream()
            .collect(
                Collectors.toMap(
                    m -> (String) m.attributes().get(PlanTelemetryManager.FEATURE_NAME),
                    m -> (String) m.attributes().get(PlanTelemetryManager.SETTING_VALUE)
                )
            );
        assertThat(found, is(expected));
    }

    private static <T> String defaultLabel(QuerySettingDef<T> def) {
        return def.telemetryLabel(def.defaultValue());
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

    private static EsqlQueryRequest executeQuery(String query, Map<QuerySettingDef<?>, ?> requestSettings) {
        EsqlQueryRequest request = syncEsqlQueryRequest(query).pragmas(randomPragmas());
        requestSettings.forEach((def, value) -> setRequestSetting(request, def, value));
        return request;
    }

    @SuppressWarnings("unchecked")
    private static <T> void setRequestSetting(EsqlQueryRequest request, QuerySettingDef<T> def, Object value) {
        request.set(def, (T) value);
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
}
