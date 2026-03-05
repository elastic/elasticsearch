/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.QuerySettings;

import java.util.Map;

import static org.elasticsearch.xpack.esql.telemetry.Metrics.SETTINGS_PREFIX;
import static org.hamcrest.Matchers.equalTo;

public class SettingsMetricsTests extends ESTestCase {

    /**
     * Creates a Metrics instance with all settings enabled (snapshot + serverless mode).
     */
    private Metrics createMetricsWithAllSettings() {
        return new Metrics(new EsqlFunctionRegistry(), true, true);
    }

    /**
     * Creates a Metrics instance for stateful non-snapshot environment (most restrictive).
     */
    private Metrics createMetricsStatefulNonSnapshot() {
        return new Metrics(new EsqlFunctionRegistry(), false, false);
    }

    /**
     * Creates a Metrics instance for stateful snapshot environment.
     */
    private Metrics createMetricsStatefulSnapshot() {
        return new Metrics(new EsqlFunctionRegistry(), true, false);
    }

    /**
     * Creates a Metrics instance for serverless non-snapshot environment.
     */
    private Metrics createMetricsServerlessNonSnapshot() {
        return new Metrics(new EsqlFunctionRegistry(), false, true);
    }

    /**
     * Helper method to check if a metric exists in the counters.
     * Returns true if the metric exists, false otherwise.
     */
    private boolean hasMetric(Counters counters, String metricName) {
        Map<String, Object> nestedMap = counters.toNestedMap();
        // The metric name is like "settings.time_zone", so we need to navigate the nested map
        String[] parts = metricName.split("\\.");
        Object current = nestedMap;
        for (String part : parts) {
            if (current instanceof Map) {
                current = ((Map<?, ?>) current).get(part);
                if (current == null) {
                    return false;
                }
            } else {
                return false;
            }
        }
        return true;
    }

    public void testSettingsMetricsInitialized_AllEnabled() {
        Metrics metrics = createMetricsWithAllSettings();
        Counters stats = metrics.stats();

        // With all flags enabled, all settings should be registered
        for (String settingName : QuerySettings.SETTINGS_BY_NAME.keySet()) {
            assertTrue("Missing metric for setting: " + settingName, hasMetric(stats, SETTINGS_PREFIX + settingName));
            assertThat(stats.get(SETTINGS_PREFIX + settingName), equalTo(0L));
        }
    }

    public void testSettingsMetricsInitialized_StatefulNonSnapshot() {
        Metrics metrics = createMetricsStatefulNonSnapshot();
        Counters stats = metrics.stats();

        // In stateful non-snapshot mode:
        // - time_zone and unmapped_fields should be registered (not restricted)
        // - approximation should NOT be registered (snapshot-only)
        // - project_routing should NOT be registered (serverless-only)
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "time_zone"));
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "unmapped_fields"));
        assertThat(stats.get(SETTINGS_PREFIX + "time_zone"), equalTo(0L));
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(0L));

        assertFalse("approximation should not be registered in non-snapshot", hasMetric(stats, SETTINGS_PREFIX + "approximation"));
        assertFalse("project_routing should not be registered in stateful", hasMetric(stats, SETTINGS_PREFIX + "project_routing"));
    }

    public void testSettingsMetricsInitialized_StatefulSnapshot() {
        Metrics metrics = createMetricsStatefulSnapshot();
        Counters stats = metrics.stats();

        // In stateful snapshot mode:
        // - time_zone, unmapped_fields, approximation should be registered
        // - project_routing should NOT be registered (serverless-only)
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "time_zone"));
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "unmapped_fields"));
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "approximation"));
        assertThat(stats.get(SETTINGS_PREFIX + "time_zone"), equalTo(0L));
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(0L));
        assertThat(stats.get(SETTINGS_PREFIX + "approximation"), equalTo(0L));

        assertFalse("project_routing should not be registered in stateful", hasMetric(stats, SETTINGS_PREFIX + "project_routing"));
    }

    public void testSettingsMetricsInitialized_ServerlessNonSnapshot() {
        Metrics metrics = createMetricsServerlessNonSnapshot();
        Counters stats = metrics.stats();

        // In serverless non-snapshot mode:
        // - time_zone, unmapped_fields, project_routing should be registered
        // - approximation should NOT be registered (snapshot-only)
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "time_zone"));
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "unmapped_fields"));
        assertTrue(hasMetric(stats, SETTINGS_PREFIX + "project_routing"));
        assertThat(stats.get(SETTINGS_PREFIX + "time_zone"), equalTo(0L));
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(0L));
        assertThat(stats.get(SETTINGS_PREFIX + "project_routing"), equalTo(0L));

        assertFalse("approximation should not be registered in non-snapshot", hasMetric(stats, SETTINGS_PREFIX + "approximation"));
    }

    public void testIncSettingByName() {
        Metrics metrics = createMetricsWithAllSettings();

        // Initial values should be 0
        Counters stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(0L));
        assertThat(stats.get(SETTINGS_PREFIX + "time_zone"), equalTo(0L));

        // Increment unmapped_fields
        metrics.incSetting("unmapped_fields");
        stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(1L));
        assertThat(stats.get(SETTINGS_PREFIX + "time_zone"), equalTo(0L));

        // Increment time_zone twice
        metrics.incSetting("time_zone");
        metrics.incSetting("time_zone");
        stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(1L));
        assertThat(stats.get(SETTINGS_PREFIX + "time_zone"), equalTo(2L));
    }

    public void testIncUnknownSettingIsIgnored() {
        Metrics metrics = createMetricsWithAllSettings();

        // Unknown setting should be silently ignored
        metrics.incSetting("unknown_setting");

        // Should not throw and other metrics should remain unchanged
        Counters stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(0L));
    }

    public void testIncAllKnownSettings() {
        Metrics metrics = createMetricsWithAllSettings();

        // Increment all known settings
        for (String settingName : QuerySettings.SETTINGS_BY_NAME.keySet()) {
            metrics.incSetting(settingName);
        }

        // Verify all settings are now 1
        Counters stats = metrics.stats();
        for (String settingName : QuerySettings.SETTINGS_BY_NAME.keySet()) {
            assertThat("Wrong count for setting: " + settingName, stats.get(SETTINGS_PREFIX + settingName), equalTo(1L));
        }
    }

    public void testIncApproximationSetting_SnapshotBuild() {
        Metrics metrics = createMetricsStatefulSnapshot();

        // Initial value should be 0
        Counters stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "approximation"), equalTo(0L));

        // Increment approximation
        metrics.incSetting("approximation");
        stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "approximation"), equalTo(1L));

        // Verify approximation is snapshot-only
        assertTrue("approximation should be a snapshot-only setting", QuerySettings.SETTINGS_BY_NAME.get("approximation").snapshotOnly());
    }

    public void testIncApproximationSetting_NonSnapshotBuild() {
        Metrics metrics = createMetricsStatefulNonSnapshot();
        Counters stats = metrics.stats();

        // approximation metric should not exist in non-snapshot builds
        assertFalse("approximation should not be registered", hasMetric(stats, SETTINGS_PREFIX + "approximation"));

        // Incrementing should be silently ignored (no counter registered)
        metrics.incSetting("approximation");
        stats = metrics.stats();
        assertFalse("approximation should still not be registered", hasMetric(stats, SETTINGS_PREFIX + "approximation"));
    }

    public void testIncProjectRoutingSetting_Serverless() {
        Metrics metrics = createMetricsServerlessNonSnapshot();

        // Initial value should be 0
        Counters stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "project_routing"), equalTo(0L));

        // Increment project_routing
        metrics.incSetting("project_routing");
        stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "project_routing"), equalTo(1L));

        // Verify project_routing is serverless-only
        assertTrue(
            "project_routing should be a serverless-only setting",
            QuerySettings.SETTINGS_BY_NAME.get("project_routing").serverlessOnly()
        );
    }

    public void testIncProjectRoutingSetting_Stateful() {
        Metrics metrics = createMetricsStatefulNonSnapshot();
        Counters stats = metrics.stats();

        // project_routing metric should not exist in stateful deployments
        assertFalse("project_routing should not be registered", hasMetric(stats, SETTINGS_PREFIX + "project_routing"));

        // Incrementing should be silently ignored (no counter registered)
        metrics.incSetting("project_routing");
        stats = metrics.stats();
        assertFalse("project_routing should still not be registered", hasMetric(stats, SETTINGS_PREFIX + "project_routing"));
    }
}
