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

import static org.elasticsearch.xpack.esql.telemetry.Metrics.SETTINGS_PREFIX;
import static org.hamcrest.Matchers.equalTo;

public class SettingsMetricsTests extends ESTestCase {

    public void testSettingsMetricsInitialized() {
        Metrics metrics = new Metrics(new EsqlFunctionRegistry());
        Counters stats = metrics.stats();

        // Verify all settings from QuerySettings have corresponding metrics initialized to 0
        for (String settingName : QuerySettings.SETTINGS_BY_NAME.keySet()) {
            assertThat("Missing metric for setting: " + settingName, stats.get(SETTINGS_PREFIX + settingName), equalTo(0L));
        }
    }

    public void testIncSettingByName() {
        Metrics metrics = new Metrics(new EsqlFunctionRegistry());

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
        Metrics metrics = new Metrics(new EsqlFunctionRegistry());

        // Unknown setting should be silently ignored
        metrics.incSetting("unknown_setting");

        // Should not throw and other metrics should remain unchanged
        Counters stats = metrics.stats();
        assertThat(stats.get(SETTINGS_PREFIX + "unmapped_fields"), equalTo(0L));
    }

    public void testIncAllKnownSettings() {
        Metrics metrics = new Metrics(new EsqlFunctionRegistry());

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
}
