/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.monitor.metrics.NodeMetrics.NODE_METRICS_CACHE_TTL_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class NodeMetricsCacheTtlSettingTests extends ESTestCase {

    public void testDefaultIsNeverBoundedByCollectionInterval() {
        // The default must never fail startup, even when the collection cadence is far below it.
        Settings settings = Settings.builder().put("telemetry.agent.metrics_interval", "1s").build();
        assertThat(NODE_METRICS_CACHE_TTL_SETTING.get(settings), equalTo(TimeValue.timeValueSeconds(5)));
    }

    public void testExplicitValueBoundedByCollectionInterval() {
        Settings within = Settings.builder()
            .put("telemetry.agent.metrics_interval", "60s")
            .put(NODE_METRICS_CACHE_TTL_SETTING.getKey(), "30s")
            .build();
        assertThat(NODE_METRICS_CACHE_TTL_SETTING.get(within), equalTo(TimeValue.timeValueSeconds(30)));

        Settings exceeding = Settings.builder()
            .put("telemetry.agent.metrics_interval", "60s")
            .put(NODE_METRICS_CACHE_TTL_SETTING.getKey(), "120s")
            .build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NODE_METRICS_CACHE_TTL_SETTING.get(exceeding));
        assertThat(e.getMessage(), containsString("must not exceed the OTel SDK collection interval"));
    }

    public void testBoundTracksExportIntervalOverLegacyKey() {
        // telemetry.export.interval, when set, is the live collection cadence regardless of the legacy metrics_interval.
        Settings settings = Settings.builder()
            .put("telemetry.agent.metrics_interval", "60s")
            .put("telemetry.export.interval", "10s")
            .put(NODE_METRICS_CACHE_TTL_SETTING.getKey(), "30s")
            .build();
        expectThrows(IllegalArgumentException.class, () -> NODE_METRICS_CACHE_TTL_SETTING.get(settings));
    }
}
