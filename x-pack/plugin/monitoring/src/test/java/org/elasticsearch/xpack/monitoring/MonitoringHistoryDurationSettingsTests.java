/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.monitoring.MonitoringField;

public class MonitoringHistoryDurationSettingsTests extends ESTestCase {

    public void testHistoryDurationDefaults7Days() {
        TimeValue sevenDays = TimeValue.timeValueHours(7 * 24);

        // 7 days
        assertEquals(sevenDays, MonitoringField.HISTORY_DURATION.get(Settings.EMPTY));
        // Note: this verifies the semantics because this is taken for granted that it never returns null!
        assertEquals(sevenDays, MonitoringField.HISTORY_DURATION.get(buildSettings(MonitoringField.HISTORY_DURATION.getKey(), null)));
        assertWarnings(
            "[xpack.monitoring.history.duration] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the deprecation documentation for the next major version."
        );
    }

    public void testHistoryDurationMinimum24Hours() {
        // hit the minimum
        assertEquals(
            MonitoringField.HISTORY_DURATION_MINIMUM,
            MonitoringField.HISTORY_DURATION.get(buildSettings(MonitoringField.HISTORY_DURATION.getKey(), "24h"))
        );
        assertWarnings(
            "[xpack.monitoring.history.duration] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the deprecation documentation for the next major version."
        );
    }

    public void testHistoryDurationMinimum24HoursBlocksLower() {
        // 1 ms early!
        final String oneSecondEarly = (MonitoringField.HISTORY_DURATION_MINIMUM.millis() - 1) + "ms";
        expectThrows(
            IllegalArgumentException.class,
            () -> MonitoringField.HISTORY_DURATION.get(buildSettings(MonitoringField.HISTORY_DURATION.getKey(), oneSecondEarly))
        );
        assertWarnings(
            "[xpack.monitoring.history.duration] setting was deprecated in Elasticsearch and will be removed in a future release. "
                + "See the deprecation documentation for the next major version."
        );
    }

    private Settings buildSettings(String key, String value) {
        return Settings.builder().put(key, value).build();
    }
}
