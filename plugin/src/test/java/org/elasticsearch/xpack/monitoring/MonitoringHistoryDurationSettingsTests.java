/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

public class MonitoringHistoryDurationSettingsTests extends ESTestCase {

    public void testHistoryDurationDefaults7Days() {
        TimeValue sevenDays = TimeValue.timeValueHours(7 * 24);

        // 7 days
        assertEquals(sevenDays, Monitoring.HISTORY_DURATION.get(Settings.EMPTY));
        // Note: this verifies the semantics because this is taken for granted that it never returns null!
        assertEquals(sevenDays, Monitoring.HISTORY_DURATION.get(buildSettings(Monitoring.HISTORY_DURATION.getKey(), null)));
    }

    public void testHistoryDurationMinimum24Hours() {
        // hit the minimum
        assertEquals(Monitoring.HISTORY_DURATION_MINIMUM,
                Monitoring.HISTORY_DURATION.get(buildSettings(Monitoring.HISTORY_DURATION.getKey(), "24h")));
    }

    public void testHistoryDurationMinimum24HoursBlocksLower() {
        // 1 ms early!
        final String oneSecondEarly = (Monitoring.HISTORY_DURATION_MINIMUM.millis() - 1) + "ms";
        expectThrows(IllegalArgumentException.class,
                () -> Monitoring.HISTORY_DURATION.get(buildSettings(Monitoring.HISTORY_DURATION.getKey(), oneSecondEarly)));
    }

    private Settings buildSettings(String key, String value) {
        return Settings.builder().put(key, value).build();
    }
}
