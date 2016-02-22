/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import org.junit.Rule;
import org.junit.rules.ExpectedException;

/**
 * Tests {@link MarvelSettings}
 */
public class MarvelSettingsTests extends ESTestCase {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    public void testHistoryDurationDefaults7Days() {
        TimeValue sevenDays = TimeValue.timeValueHours(7 * 24);

        // 7 days
        assertEquals(sevenDays, MarvelSettings.HISTORY_DURATION.get(Settings.EMPTY));
        // Note: this verifies the semantics because this is taken for granted that it never returns null!
        assertEquals(sevenDays, MarvelSettings.HISTORY_DURATION.get(buildSettings(MarvelSettings.HISTORY_DURATION.getKey(), null)));
    }

    public void testHistoryDurationMinimum24Hours() {
        // hit the minimum
        assertEquals(MarvelSettings.HISTORY_DURATION_MINIMUM,
                     MarvelSettings.HISTORY_DURATION.get(buildSettings(MarvelSettings.HISTORY_DURATION.getKey(), "24h")));
    }

    public void testHistoryDurationMinimum24HoursBlocksLower() {
        expectedException.expect(IllegalArgumentException.class);

        // 1 ms early!
        String oneSecondEarly = (MarvelSettings.HISTORY_DURATION_MINIMUM.millis() - 1) + "ms";

        MarvelSettings.HISTORY_DURATION.get(buildSettings(MarvelSettings.HISTORY_DURATION.getKey(), oneSecondEarly));
    }

    private Settings buildSettings(String key, String value) {
        return Settings.builder().put(key, value).build();
    }
}
