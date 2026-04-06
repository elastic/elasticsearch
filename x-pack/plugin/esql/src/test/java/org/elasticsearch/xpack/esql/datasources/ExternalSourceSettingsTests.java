/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

public class ExternalSourceSettingsTests extends ESTestCase {

    public void testDefaults() {
        Settings settings = Settings.EMPTY;
        assertEquals(50, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
        assertEquals(30, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
    }

    public void testCustomValues() {
        Settings settings = Settings.builder()
            .put("esql.external.max_concurrent_requests", 100)
            .put("esql.external.throttle_max_retry_duration", 60)
            .build();

        assertEquals(100, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
        assertEquals(60, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
    }

    public void testZeroConcurrencyDisablesLimiting() {
        Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 0).build();
        assertEquals(0, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
    }

    public void testConcurrencyUpperBound() {
        expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put("esql.external.max_concurrent_requests", 501).build();
            ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings);
        });
    }

    public void testThrottleMaxRetryDurationZeroDisablesBudget() {
        Settings settings = Settings.builder().put("esql.external.throttle_max_retry_duration", 0).build();
        assertEquals(0, (int) ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings));
    }

    public void testThrottleMaxRetryDurationUpperBound() {
        expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put("esql.external.throttle_max_retry_duration", 301).build();
            ExternalSourceSettings.THROTTLE_MAX_RETRY_DURATION.get(settings);
        });
    }

    public void testSettingsListNotEmpty() {
        assertFalse(ExternalSourceSettings.settings().isEmpty());
        assertEquals(4, ExternalSourceSettings.settings().size());
    }
}
