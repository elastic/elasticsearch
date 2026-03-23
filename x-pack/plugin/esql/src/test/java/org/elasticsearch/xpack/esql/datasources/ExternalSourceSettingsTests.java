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
        assertEquals(10, (int) ExternalSourceSettings.THROTTLE_RETRY_LIMIT.get(settings));
        assertTrue(ExternalSourceSettings.ADAPTIVE_BACKOFF_ENABLED.get(settings));
    }

    public void testCustomValues() {
        Settings settings = Settings.builder()
            .put("esql.external.max_concurrent_requests", 100)
            .put("esql.external.throttle_retry_limit", 20)
            .put("esql.external.adaptive_backoff_enabled", false)
            .build();

        assertEquals(100, (int) ExternalSourceSettings.MAX_CONCURRENT_REQUESTS.get(settings));
        assertEquals(20, (int) ExternalSourceSettings.THROTTLE_RETRY_LIMIT.get(settings));
        assertFalse(ExternalSourceSettings.ADAPTIVE_BACKOFF_ENABLED.get(settings));
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

    public void testThrottleRetryLowerBound() {
        expectThrows(IllegalArgumentException.class, () -> {
            Settings settings = Settings.builder().put("esql.external.throttle_retry_limit", 0).build();
            ExternalSourceSettings.THROTTLE_RETRY_LIMIT.get(settings);
        });
    }

    public void testSettingsListNotEmpty() {
        assertFalse(ExternalSourceSettings.settings().isEmpty());
        assertEquals(3, ExternalSourceSettings.settings().size());
    }
}
