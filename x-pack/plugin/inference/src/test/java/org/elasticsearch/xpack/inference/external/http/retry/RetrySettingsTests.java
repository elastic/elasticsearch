/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.retry;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.xpack.inference.Utils.mockClusterServiceEmpty;

public class RetrySettingsTests extends ESTestCase {

    /**
     * Creates a {@link RetrySettings} object with initial delay of 1 millisecond, max delay bound of 1 millisecond,
     * and timeout of 30 seconds
     */
    public static RetrySettings createDefaultRetrySettings() {
        return createRetrySettings(TimeValue.timeValueMillis(1), TimeValue.timeValueMillis(1), TimeValue.timeValueSeconds(30));
    }

    public static RetrySettings createRetrySettings(TimeValue initialDelay, TimeValue maxDelayBound, TimeValue timeout) {
        var settings = buildSettingsWithRetryFields(initialDelay, maxDelayBound, timeout);

        return new RetrySettings(settings, mockClusterServiceEmpty());
    }

    public static Settings buildSettingsWithRetryFields(TimeValue initialDelay, TimeValue maxDelayBound, TimeValue timeout) {
        return Settings.builder()
            .put(RetrySettings.RETRY_INITIAL_DELAY_SETTING.getKey(), initialDelay)
            .put(RetrySettings.RETRY_MAX_DELAY_BOUND_SETTING.getKey(), maxDelayBound)
            .put(RetrySettings.RETRY_TIMEOUT_SETTING.getKey(), timeout)
            .build();
    }
}
