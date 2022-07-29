/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

public class RecoverySettingsDynamicUpdateTests extends ESTestCase {
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, clusterSettings);

    public void testZeroBytesPerSecondIsNoRateLimit() {
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), 0).build()
        );
        assertEquals(null, recoverySettings.rateLimiter());
    }

    public void testRetryDelayStateSync() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.retryDelayStateSync());
    }

    public void testRetryDelayNetwork() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.retryDelayNetwork());
    }

    public void testActivityTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.activityTimeout());
    }

    public void testInternalActionTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder().put(RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), duration, timeUnit).build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.internalActionTimeout());
    }

    public void testInternalLongActionTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(
            Settings.builder()
                .put(RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.getKey(), duration, timeUnit)
                .build()
        );
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.internalActionLongTimeout());
    }
}
