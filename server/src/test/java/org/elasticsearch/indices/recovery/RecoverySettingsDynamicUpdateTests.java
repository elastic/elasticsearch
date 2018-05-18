/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.TimeUnit;

public class RecoverySettingsDynamicUpdateTests extends ESTestCase {
    private final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, clusterSettings);

    public void testZeroBytesPerSecondIsNoRateLimit() {
        clusterSettings.applySettings(Settings.builder().put(
                RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), 0).build());
        assertEquals(null, recoverySettings.rateLimiter());
    }

    public void testRetryDelayStateSync() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(Settings.builder().put(
                RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_STATE_SYNC_SETTING.getKey(), duration, timeUnit
        ).build());
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.retryDelayStateSync());
    }

    public void testRetryDelayNetwork() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(Settings.builder().put(
                RecoverySettings.INDICES_RECOVERY_RETRY_DELAY_NETWORK_SETTING.getKey(), duration, timeUnit
        ).build());
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.retryDelayNetwork());
    }

    public void testActivityTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(Settings.builder().put(
                RecoverySettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.getKey(), duration, timeUnit
        ).build());
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.activityTimeout());
    }

    public void testInternalActionTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(Settings.builder().put(
                RecoverySettings.INDICES_RECOVERY_INTERNAL_ACTION_TIMEOUT_SETTING.getKey(), duration, timeUnit
        ).build());
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.internalActionTimeout());
    }

    public void testInternalLongActionTimeout() {
        long duration = between(1, 1000);
        TimeUnit timeUnit = randomFrom(TimeUnit.MILLISECONDS, TimeUnit.SECONDS, TimeUnit.MINUTES, TimeUnit.HOURS);
        clusterSettings.applySettings(Settings.builder().put(
                RecoverySettings.INDICES_RECOVERY_INTERNAL_LONG_ACTION_TIMEOUT_SETTING.getKey(), duration, timeUnit
        ).build());
        assertEquals(new TimeValue(duration, timeUnit), recoverySettings.internalActionLongTimeout());
    }
}
