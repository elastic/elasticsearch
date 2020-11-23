/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class CcrSettingsTests extends ESTestCase {

    public void testDefaultSettings() {
        final Settings settings = Settings.EMPTY;
        final CcrSettings ccrSettings = new CcrSettings(settings,
            new ClusterSettings(settings, Sets.newHashSet(CcrSettings.RECOVERY_CHUNK_SIZE,
                CcrSettings.RECOVERY_MAX_BYTES_PER_SECOND, CcrSettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
                CcrSettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING,
                CcrSettings.SHARD_SIZE_FETCHING_TIMEOUT_SETTING)));
        assertEquals(CcrSettings.RECOVERY_CHUNK_SIZE.get(settings), ccrSettings.getChunkSize());
        assertEquals(CcrSettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings).intValue(),
            ccrSettings.getMaxConcurrentFileChunks());
        assertEquals(CcrSettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings),
            ccrSettings.getRecoveryActivityTimeout());
        assertEquals(CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.get(settings),
            ccrSettings.getRecoveryActionTimeout());
        assertEquals(CcrSettings.SHARD_SIZE_FETCHING_TIMEOUT_SETTING.get(settings),
            ccrSettings.getShardSizeFetchingTimeout());
        assertEquals(CcrSettings.RECOVERY_MAX_BYTES_PER_SECOND.get(settings).getMbFrac(),
            ccrSettings.getRateLimiter().getMBPerSec(), 0.0d);
    }

    public void testShardSizeFetchingTimeoutSetting() {
        final Settings.Builder builder = Settings.builder();
        final String recoveryActionTimeout = randomPositiveTimeValue();
        builder.put(CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.getKey(), recoveryActionTimeout);
        final String shardSizeFetchingTimeout;
        if (randomBoolean()) {
            shardSizeFetchingTimeout = randomPositiveTimeValue();
            builder.put(CcrSettings.SHARD_SIZE_FETCHING_TIMEOUT_SETTING.getKey(), shardSizeFetchingTimeout);
        } else {
            shardSizeFetchingTimeout = recoveryActionTimeout;
            if (randomBoolean()) {
                builder.put(CcrSettings.SHARD_SIZE_FETCHING_TIMEOUT_SETTING.getKey(), shardSizeFetchingTimeout);
            }
        }
        final Settings settings = builder.build();
        final CcrSettings ccrSettings = new CcrSettings(settings,
            new ClusterSettings(settings, Sets.newHashSet(CcrSettings.RECOVERY_CHUNK_SIZE,
                CcrSettings.RECOVERY_MAX_BYTES_PER_SECOND, CcrSettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
                CcrSettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING,
                CcrSettings.SHARD_SIZE_FETCHING_TIMEOUT_SETTING)));
        assertThat(CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.get(settings), equalTo(ccrSettings.getRecoveryActionTimeout()));
        assertThat(ccrSettings.getRecoveryActionTimeout(), equalTo(TimeValue.parseTimeValue(recoveryActionTimeout, getTestName())));
        assertThat(CcrSettings.SHARD_SIZE_FETCHING_TIMEOUT_SETTING.get(settings), equalTo(ccrSettings.getShardSizeFetchingTimeout()));
        assertThat(ccrSettings.getShardSizeFetchingTimeout(), equalTo(TimeValue.parseTimeValue(shardSizeFetchingTimeout, getTestName())));
    }
}
