/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ccr;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

public class CcrSettingsTests extends ESTestCase {

    public void testDefaultSettings() {
        final Settings settings = Settings.EMPTY;
        final CcrSettings ccrSettings = new CcrSettings(settings,
            new ClusterSettings(settings, Sets.newHashSet(CcrSettings.RECOVERY_CHUNK_SIZE,
                CcrSettings.RECOVERY_MAX_BYTES_PER_SECOND, CcrSettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING,
                CcrSettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING, CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING)));
        assertEquals(CcrSettings.RECOVERY_CHUNK_SIZE.get(settings), ccrSettings.getChunkSize());
        assertEquals(CcrSettings.INDICES_RECOVERY_MAX_CONCURRENT_FILE_CHUNKS_SETTING.get(settings).intValue(),
            ccrSettings.getMaxConcurrentFileChunks());
        assertEquals(CcrSettings.INDICES_RECOVERY_ACTIVITY_TIMEOUT_SETTING.get(settings),
            ccrSettings.getRecoveryActivityTimeout());
        assertEquals(CcrSettings.INDICES_RECOVERY_ACTION_TIMEOUT_SETTING.get(settings),
            ccrSettings.getRecoveryActionTimeout());
        assertEquals(CcrSettings.RECOVERY_MAX_BYTES_PER_SECOND.get(settings).getMbFrac(),
            ccrSettings.getRateLimiter().getMBPerSec(), 0.0d);
    }
}
