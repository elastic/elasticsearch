/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class IndexingStatsSettingsTests extends ESTestCase {

    public void testRecentWriteLoadHalfLife_defaultValue() {
        IndexingStatsSettings settings = new IndexingStatsSettings(ClusterSettings.createBuiltInClusterSettings());
        assertThat(settings.getRecentWriteLoadHalfLifeForNewShards(), equalTo(IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_DEFAULT));
    }

    public void testRecentWriteLoadHalfLife_initialValue() {
        IndexingStatsSettings settings = new IndexingStatsSettings(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder().put(IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_SETTING.getKey(), "2h").build()
            )
        );
        assertThat(settings.getRecentWriteLoadHalfLifeForNewShards(), equalTo(TimeValue.timeValueHours(2)));
    }

    public void testRecentWriteLoadHalfLife_updateValue() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        IndexingStatsSettings settings = new IndexingStatsSettings(clusterSettings);
        clusterSettings.applySettings(
            Settings.builder().put(IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_SETTING.getKey(), "90m").build()
        );
        assertThat(settings.getRecentWriteLoadHalfLifeForNewShards(), equalTo(TimeValue.timeValueMinutes(90)));
    }
}
