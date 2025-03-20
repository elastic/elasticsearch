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

import java.math.BigDecimal;
import java.math.RoundingMode;

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

    public void testRecentWriteLoadHalfLife_minValue() {
        IndexingStatsSettings settings = new IndexingStatsSettings(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(
                        IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_SETTING.getKey(),
                        IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_MIN.toString()
                    )
                    .build()
            )
        );
        assertThat(settings.getRecentWriteLoadHalfLifeForNewShards(), equalTo(IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_MIN));
    }

    public void testRecentWriteLoadHalfLife_valueTooSmall() {
        long tooFewMillis = IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_MIN.getMillis() / 2;
        assertThrows(
            IllegalArgumentException.class,
            () -> new IndexingStatsSettings(
                ClusterSettings.createBuiltInClusterSettings(
                    Settings.builder().put(IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_SETTING.getKey(), tooFewMillis + "ms").build()
                )
            )
        );
    }

    public void testRecentWriteLoadHalfLife_maxValue() {
        IndexingStatsSettings settings = new IndexingStatsSettings(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(
                        IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_SETTING.getKey(),
                        IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_MAX.toString()
                    )
                    .build()
            )
        );
        assertThat(settings.getRecentWriteLoadHalfLifeForNewShards(), equalTo(IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_MAX));
    }

    public void testRecentWriteLoadHalfLife_valueTooLarge() {
        int tooManyDays = BigDecimal.valueOf(Long.MAX_VALUE)
            .add(BigDecimal.ONE)
            .divide(BigDecimal.valueOf(24 * 60 * 60 * 1_000_000_000L), RoundingMode.UP)
            .setScale(0, RoundingMode.UP)
            .intValueExact();
        assertThrows(
            IllegalArgumentException.class,
            () -> new IndexingStatsSettings(
                ClusterSettings.createBuiltInClusterSettings(
                    Settings.builder().put(IndexingStatsSettings.RECENT_WRITE_LOAD_HALF_LIFE_SETTING.getKey(), tooManyDays + "d").build()
                )
            )
        );
    }
}
