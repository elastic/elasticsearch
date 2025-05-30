/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.stats;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.search.stats.SearchStatsSettings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link SearchStatsSettings}.
 */
public class SearchStatsSettingsTests extends ESTestCase {

    /**
     * Test the default value of the recent read load half-life setting.
     */
    public void testRecentReadLoadHalfLife_defaultValue() {
        SearchStatsSettings settings = new SearchStatsSettings(ClusterSettings.createBuiltInClusterSettings());
        assertThat(settings.getRecentReadLoadHalfLifeForNewShards(), equalTo(SearchStatsSettings.RECENT_READ_LOAD_HALF_LIFE_DEFAULT));
    }

    /**
     * Test the initial value of the recent read load half-life setting.
     */
    public void testRecentReadLoadHalfLife_initialValue() {
        SearchStatsSettings settings = new SearchStatsSettings(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder().put(SearchStatsSettings.RECENT_READ_LOAD_HALF_LIFE_SETTING.getKey(), "2h").build()
            )
        );
        assertThat(settings.getRecentReadLoadHalfLifeForNewShards(), equalTo(TimeValue.timeValueHours(2)));
    }

    /**
     * Test updating the recent read load half-life setting.
     */
    public void testRecentReadLoadHalfLife_updateValue() {
        ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        SearchStatsSettings settings = new SearchStatsSettings(clusterSettings);
        clusterSettings.applySettings(
            Settings.builder().put(SearchStatsSettings.RECENT_READ_LOAD_HALF_LIFE_SETTING.getKey(), "90m").build()
        );
        assertThat(settings.getRecentReadLoadHalfLifeForNewShards(), equalTo(TimeValue.timeValueMinutes(90)));
    }
}
