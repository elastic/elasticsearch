/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.cluster.routing.allocation.SearchLoadConstraintSettings.SEARCH_LOAD_CPU_THRESHOLD_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class SearchLoadConstraintSettingsTests extends ESTestCase {

    public void testCpuThresholdKeyAndDefault() {
        assertThat(SEARCH_LOAD_CPU_THRESHOLD_SETTING.getKey(), equalTo("cluster.routing.allocation.search_load.cpu_threshold"));
        assertThat(SEARCH_LOAD_CPU_THRESHOLD_SETTING.get(Settings.EMPTY), equalTo(0.9));
    }

    public void testCpuThresholdAcceptsValuesAboveZeroUpToOne() {
        assertThat(cpuThreshold("0.75"), equalTo(0.75));
        assertThat(cpuThreshold("1"), equalTo(1.0));
        assertThat(cpuThreshold("0.0001"), equalTo(0.0001));
    }

    public void testCpuThresholdMustBeAboveZeroAndAtMostOne() {
        for (String invalid : new String[] { "0", "0.0", "-0.0", "-0.1", "1.01", "75%", "not-a-number" }) {
            expectThrows(IllegalArgumentException.class, () -> cpuThreshold(invalid));
        }
    }

    public void testCpuThresholdIsABuiltInClusterSetting() {
        assertThat(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, hasItem(SEARCH_LOAD_CPU_THRESHOLD_SETTING));
    }

    public void testCpuThresholdIsOperatorOnly() {
        assertTrue(SEARCH_LOAD_CPU_THRESHOLD_SETTING.isDynamic());
        assertTrue(SEARCH_LOAD_CPU_THRESHOLD_SETTING.isOperatorOnly());
    }

    public void testCpuThresholdFollowsClusterSettingUpdates() {
        final ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();
        final SearchLoadConstraintSettings settings = new SearchLoadConstraintSettings(clusterSettings);
        assertThat(settings.getCpuThreshold(), equalTo(0.9));

        clusterSettings.applySettings(Settings.builder().put(SEARCH_LOAD_CPU_THRESHOLD_SETTING.getKey(), 0.8).build());
        assertThat(settings.getCpuThreshold(), equalTo(0.8));

        expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(SEARCH_LOAD_CPU_THRESHOLD_SETTING.getKey(), 0.0).build())
        );
        assertThat(settings.getCpuThreshold(), equalTo(0.8));
    }

    private static double cpuThreshold(String value) {
        return SEARCH_LOAD_CPU_THRESHOLD_SETTING.get(Settings.builder().put(SEARCH_LOAD_CPU_THRESHOLD_SETTING.getKey(), value).build());
    }
}
