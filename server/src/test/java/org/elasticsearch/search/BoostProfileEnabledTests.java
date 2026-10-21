/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.search.BoostProfileEnabled.BOOST_PROFILE_ENABLED_SETTING;

public class BoostProfileEnabledTests extends ESTestCase {

    public void testDisabledByDefault() {
        assertFalse(new BoostProfileEnabled(clusterSettings(Settings.EMPTY)).isEnabled());
    }

    public void testEnabledWhenTheSettingIsSet() {
        assumeTrue("Enabling the boost profile requires the boost_profile feature flag", BoostProfileEnabled.FEATURE_FLAG.isEnabled());

        final var settings = Settings.builder().put(BOOST_PROFILE_ENABLED_SETTING.getKey(), true).build();
        assertTrue(new BoostProfileEnabled(clusterSettings(settings)).isEnabled());
    }

    public void testTracksDynamicUpdates() {
        assumeTrue("Enabling the boost profile requires the boost_profile feature flag", BoostProfileEnabled.FEATURE_FLAG.isEnabled());

        final var clusterSettings = clusterSettings(Settings.EMPTY);
        final var boostProfileEnabled = new BoostProfileEnabled(clusterSettings);
        assertFalse(boostProfileEnabled.isEnabled());

        clusterSettings.applySettings(Settings.builder().put(BOOST_PROFILE_ENABLED_SETTING.getKey(), true).build());
        assertTrue(boostProfileEnabled.isEnabled());

        clusterSettings.applySettings(Settings.EMPTY);
        assertFalse(boostProfileEnabled.isEnabled());
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }
}
