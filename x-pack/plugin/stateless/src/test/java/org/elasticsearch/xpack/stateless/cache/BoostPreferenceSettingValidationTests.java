/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.elasticsearch.xpack.stateless.cache.SearchCommitPrefetcherDynamicSettings.STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT;
import static org.elasticsearch.xpack.stateless.cache.StatelessSharedBlobCacheService.STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class BoostPreferenceSettingValidationTests extends ESTestCase {

    public void testStartupBoostEnabledWithReplicatedDisabledIsInvalid() {
        final Settings settings = Settings.builder()
            .put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), false)
            .build();
        final var e = expectThrows(IllegalArgumentException.class, () -> STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings));
        assertThat(e.getMessage(), containsString("[stateless.cache_boost_preference.enabled] cannot be [true] unless"));
    }

    public void testStartupBoostEnabledWithReplicatedEnabledIsValid() {
        final Settings settings = Settings.builder()
            .put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
            .put(STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
            .build();
        assertTrue(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings));
    }

    public void testStartupBoostDisabledIsAlwaysValid() {
        // boost off, replicated can be anything
        final Settings settings = Settings.builder()
            .put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), false)
            .put(STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), randomBoolean())
            .build();
        assertFalse(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.get(settings));
    }

    public void testDynamicReplicatedDisabledWhileBoostEnabledIsInvalid() {
        final var clusterSettings = createClusterSettings(
            Settings.builder()
                .put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
                .put(STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), true)
                .build()
        );
        initializeAndWatch(clusterSettings);

        final var e = expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(
                Settings.builder()
                    .put(STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING.getKey(), true)
                    .put(STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), false)
                    .build()
            )
        );
        assertThat(e.getMessage(), containsString("illegal value can't update [stateless.search.use_internal_files_replicated_content]"));
        assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            e.getCause().getMessage(),
            containsString("[stateless.search.use_internal_files_replicated_content] cannot be [false] while setting")
        );
    }

    public void testDynamicReplicatedDisabledWhileBoostDisabledIsValid() {
        final var clusterSettings = createClusterSettings(Settings.EMPTY);
        initializeAndWatch(clusterSettings);

        clusterSettings.applySettings(
            Settings.builder().put(STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT.getKey(), false).build()
        );
    }

    private static void initializeAndWatch(ClusterSettings clusterSettings) {
        clusterSettings.initializeAndWatch(STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT, value -> {});
    }

    private static ClusterSettings createClusterSettings(Settings settings) {
        final Set<Setting<?>> settingsSet = Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            STATELESS_CACHE_BOOST_PREFERENCE_ENABLED_SETTING,
            STATELESS_SEARCH_USE_INTERNAL_FILES_REPLICATED_CONTENT
        );
        return new ClusterSettings(settings, settingsSet);
    }
}
