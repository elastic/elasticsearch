/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link ReindexSettings}: initialization from {@link ClusterSettings}, dynamic updates, and validation of
 * {@link ReindexSettings#REINDEX_PIT_KEEP_ALIVE_SETTING}.
 */
public class ReindexSettingsTests extends ESTestCase {

    /**
     * After construction with empty node settings, {@link ReindexSettings#pitKeepAlive()} matches the setting default
     */
    public void testPitKeepAliveMatchesDefaultWhenNodeSettingsEmpty() {
        ClusterSettings clusterSettings = clusterSettings(Set.of(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING), Settings.EMPTY);
        ReindexSettings reindexSettings = new ReindexSettings(clusterSettings);
        assertThat(reindexSettings.pitKeepAlive(), equalTo(TimeValue.timeValueMinutes(15)));
    }

    /**
     * When the PIT keep-alive setting is not registered on {@link ClusterSettings} (nodes without {@code ReindexPlugin}),
     * {@link ReindexSettings} still constructs and exposes the static default
     */
    public void testPitKeepAliveUsesDefaultWhenSettingNotRegistered() {
        ReindexSettings reindexSettings = new ReindexSettings();
        assertThat(reindexSettings.pitKeepAlive(), equalTo(TimeValue.timeValueMinutes(15)));
    }

    /**
     * When the PIT keep-alive is present in node settings at {@link ClusterSettings} construction time, the watcher initializes
     * {@link ReindexSettings#pitKeepAlive()} to that value instead of the built-in default.
     */
    public void testPitKeepAliveUsesNodeSettingAtConstruction() {
        TimeValue configured = TimeValue.timeValueMinutes(randomIntBetween(10, 30));
        Settings nodeSettings = Settings.builder()
            .put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), configured.getStringRep())
            .build();
        ClusterSettings clusterSettings = clusterSettings(Set.of(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING), nodeSettings);
        ReindexSettings reindexSettings = new ReindexSettings(clusterSettings);
        assertThat(reindexSettings.pitKeepAlive(), equalTo(configured));
    }

    /**
     * A dynamic cluster settings update via {@link ClusterSettings#applySettings} changes {@link ReindexSettings#pitKeepAlive()}
     * to the newly applied value.
     */
    public void testPitKeepAliveUpdatesWhenClusterSettingApplied() {
        ClusterSettings clusterSettings = clusterSettings(Set.of(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING), Settings.EMPTY);
        ReindexSettings reindexSettings = new ReindexSettings(clusterSettings);
        assertThat(reindexSettings.pitKeepAlive(), equalTo(TimeValue.timeValueMinutes(15)));

        TimeValue updated = TimeValue.timeValueMinutes(randomIntBetween(11, 45));
        clusterSettings.applySettings(
            Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), updated.getStringRep()).build()
        );

        assertThat(reindexSettings.pitKeepAlive(), equalTo(updated));
    }

    /**
     * Successive valid {@link ClusterSettings#applySettings} calls each replace the cached PIT keep-alive with the latest value.
     */
    public void testPitKeepAliveTracksLatestAppliedValue() {
        ClusterSettings clusterSettings = clusterSettings(Set.of(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING), Settings.EMPTY);
        ReindexSettings reindexSettings = new ReindexSettings(clusterSettings);

        TimeValue first = TimeValue.timeValueMinutes(12);
        clusterSettings.applySettings(
            Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), first.getStringRep()).build()
        );
        assertThat(reindexSettings.pitKeepAlive(), equalTo(first));

        TimeValue second = TimeValue.timeValueMinutes(42);
        clusterSettings.applySettings(
            Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), second.getStringRep()).build()
        );
        assertThat(reindexSettings.pitKeepAlive(), equalTo(second));
    }

    /**
     * If {@link ClusterSettings#applySettings} rejects an out-of-range value, {@link ReindexSettings#pitKeepAlive()} stays at the
     * last successfully applied value.
     */
    public void testInvalidClusterSettingUpdateDoesNotChangeCachedPitKeepAlive() {
        ClusterSettings clusterSettings = clusterSettings(Set.of(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING), Settings.EMPTY);
        ReindexSettings reindexSettings = new ReindexSettings(clusterSettings);

        TimeValue valid = TimeValue.timeValueMinutes(15);
        clusterSettings.applySettings(
            Settings.builder().put(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(), valid.getStringRep()).build()
        );
        assertThat(reindexSettings.pitKeepAlive(), equalTo(valid));

        String key = ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey();
        Settings invalid = Settings.builder().put(key, "0ms").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> clusterSettings.applySettings(invalid));
        assertThat(e.getMessage(), containsString(key));
        assertThat(reindexSettings.pitKeepAlive(), equalTo(valid));
    }

    /**
     * {@link ReindexSettings#REINDEX_PIT_KEEP_ALIVE_SETTING} parses the documented default and accepts the minimum ({@code 1ms})
     */
    public void testReindexPitKeepAliveSettingDefinition() {
        assertThat(ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.get(Settings.EMPTY), equalTo(TimeValue.timeValueMinutes(15)));
        String key = ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey();
        assertThat(
            ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.get(Settings.builder().put(key, "1ms").build()),
            equalTo(TimeValue.timeValueMillis(1))
        );
    }

    /**
     * Values strictly below the minimum allowed keep-alive are rejected with a stable error message.
     */
    public void testReindexPitKeepAliveSettingRejectsBelowMinimum() {
        String key = ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.get(Settings.builder().put(key, "0ms").build())
        );
        assertThat(e.getMessage(), equalTo("failed to parse value [0ms] for setting [" + key + "], must be >= [1ms]"));
    }

    /**
     * The setting has no defined upper bound so accept anything
     */
    public void testReindexPitKeepAliveSettingHasNoUpperBound() {
        String key = ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey();
        long value = randomLongBetween(1, Long.MAX_VALUE);
        assertThat(
            ReindexSettings.REINDEX_PIT_KEEP_ALIVE_SETTING.get(Settings.builder().put(key, value + "ms").build()),
            equalTo(TimeValue.timeValueMillis(value))
        );
    }

    private static ClusterSettings clusterSettings(Set<Setting<?>> settingsSet, Settings nodeSettings) {
        return new ClusterSettings(nodeSettings, settingsSet);
    }
}
