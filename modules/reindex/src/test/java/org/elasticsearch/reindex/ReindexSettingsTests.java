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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Unit tests for {@link ReindexSettings}: initialization from {@link ClusterSettings}, dynamic updates, and validation of
 * {@link ReindexSettings#REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING}.
 */
public class ReindexSettingsTests extends ESTestCase {

    /**
     * After construction with empty node settings, {@link ReindexSettings#getMemoryAccountingThresholdInBytes()} matches the
     * documented default of 1 MB.
     */
    public void testMemoryAccountingThresholdMatchesDefaultWhenNodeSettingsEmpty() {
        ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        ReindexSettings reindexSettings = new ReindexSettings(clusterSettings);
        assertThat(reindexSettings.getMemoryAccountingThresholdInBytes(), equalTo(ByteSizeValue.of(1, ByteSizeUnit.MB).getBytes()));
    }

    /**
     * The no-arg constructor (used by nodes that don't load ReindexPlugin and by some tests) falls back to the static default.
     */
    public void testMemoryAccountingThresholdUsesDefaultWhenSettingNotRegistered() {
        ReindexSettings reindexSettings = new ReindexSettings();
        assertThat(reindexSettings.getMemoryAccountingThresholdInBytes(), equalTo(ByteSizeValue.of(1, ByteSizeUnit.MB).getBytes()));
    }

    /**
     * A dynamic cluster settings update propagates the new threshold into the cached value.
     */
    public void testMemoryAccountingThresholdUpdatesWhenClusterSettingApplied() {
        ClusterSettings clusterSettings = clusterSettings(Settings.EMPTY);
        ReindexSettings reindexSettings = new ReindexSettings(clusterSettings);

        ByteSizeValue updated = ByteSizeValue.of(randomIntBetween(2, 512), ByteSizeUnit.MB);
        clusterSettings.applySettings(
            Settings.builder().put(ReindexSettings.REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING.getKey(), updated.getStringRep()).build()
        );

        assertThat(reindexSettings.getMemoryAccountingThresholdInBytes(), equalTo(updated.getBytes()));
    }

    /**
     * Values strictly below the 1 MB minimum are rejected so the cached value is unchanged.
     */
    public void testMemoryAccountingThresholdRejectsBelowMinimum() {
        String key = ReindexSettings.REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING.getKey();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ReindexSettings.REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING.get(Settings.builder().put(key, "512kb").build())
        );
        assertThat(e.getMessage(), containsString(key));
    }

    private static ClusterSettings clusterSettings(Settings nodeSettings) {
        return new ClusterSettings(nodeSettings, Set.of(ReindexSettings.REINDEX_MEMORY_ACCOUNTING_THRESHOLD_SETTING));
    }
}
