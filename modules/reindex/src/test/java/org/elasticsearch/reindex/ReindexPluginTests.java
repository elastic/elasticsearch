/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class ReindexPluginTests extends ESTestCase {

    public void testReindexPitKeepAliveSettingRegistered() throws IOException {
        ReindexPlugin plugin = new ReindexPlugin();
        try {
            List<Setting<?>> settings = plugin.getSettings();
            assertTrue(
                "getSettings() must include " + ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey(),
                settings.stream().anyMatch(s -> s.getKey().equals(ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING.getKey()))
            );
        } finally {
            plugin.close();
        }
    }

    public void testReindexPitKeepAliveDefault() {
        assertThat(ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING.get(Settings.EMPTY), equalTo(TimeValue.timeValueMinutes(5)));
    }

    public void testReindexPitKeepAliveAcceptsMinAndMax() {
        Setting<TimeValue> setting = ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING;
        String key = setting.getKey();
        assertThat(setting.get(Settings.builder().put(key, "10s").build()), equalTo(TimeValue.timeValueSeconds(10)));
        assertThat(setting.get(Settings.builder().put(key, "1h").build()), equalTo(TimeValue.timeValueHours(1)));
    }

    public void testReindexPitKeepAliveRejectsBelowMinimum() {
        Setting<TimeValue> setting = ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING;
        String key = setting.getKey();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> setting.get(Settings.builder().put(key, "9999ms").build())
        );
        assertThat(e.getMessage(), equalTo("failed to parse value [9999ms] for setting [" + key + "], must be >= [10s]"));
    }

    public void testReindexPitKeepAliveRejectsAboveMaximum() {
        Setting<TimeValue> setting = ReindexPlugin.REINDEX_PIT_KEEP_ALIVE_SETTING;
        String key = setting.getKey();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> setting.get(Settings.builder().put(key, "3600001ms").build())
        );
        assertThat(e.getMessage(), equalTo("failed to parse value [3600001ms] for setting [" + key + "], must be <= [1h]"));
    }
}
