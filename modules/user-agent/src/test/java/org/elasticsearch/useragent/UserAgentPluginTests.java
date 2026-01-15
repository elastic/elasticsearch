/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.useragent;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class UserAgentPluginTests extends ESTestCase {

    private UserAgentPlugin plugin;
    private Setting<?> cacheSizeSetting;

    @Before
    public void createPlugin() {
        plugin = new UserAgentPlugin();
        List<Setting<?>> settingsList = plugin.getSettings();
        cacheSizeSetting = null;
        for (Setting<?> setting : settingsList) {
            if (setting.getKey().equals("user_agent.cache_size")) {
                cacheSizeSetting = setting;
                break;
            }
        }
        assertNotNull(cacheSizeSetting);
    }

    @After
    public void closePlugin() throws Exception {
        plugin.close();
    }

    public void testCacheSizeSettingResolution() {
        // Scenario 1: Neither setting is present (should use default from fallback's default)
        Settings settings1 = Settings.EMPTY;
        assertThat(cacheSizeSetting.get(settings1), equalTo(1000L));

        // Scenario 2: Only deprecated setting is present
        Settings settings2 = Settings.builder().put("ingest.user_agent.cache_size", "2000").build();
        assertThat(cacheSizeSetting.get(settings2), equalTo(2000L));

        // Scenario 3: Only new setting is present
        Settings settings3 = Settings.builder().put("user_agent.cache_size", "3000").build();
        assertThat(cacheSizeSetting.get(settings3), equalTo(3000L));

        // Scenario 4: Both settings are present (new should take precedence)
        Settings settings4 = Settings.builder().put("user_agent.cache_size", "4000").put("ingest.user_agent.cache_size", "5000").build();
        assertThat(cacheSizeSetting.get(settings4), equalTo(4000L));

        // Scenario 5: Test invalid value (should throw IllegalArgumentException)
        Settings settings5 = Settings.builder().put("user_agent.cache_size", "-100").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> cacheSizeSetting.get(settings5));
        assertThat(e.getMessage(), containsString("must be >= 0"));
    }
}
