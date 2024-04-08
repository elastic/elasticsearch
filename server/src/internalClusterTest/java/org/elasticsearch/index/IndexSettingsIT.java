/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class IndexSettingsIT extends ESIntegTestCase {

    public static final Setting<Boolean> TEST_SETTING = Setting.boolSetting("index.test_setting", false, Setting.Property.IndexScope);

    public static class TestPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return registerSetting ? List.of(TEST_SETTING) : List.of();
        }
    }

    private static volatile boolean registerSetting = true;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestPlugin.class);
    }

    public void testCanRemoveArchivedSettings() throws Exception {
        createIndex("test", Settings.builder().put(TEST_SETTING.getKey(), "true").build());
        registerSetting = false;
        try {
            internalCluster().fullRestart();

            final var indicesClient = indicesAdmin();
            assertThat(indicesClient.prepareGetSettings("test").get().getSetting("test", "archived.index.test_setting"), equalTo("true"));
            updateIndexSettings(Settings.builder().putNull("archived.*"), "test");
            assertNull(indicesClient.prepareGetSettings("test").get().getSetting("test", "archived.index.test_setting"));
        } finally {
            registerSetting = true;
        }
    }
}
