/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.settings;

import org.elasticsearch.action.admin.cluster.node.info.NodeInfo;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class SettingsFilteringIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SettingsFilteringPlugin.class);
    }

    public static class SettingsFilteringPlugin extends Plugin {
        public static final Setting<Boolean> SOME_NODE_SETTING = Setting.boolSetting(
            "some.node.setting",
            false,
            Property.NodeScope,
            Property.Filtered
        );
        public static final Setting<Boolean> SOME_OTHER_NODE_SETTING = Setting.boolSetting(
            "some.other.node.setting",
            false,
            Property.NodeScope
        );

        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("some.node.setting", true).put("some.other.node.setting", true).build();
        }

        @Override
        public List<Setting<?>> getSettings() {
            return Arrays.asList(
                SOME_NODE_SETTING,
                SOME_OTHER_NODE_SETTING,
                Setting.groupSetting("index.filter_test.", Property.IndexScope)
            );
        }

        @Override
        public List<String> getSettingsFilter() {
            return Arrays.asList("index.filter_test.foo", "index.filter_test.bar*");
        }
    }

    public void testSettingsFiltering() {
        assertAcked(
            indicesAdmin().prepareCreate("test-idx")
                .setSettings(
                    Settings.builder()
                        .put("filter_test.foo", "test")
                        .put("filter_test.bar1", "test")
                        .put("filter_test.bar2", "test")
                        .put("filter_test.notbar", "test")
                        .put("filter_test.notfoo", "test")
                        .build()
                )
                .get()
        );
        GetSettingsResponse response = indicesAdmin().prepareGetSettings("test-idx").get();
        Settings settings = response.getIndexToSettings().get("test-idx");

        assertThat(settings.get("index.filter_test.foo"), nullValue());
        assertThat(settings.get("index.filter_test.bar1"), nullValue());
        assertThat(settings.get("index.filter_test.bar2"), nullValue());
        assertThat(settings.get("index.filter_test.notbar"), equalTo("test"));
        assertThat(settings.get("index.filter_test.notfoo"), equalTo("test"));
    }

    public void testNodeInfoIsFiltered() {
        NodesInfoResponse nodeInfos = clusterAdmin().prepareNodesInfo().clear().setSettings(true).get();
        for (NodeInfo info : nodeInfos.getNodes()) {
            Settings settings = info.getSettings();
            assertNotNull(settings);
            assertNull(settings.get(SettingsFilteringPlugin.SOME_NODE_SETTING.getKey()));
            assertTrue(settings.getAsBoolean(SettingsFilteringPlugin.SOME_OTHER_NODE_SETTING.getKey(), false));
            assertEquals(settings.get("node.name"), info.getNode().getName());
        }
    }
}
