/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.internal;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.MockNode;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.ReloadablePlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.netty4.Netty4Plugin;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESTestCase.WithoutSecurityManager
public class ReloadAwarePluginTests extends ESTestCase {

    public void testNodeInitializesReloadingPlugin() throws IOException {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        try (MockNode node = new MockNode(settings, List.of(TestReloadablePlugin.class, TestReloadAwarePlugin.class, Netty4Plugin.class));) {
            PluginsService pluginsService = node.injector().getInstance(PluginsService.class);

            var reloadingPlugins = pluginsService.filterPlugins(ReloadAwarePlugin.class).stream().toList();
            var reloadablePlugins = pluginsService.filterPlugins(ReloadablePlugin.class).stream().toList();

            assertThat(reloadingPlugins.size(), equalTo(1));

            ReloadAwarePlugin plugin = reloadingPlugins.get(0);

            assertThat(plugin, instanceOf(TestReloadAwarePlugin.class));

            assertThat(((TestReloadAwarePlugin) plugin).getReloadablePlugins(), equalTo(reloadablePlugins));
        }
    }

    public static class TestReloadAwarePlugin extends Plugin implements ReloadAwarePlugin {
        private List<ReloadablePlugin> reloadablePlugins;

        @Override
        public void setReloadablePlugins(List<ReloadablePlugin> reloadablePlugins) {
            this.reloadablePlugins = reloadablePlugins;
        }

        public List<ReloadablePlugin> getReloadablePlugins() {
            return this.reloadablePlugins;
        }
    }

    public static class TestReloadablePlugin extends Plugin implements ReloadablePlugin {
        @Override
        public void reload(Settings settings) throws Exception {
            // do nothing
        }
    }
}
