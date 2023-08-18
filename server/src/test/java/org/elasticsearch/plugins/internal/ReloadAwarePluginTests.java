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

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESTestCase.WithoutSecurityManager
public class ReloadAwarePluginTests extends ESTestCase {

    public void testNodeInitializesReloadingPlugin() throws Exception {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        try (
            MockNode node = new MockNode(settings, List.of(TestReloadablePlugin.class, TestReloadAwarePlugin.class, Netty4Plugin.class));
        ) {
            PluginsService pluginsService = node.injector().getInstance(PluginsService.class);

            var reloadAwarePlugins = pluginsService.filterPlugins(ReloadAwarePlugin.class).stream().toList();
            var reloadablePlugins = pluginsService.filterPlugins(ReloadablePlugin.class).stream().toList();

            assertThat(reloadAwarePlugins.size(), equalTo(1));
            assertThat(reloadAwarePlugins.get(0), instanceOf(TestReloadAwarePlugin.class));
            TestReloadAwarePlugin reloadAwarePlugin = (TestReloadAwarePlugin) reloadAwarePlugins.get(0);

            assertThat(reloadablePlugins.size(), equalTo(1));
            assertThat(reloadablePlugins.get(0), instanceOf(TestReloadablePlugin.class));
            TestReloadablePlugin reloadablePlugin = (TestReloadablePlugin) reloadablePlugins.get(0);

            assertFalse("Plugin has been reloaded", reloadablePlugin.isReloaded());
            reloadAwarePlugin.invokeReloadOperation();
            assertTrue("Plugin has been reloaded", reloadablePlugin.isReloaded());
        }
    }

    public static class TestReloadAwarePlugin extends Plugin implements ReloadAwarePlugin {
        private ReloadablePlugin reloadablePlugin;

        @Override
        public void setReloadCallback(ReloadablePlugin reloadablePlugin) {
            this.reloadablePlugin = reloadablePlugin;
        }

        public void invokeReloadOperation() throws Exception {
            reloadablePlugin.reload(Settings.EMPTY);
        }
    }

    public static class TestReloadablePlugin extends Plugin implements ReloadablePlugin {

        private boolean reloaded = false;

        @Override
        public void reload(Settings settings) throws Exception {
            reloaded = true;
        }

        public boolean isReloaded() {
            return reloaded;
        }
    }
}
