/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.inject.AbstractModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.test.ESTestCase;

public class PluginsServiceTests extends ESTestCase {
    public static class AdditionalSettingsPlugin1 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("foo.bar", "1").put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.MMAPFS.getSettingsKey()).build();
        }
    }
    public static class AdditionalSettingsPlugin2 extends Plugin {
        @Override
        public Settings additionalSettings() {
            return Settings.builder().put("foo.bar", "2").build();
        }
    }

    public static class FailOnModule extends Plugin {
        public void onModule(BrokenModule brokenModule) {
            throw new IllegalStateException("boom");
        }
    }

    public static class FilterablePlugin extends Plugin implements ScriptPlugin {}

    public static class BrokenModule extends AbstractModule {

        @Override
        protected void configure() {
        }
    }

    static PluginsService newPluginsService(Settings settings, Class<? extends Plugin>... classpathPlugins) {
        return new PluginsService(settings, null, new Environment(settings).pluginsFile(), Arrays.asList(classpathPlugins));
    }

    public void testAdditionalSettings() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("my.setting", "test")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.SIMPLEFS.getSettingsKey()).build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class);
        Settings newSettings = service.updatedSettings();
        assertEquals("test", newSettings.get("my.setting")); // previous settings still exist
        assertEquals("1", newSettings.get("foo.bar")); // added setting exists
        assertEquals(IndexModule.Type.SIMPLEFS.getSettingsKey(), newSettings.get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey())); // does not override pre existing settings
    }

    public void testAdditionalSettingsClash() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class, AdditionalSettingsPlugin2.class);
        try {
            service.updatedSettings();
            fail("Expected exception when building updated settings");
        } catch (IllegalArgumentException e) {
            String msg = e.getMessage();
            assertTrue(msg, msg.contains("Cannot have additional setting [foo.bar]"));
            assertTrue(msg, msg.contains("plugin [" + AdditionalSettingsPlugin1.class.getName()));
            assertTrue(msg, msg.contains("plugin [" + AdditionalSettingsPlugin2.class.getName()));
        }
    }

    public void testOnModuleExceptionsArePropagated() {
        Settings settings = Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        PluginsService service = newPluginsService(settings, FailOnModule.class);
        try {
            service.processModule(new BrokenModule());
            fail("boom");
        } catch (ElasticsearchException ex) {
            assertEquals("failed to invoke onModule", ex.getMessage());
            assertEquals("boom", ex.getCause().getCause().getMessage());
        }
    }

    public void testExistingPluginMissingDescriptor() throws Exception {
        Path pluginsDir = createTempDir();
        Files.createDirectory(pluginsDir.resolve("plugin-missing-descriptor"));
        try {
            PluginsService.getPluginBundles(pluginsDir);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Could not load plugin descriptor for existing plugin"));
        }
    }

    public void testFilterPlugins() {
        Settings settings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("my.setting", "test")
            .put(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.SIMPLEFS.getSettingsKey()).build();
        PluginsService service = newPluginsService(settings, AdditionalSettingsPlugin1.class, FilterablePlugin.class);
        List<ScriptPlugin> scriptPlugins = service.filterPlugins(ScriptPlugin.class);
        assertEquals(1, scriptPlugins.size());
        assertEquals(FilterablePlugin.class, scriptPlugins.get(0).getClass());
    }
}
