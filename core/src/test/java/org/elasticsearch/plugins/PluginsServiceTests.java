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

import org.apache.lucene.util.LuceneTestCase;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
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

    public static class FilterablePlugin extends Plugin implements ScriptPlugin {}

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

    public void testExistingPluginMissingDescriptor() throws Exception {
        Path pluginsDir = createTempDir();
        Files.createDirectory(pluginsDir.resolve("plugin-missing-descriptor"));
        try {
            PluginsService.getPluginBundles(pluginsDir);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains("Could not load plugin descriptor for existing plugin [plugin-missing-descriptor]"));
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

    public void testHiddenFiles() throws IOException {
        final Path home = createTempDir();
        final Settings settings =
                Settings.builder()
                        .put(Environment.PATH_HOME_SETTING.getKey(), home)
                        .build();
        final Path hidden = home.resolve("plugins").resolve(".hidden");
        Files.createDirectories(hidden);
        @SuppressWarnings("unchecked")
        final IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> newPluginsService(settings));

        final String expected = "Could not load plugin descriptor for existing plugin [.hidden]";
        assertThat(e, hasToString(containsString(expected)));
    }

    public void testStartupWithRemovingMarker() throws IOException {
        final Path home = createTempDir();
        final Settings settings =
                Settings.builder()
                        .put(Environment.PATH_HOME_SETTING.getKey(), home)
                        .build();
        final Path fake = home.resolve("plugins").resolve("fake");
        Files.createDirectories(fake);
        Files.createFile(fake.resolve("plugin.jar"));
        final Path removing = home.resolve("plugins").resolve(".removing-fake");
        Files.createFile(removing);
        PluginTestUtil.writeProperties(
                fake,
                "description", "fake",
                "name", "fake",
                "version", "1.0.0",
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", System.getProperty("java.specification.version"),
                "classname", "Fake",
                "has.native.controller", "false");
        final IllegalStateException e = expectThrows(IllegalStateException.class, () -> newPluginsService(settings));
        final String expected = String.format(
                Locale.ROOT,
                "found file [%s] from a failed attempt to remove the plugin [fake]; execute [elasticsearch-plugin remove fake]",
                removing);
        assertThat(e, hasToString(containsString(expected)));
    }

}
