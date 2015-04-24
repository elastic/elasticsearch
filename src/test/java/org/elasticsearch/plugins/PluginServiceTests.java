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

import com.google.common.collect.ImmutableList;

import org.elasticsearch.action.admin.cluster.node.info.PluginInfo;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.nodesinfo.SimpleNodesInfoTests;
import org.elasticsearch.plugins.loading.classpath.InClassPathPlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import org.junit.Test;

import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.instanceOf;

@ClusterScope(scope= ElasticsearchIntegrationTest.Scope.TEST, numDataNodes=0, numClientNodes = 1, transportClientRatio = 0)
public class PluginServiceTests extends PluginTestCase {

    @Test
    public void testPluginLoadingFromClassName() throws URISyntaxException {
        Settings settings = settingsBuilder()
                                // Defines a plugin in classpath
                                .put(PluginsService.LOAD_PLUGIN_FROM_CLASSPATH, true)
                                .put(PluginsService.ES_PLUGIN_PROPERTIES_FILE_KEY, "es-plugin-test.properties")
                                // Defines a plugin in settings
                                .put("plugin.types", InSettingsPlugin.class.getName())
                            .build();

        startNodeWithPlugins(settings, "/org/elasticsearch/plugins/loading/");

        Plugin plugin = getPlugin("in-settings-plugin");
        assertNotNull("InSettingsPlugin (defined below in this class) must be loaded", plugin);
        assertThat(plugin, instanceOf(InSettingsPlugin.class));

        plugin = getPlugin("in-classpath-plugin");
        assertNotNull("InClassPathPlugin (defined in package ) must be loaded", plugin);
        assertThat(plugin, instanceOf(InClassPathPlugin.class));

        plugin = getPlugin("in-jar-plugin");
        assertNotNull("InJarPlugin (packaged as a JAR file in a plugins directory) must be loaded", plugin);
        assertThat(plugin.getClass().getName(), endsWith("InJarPlugin"));

        plugin = getPlugin("in-zip-plugin");
        assertNotNull("InZipPlugin (packaged as a Zipped file in a plugins directory) must be loaded", plugin);
        assertThat(plugin.getClass().getName(), endsWith("InZipPlugin"));
    }

    @Test
    public void testHasLibExtension() {
        PathMatcher matcher = PathUtils.getDefaultFileSystem().getPathMatcher(PluginsService.PLUGIN_LIB_PATTERN);

        Path p = PathUtils.get("path", "to", "plugin.jar");
        assertTrue(matcher.matches(p));

        p = PathUtils.get("path", "to", "plugin.zip");
        assertTrue(matcher.matches(p));

        p = PathUtils.get("path", "to", "plugin.tar.gz");
        assertFalse(matcher.matches(p));

        p = PathUtils.get("path", "to", "plugin");
        assertFalse(matcher.matches(p));
    }

    private Plugin getPlugin(String pluginName) {
        assertNotNull("cannot check plugin existence with a null plugin's name", pluginName);
        PluginsService pluginsService = internalCluster().getInstance(PluginsService.class);
        ImmutableList<Tuple<PluginInfo, Plugin>> plugins = pluginsService.plugins();

        if ((plugins != null) && (!plugins.isEmpty())) {
            for (Tuple<PluginInfo, Plugin> plugin:plugins) {
                if (pluginName.equals(plugin.v1().getName())) {
                    return plugin.v2();
                }
            }
        }
        return null;
    }

    static class InSettingsPlugin extends AbstractPlugin {

        private final Settings settings;

        public InSettingsPlugin(Settings settings) {
            this.settings = settings;
        }

        @Override
        public String name() {
            return "in-settings-plugin";
        }

        @Override
        public String description() {
            return "A plugin defined in settings";
        }
    }
}
