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
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;

@LuceneTestCase.SuppressFileSystems(value = "ExtrasFS")
public class UberPluginInfoTests extends ESTestCase {

    public void testReadFromProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-uber-plugin");
        PluginTestUtil.writeUberPluginProperties(pluginDir,
            "description", "fake desc",
            "name", "my_uber_plugin",
            "plugins", "fake_plugin1,fake_plugin2");
        UberPluginInfo info = UberPluginInfo.readFromProperties(pluginDir);
        assertEquals("my_uber_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals(2, info.getPlugins().length);
        assertEquals("fake_plugin1", info.getPlugins()[0]);
        assertEquals("fake_plugin2", info.getPlugins()[1]);
    }

    public void testReadFromPropertiesNameMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-uber-plugin");
        PluginTestUtil.writeUberPluginProperties(pluginDir);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> UberPluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [name] is missing for"));

        PluginTestUtil.writeUberPluginProperties(pluginDir, "name", "");
        e = expectThrows(IllegalArgumentException.class, () -> UberPluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [name] is missing for"));
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-uber-plugin");
        PluginTestUtil.writeUberPluginProperties(pluginDir, "name", "fake-uber-plugin");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> UberPluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[description] is missing"));
    }

    public void testReadFromPropertiesPluginsMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-uber-plugin");
        PluginTestUtil.writeUberPluginProperties(pluginDir,
            "name", "fake-uber-plugin",
            "description", "desc");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> UberPluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[plugins] is missing"));
    }

    public void testReadFromPropertiesPluginsEmpty() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-uber-plugin");
        PluginTestUtil.writeUberPluginProperties(pluginDir,
            "name", "fake-uber-plugin",
            "description", "desc",
            "plugins", "");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> UberPluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[plugins] is missing or empty"));
    }

    public void testReadFromPropertiesPluginsTrim() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-uber-plugin");
        PluginTestUtil.writeUberPluginProperties(pluginDir,
            "name", "fake-uber-plugin",
            "description", "desc",
            "plugins", " fake_plugin1, fake_plugin2 ,fake_plugin3  ");
        UberPluginInfo info = UberPluginInfo.readFromProperties(pluginDir);
        assertEquals(3, info.getPlugins().length);
        assertEquals("fake_plugin1", info.getPlugins()[0]);
        assertEquals("fake_plugin2", info.getPlugins()[1]);
        assertEquals("fake_plugin3", info.getPlugins()[2]);
    }

    public void testUnknownProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-uber-plugin");
        PluginTestUtil.writeUberPluginProperties(pluginDir,
            "extra", "property",
            "unknown", "property",
            "description", "fake desc",
            "plugins", "my_fake_plugin_1",
            "name", "my_uber_plugin");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> UberPluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Unknown properties in uber-plugin descriptor"));
    }

    public void testExtractAllPlugins() throws Exception {
        Path pluginDir = createTempDir().resolve("plugins");
        // Simple plugin
        Path plugin1 = pluginDir.resolve("plugin1");
        Files.createDirectories(plugin1);
        PluginTestUtil.writePluginProperties(plugin1,
            "description", "fake desc",
            "name", "plugin1",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");

        // Uber plugin
        Path uberPlugin = pluginDir.resolve("uber_plugin");
        Files.createDirectory(uberPlugin);
        PluginTestUtil.writeUberPluginProperties(uberPlugin,
            "description", "fake desc",
            "plugins", "plugin2,plugin3",
            "name", "uber_plugin");
        Path plugin2 = uberPlugin.resolve("plugin2");
        Files.createDirectory(plugin2);
        PluginTestUtil.writePluginProperties(plugin2,
            "description", "fake desc",
            "name", "plugin2",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");
        Path plugin3 = uberPlugin.resolve("plugin3");
        Files.createDirectory(plugin3);
        PluginTestUtil.writePluginProperties(plugin3,
            "description", "fake desc",
            "name", "plugin3",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");

        List<PluginInfo> infos = PluginInfo.extractAllPlugins(pluginDir);
        Collections.sort(infos, Comparator.comparing(PluginInfo::getFullName));
        assertEquals(infos.size(), 3);
        assertEquals(infos.get(0).getName(), "plugin1");
        assertNull(infos.get(0).getUberPlugin());
        assertEquals(infos.get(0).getPath(pluginDir), plugin1);
        assertEquals(infos.get(1).getName(), "plugin2");
        assertEquals(infos.get(1).getUberPlugin(), "uber_plugin");
        assertEquals(infos.get(1).getPath(pluginDir), plugin2);
        assertEquals(infos.get(2).getName(), "plugin3");
        assertEquals(infos.get(2).getUberPlugin(), "uber_plugin");
        assertEquals(infos.get(2).getPath(pluginDir), plugin3);
    }

    public void testExtractAllPluginsWithDuplicates() throws Exception {
        Path pluginDir = createTempDir().resolve("plugins");
        // Simple plugin
        Path plugin1 = pluginDir.resolve("plugin1");
        Files.createDirectories(plugin1);
        PluginTestUtil.writePluginProperties(plugin1,
            "description", "fake desc",
            "name", "plugin1",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");

        // Uber plugin
        Path uberPlugin = pluginDir.resolve("uber_plugin");
        Files.createDirectory(uberPlugin);
        PluginTestUtil.writeUberPluginProperties(uberPlugin,
            "description", "fake desc",
            "plugins", "plugin1,plugin2",
            "name", "uber_plugin");
        Path plugin2 = uberPlugin.resolve("plugin1");
        Files.createDirectory(plugin2);
        PluginTestUtil.writePluginProperties(plugin2,
            "description", "fake desc",
            "name", "plugin1",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");
        Path plugin3 = uberPlugin.resolve("plugin2");
        Files.createDirectory(plugin3);
        PluginTestUtil.writePluginProperties(plugin3,
            "description", "fake desc",
            "name", "plugin2",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");

        IllegalStateException exc =
            expectThrows(IllegalStateException.class, () -> PluginInfo.extractAllPlugins(pluginDir));
        assertThat(exc.getMessage(), containsString("duplicate plugin"));
        assertThat(exc.getMessage(), endsWith("plugin1"));
    }
}
