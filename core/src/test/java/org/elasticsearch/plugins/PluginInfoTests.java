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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;

public class PluginInfoTests extends ESTestCase {

    public void testReadFromProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertEquals("my_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
    }

    public void testReadFromPropertiesNameMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir);
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing name exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Property [name] is missing in"));
        }

        PluginTestUtil.writeProperties(pluginDir, "name", "");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing name exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Property [name] is missing in"));
        }
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir, "name", "fake-plugin");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing description exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[description] is missing"));
        }
    }

    public void testReadFromPropertiesVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir, "description", "fake desc", "name", "fake-plugin");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[version] is missing"));
        }
    }

    public void testReadFromPropertiesElasticsearchVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[elasticsearch.version] is missing"));
        }
    }

    public void testReadFromPropertiesJavaVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "elasticsearch.version", Version.CURRENT.toString(),
            "version", "1.0");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing java version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[java.version] is missing"));
        }
    }

    public void testReadFromPropertiesJavaVersionIncompatible() throws Exception {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        PluginTestUtil.writeProperties(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", "1000000.0",
            "classname", "FakePlugin",
            "version", "1.0");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected incompatible java version exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().contains(pluginName + " requires Java"));
        }
    }

    public void testReadFromPropertiesBadJavaVersionFormat() throws Exception {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        PluginTestUtil.writeProperties(pluginDir,
                "description", "fake desc",
                "name", pluginName,
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", "1.7.0_80",
                "classname", "FakePlugin",
                "version", "1.0");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected bad java version format exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().equals("version string must be a sequence of nonnegative decimal integers separated by \".\"'s and may have leading zeros but was 1.7.0_80"));
        }
    }

    public void testReadFromPropertiesBogusElasticsearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir,
            "description", "fake desc",
            "version", "1.0",
            "name", "my_plugin",
            "elasticsearch.version", "bogus");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected bogus elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("version needs to contain major, minor, and revision"));
        }
    }

    public void testReadFromPropertiesOldElasticsearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.V_2_0_0.toString());
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected old elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Was designed for version [2.0.0]"));
        }
    }

    public void testReadFromPropertiesJvmMissingClassname() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"));
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected old elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Property [classname] is missing"));
        }
    }

    public void testPluginListSorted() {
        PluginsAndModules pluginsInfo = new PluginsAndModules();
        pluginsInfo.addPlugin(new PluginInfo("c", "foo", "dummy", "dummyclass"));
        pluginsInfo.addPlugin(new PluginInfo("b", "foo", "dummy", "dummyclass"));
        pluginsInfo.addPlugin(new PluginInfo("e", "foo", "dummy", "dummyclass"));
        pluginsInfo.addPlugin(new PluginInfo("a", "foo", "dummy", "dummyclass"));
        pluginsInfo.addPlugin(new PluginInfo("d", "foo", "dummy", "dummyclass"));

        final List<PluginInfo> infos = pluginsInfo.getPluginInfos();
        List<String> names = infos.stream().map((input) -> input.getName()).collect(Collectors.toList());
        assertThat(names, contains("a", "b", "c", "d", "e"));
    }
}
