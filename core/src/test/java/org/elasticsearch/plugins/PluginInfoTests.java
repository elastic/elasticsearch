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
import org.elasticsearch.action.admin.cluster.node.info.PluginsInfo;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;

public class PluginInfoTests extends ESTestCase {

    static void writeProperties(Path pluginDir, String... stringProps) throws IOException {
        assert stringProps.length % 2 == 0;
        Files.createDirectories(pluginDir);
        Path propertiesFile = pluginDir.resolve(PluginInfo.ES_PLUGIN_PROPERTIES);
        Properties properties =  new Properties();
        for (int i = 0; i < stringProps.length; i += 2) {
            properties.put(stringProps[i], stringProps[i + 1]);
        }
        try (OutputStream out = Files.newOutputStream(propertiesFile)) {
            properties.store(out, "");
        }
    }

    public void testReadFromProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true",
            "classname", "FakePlugin");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertEquals("my_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
        assertTrue(info.isJvm());
        assertTrue(info.isIsolated());
        assertFalse(info.isSite());
        assertNull(info.getUrl());
    }

    public void testReadFromPropertiesNameMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir);
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing name exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Property [name] is missing in"));
        }

        writeProperties(pluginDir, "name", "");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing name exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Property [name] is missing in"));
        }
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir, "name", "fake-plugin");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing description exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[description] is missing"));
        }
    }

    public void testReadFromPropertiesVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir, "description", "fake desc", "name", "fake-plugin");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[version] is missing"));
        }
    }

    public void testReadFromPropertiesJvmAndSiteMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "version", "1.0",
            "name", "my_plugin");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected jvm or site exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("must be at least a jvm or site plugin"));
        }
    }

    public void testReadFromPropertiesElasticsearchVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "jvm", "true");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[elasticsearch.version] is missing"));
        }
    }

    public void testReadFromPropertiesJavaVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "elasticsearch.version", Version.CURRENT.toString(),
            "version", "1.0",
            "jvm", "true");
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
        writeProperties(pluginDir,
            "description", "fake desc",
            "name", pluginName,
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", "1000000.0",
            "classname", "FakePlugin",
            "version", "1.0",
            "jvm", "true");
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
        writeProperties(pluginDir,
                "description", "fake desc",
                "name", pluginName,
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", "1.7.0_80",
                "classname", "FakePlugin",
                "version", "1.0",
                "jvm", "true");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected bad java version format exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage(), e.getMessage().equals("version string must be a sequence of nonnegative decimal integers separated by \".\"'s and may have leading zeros but was 1.7.0_80"));
        }
    }

    public void testReadFromPropertiesBogusElasticsearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "version", "1.0",
            "jvm", "true",
            "name", "my_plugin",
            "elasticsearch.version", "bogus");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected bogus elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("version needs to contain major, minor and revision"));
        }
    }

    public void testReadFromPropertiesOldElasticsearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "jvm", "true",
            "elasticsearch.version", Version.V_1_7_0.toString());
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected old elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Elasticsearch version [1.7.0] is too old"));
        }
    }

    public void testReadFromPropertiesJvmMissingClassname() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "jvm", "true");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected old elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Property [classname] is missing"));
        }
    }

    public void testReadFromPropertiesSitePlugin() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        Files.createDirectories(pluginDir.resolve("_site"));
        writeProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "site", "true");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertTrue(info.isSite());
        assertFalse(info.isJvm());
        assertEquals("NA", info.getClassname());
    }

    public void testReadFromPropertiesSitePluginWithoutSite() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
                "description", "fake desc",
                "name", "my_plugin",
                "version", "1.0",
                "site", "true");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("didn't get expected exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("site plugin but has no '_site"));
        }
    }

    public void testPluginListSorted() {
        PluginsInfo pluginsInfo = new PluginsInfo(5);
        pluginsInfo.add(new PluginInfo("c", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("b", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("e", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("a", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("d", "foo", true, "dummy", true, "dummyclass", true));

        final List<PluginInfo> infos = pluginsInfo.getInfos();
        List<String> names = infos.stream().map((input) -> input.getName()).collect(Collectors.toList());
        assertThat(names, contains("a", "b", "c", "d", "e"));
    }
}
