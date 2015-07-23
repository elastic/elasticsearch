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

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.info.PluginsInfo;
import org.elasticsearch.plugins.PluginInfo;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;

import static org.hamcrest.Matchers.contains;

public class PluginInfoTests extends ElasticsearchTestCase {

    void writeProperties(Path pluginDir, String... stringProps) throws IOException {
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
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "jvm", "true",
            "classname", "FakePlugin");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertEquals("fake-plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
        assertTrue(info.isJvm());
        assertTrue(info.isIsolated());
        assertFalse(info.isSite());
        assertNull(info.getUrl());
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir);
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing description exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[description] is missing"));
        }
    }

    public void testReadFromPropertiesVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir, "description", "fake desc");
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
            "version", "1.0");
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
            "version", "1.0",
            "jvm", "true");
        try {
            PluginInfo.readFromProperties(pluginDir);
            fail("expected missing elasticsearch version exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("[elasticsearch.version] is missing"));
        }
    }

    public void testReadFromPropertiesBogusElasticsearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        writeProperties(pluginDir,
            "description", "fake desc",
            "version", "1.0",
            "jvm", "true",
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
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
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
        writeProperties(pluginDir,
            "description", "fake desc",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "site", "true");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertTrue(info.isSite());
        assertFalse(info.isJvm());
        assertEquals("NA", info.getClassname());
    }

    public void testPluginListSorted() {
        PluginsInfo pluginsInfo = new PluginsInfo(5);
        pluginsInfo.add(new PluginInfo("c", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("b", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("e", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("a", "foo", true, "dummy", true, "dummyclass", true));
        pluginsInfo.add(new PluginInfo("d", "foo", true, "dummy", true, "dummyclass", true));

        final List<PluginInfo> infos = pluginsInfo.getInfos();
        List<String> names = Lists.transform(infos, new Function<PluginInfo, String>() {
            @Override
            public String apply(PluginInfo input) {
                return input.getName();
            }
        });
        assertThat(names, contains("a", "b", "c", "d", "e"));
    }
}
