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
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class PluginInfoTests extends ESTestCase {

    public void testReadFromProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
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
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testReadFromPropertiesNameMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [name] is missing in"));

        PluginTestUtil.writePluginProperties(pluginDir, "name", "");
        e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [name] is missing in"));
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir, "name", "fake-plugin");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[description] is missing"));
    }

    public void testReadFromPropertiesVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(
                pluginDir, "description", "fake desc", "name", "fake-plugin");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[version] is missing"));
    }

    public void testReadFromPropertiesElasticsearchVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[elasticsearch.version] is missing"));
    }

    public void testReadFromPropertiesJavaVersionMissing() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "elasticsearch.version", Version.CURRENT.toString(),
            "version", "1.0");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("[java.version] is missing"));
    }

    public void testReadFromPropertiesBadJavaVersionFormat() throws Exception {
        String pluginName = "fake-plugin";
        Path pluginDir = createTempDir().resolve(pluginName);
        PluginTestUtil.writePluginProperties(pluginDir,
                "description", "fake desc",
                "name", pluginName,
                "elasticsearch.version", Version.CURRENT.toString(),
                "java.version", "1.7.0_80",
                "classname", "FakePlugin",
                "version", "1.0");
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), equalTo("version string must be a sequence of nonnegative decimal integers separated" +
                                           " by \".\"'s and may have leading zeros but was 1.7.0_80"));
    }

    public void testReadFromPropertiesBogusElasticsearchVersion() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "description", "fake desc",
            "version", "1.0",
            "name", "my_plugin",
            "elasticsearch.version", "bogus");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("version needs to contain major, minor, and revision"));
    }

    public void testReadFromPropertiesJvmMissingClassname() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("property [classname] is missing"));
    }

    public void testExtendedPluginsSingleExtension() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin",
            "extended.plugins", "foo");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertThat(info.getExtendedPlugins(), contains("foo"));
    }

    public void testExtendedPluginsMultipleExtensions() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin",
            "extended.plugins", "foo,bar,baz");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertThat(info.getExtendedPlugins(), contains("foo", "bar", "baz"));
    }

    public void testExtendedPluginsEmpty() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "description", "fake desc",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"),
            "classname", "FakePlugin",
            "extended.plugins", "");
        PluginInfo info = PluginInfo.readFromProperties(pluginDir);
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testSerialize() throws Exception {
        PluginInfo info = new PluginInfo("c", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass",
                                         Collections.singletonList("foo"), randomBoolean());
        BytesStreamOutput output = new BytesStreamOutput();
        info.writeTo(output);
        ByteBuffer buffer = ByteBuffer.wrap(output.bytes().toBytesRef().bytes);
        ByteBufferStreamInput input = new ByteBufferStreamInput(buffer);
        PluginInfo info2 = new PluginInfo(input);
        assertThat(info2.toString(), equalTo(info.toString()));

    }

    public void testPluginListSorted() {
        List<PluginInfo> plugins = new ArrayList<>();
        plugins.add(new PluginInfo("c", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass",
            Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo("b", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass",
            Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo( "e", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass",
            Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo("a", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass",
            Collections.emptyList(), randomBoolean()));
        plugins.add(new PluginInfo("d", "foo", "dummy", Version.CURRENT, "1.8", "dummyclass",
            Collections.emptyList(), randomBoolean()));
        PluginsAndModules pluginsInfo = new PluginsAndModules(plugins, Collections.emptyList());

        final List<PluginInfo> infos = pluginsInfo.getPluginInfos();
        List<String> names = infos.stream().map(PluginInfo::getName).collect(Collectors.toList());
        assertThat(names, contains("a", "b", "c", "d", "e"));
    }

    public void testUnknownProperties() throws Exception {
        Path pluginDir = createTempDir().resolve("fake-plugin");
        PluginTestUtil.writePluginProperties(pluginDir,
            "extra", "property",
            "unknown", "property",
            "description", "fake desc",
            "classname", "Foo",
            "name", "my_plugin",
            "version", "1.0",
            "elasticsearch.version", Version.CURRENT.toString(),
            "java.version", System.getProperty("java.specification.version"));
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PluginInfo.readFromProperties(pluginDir));
        assertThat(e.getMessage(), containsString("Unknown properties in plugin descriptor"));
    }

}
