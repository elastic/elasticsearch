/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.Build;
import org.elasticsearch.action.admin.cluster.node.info.PluginsAndModules;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class PluginDescriptorTests extends ESTestCase {

    private static final Map<String, String> INTERNAL_DESCRIPTOR_TEMPLATE = Map.of(
        "name",
        "my_plugin",
        "description",
        "fake desc",
        "version",
        "1.0",
        "elasticsearch.version",
        Build.current().version(),
        "java.version",
        System.getProperty("java.specification.version"),
        "classname",
        "FakePlugin",
        "modulename",
        "org.mymodule"
    );

    private static final Map<String, String> STABLE_DESCRIPTOR_TEMPLATE = Map.of(
        "name",
        "my_plugin",
        "description",
        "fake desc",
        "version",
        "1.0",
        "elasticsearch.version",
        Build.current().version(),
        "java.version",
        System.getProperty("java.specification.version"),
        "modular",
        "true"
    );

    private interface PropertiesWriter {
        void write(Path path, String... props) throws IOException;
    }

    private interface DescriptorWriter {
        PluginDescriptor write(String... props) throws IOException;
    }

    static PluginDescriptor mockInternalDescriptor(String... additionalProps) throws IOException {
        return mockDescriptor(INTERNAL_DESCRIPTOR_TEMPLATE, PluginTestUtil::writePluginProperties, additionalProps);
    }

    static PluginDescriptor mockStableDescriptor(String... additionalProps) throws IOException {
        return mockDescriptor(STABLE_DESCRIPTOR_TEMPLATE, PluginTestUtil::writeStablePluginProperties, additionalProps);
    }

    static PluginDescriptor mockDescriptor(Map<String, String> template, PropertiesWriter writer, String... addProps) throws IOException {
        assert addProps.length % 2 == 0;
        Map<String, String> propsMap = new HashMap<>(template);
        for (int i = 0; i < addProps.length; i += 2) {
            propsMap.put(addProps[i], addProps[i + 1]);
        }
        String[] props = new String[propsMap.size() * 2];
        int i = 0;
        for (var e : propsMap.entrySet()) {
            props[i] = e.getKey();
            props[i + 1] = e.getValue();
            i += 2;
        }

        Path pluginDir = createTempDir().resolve("fake-plugin");
        writer.write(pluginDir, props);
        return PluginDescriptor.readFromProperties(pluginDir);
    }

    void assertBothDescriptors(Consumer<DescriptorWriter> assertions) {
        assertions.accept(PluginDescriptorTests::mockInternalDescriptor);
        assertions.accept(PluginDescriptorTests::mockStableDescriptor);
    }

    public void testReadInternalDescriptorFromStream() throws Exception {
        PluginDescriptor info = mockInternalDescriptor();
        assertEquals("my_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertEquals("FakePlugin", info.getClassname());
        assertEquals("org.mymodule", info.getModuleName().orElseThrow());
        assertThat(info.isModular(), is(true));
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testReadStableDescriptor() throws Exception {
        PluginDescriptor info = mockStableDescriptor();
        assertEquals("my_plugin", info.getName());
        assertEquals("fake desc", info.getDescription());
        assertEquals("1.0", info.getVersion());
        assertThat(info.isModular(), is(true));
    }

    public void testReadFromPropertiesNameMissing() throws Exception {
        assertBothDescriptors(writer -> {
            var e = expectThrows(IllegalArgumentException.class, () -> writer.write("name", null));
            assertThat(e.getMessage(), containsString("property [name] is missing"));

            e = expectThrows(IllegalArgumentException.class, () -> writer.write("name", ""));
            assertThat(e.getMessage(), containsString("property [name] is missing"));
        });
    }

    public void testReadFromPropertiesDescriptionMissing() throws Exception {
        assertBothDescriptors(writer -> {
            var e = expectThrows(IllegalArgumentException.class, () -> writer.write("description", null));
            assertThat(e.getMessage(), containsString("[description] is missing"));
        });
    }

    public void testReadFromPropertiesVersionMissing() throws Exception {
        assertBothDescriptors(writer -> {
            var e = expectThrows(IllegalArgumentException.class, () -> writer.write("version", null));
            assertThat(e.getMessage(), containsString("[version] is missing"));
        });
    }

    public void testReadFromPropertiesElasticsearchVersionMissing() throws Exception {
        assertBothDescriptors(writer -> {
            var e = expectThrows(IllegalArgumentException.class, () -> writer.write("elasticsearch.version", null));
            assertThat(e.getMessage(), containsString("[elasticsearch.version] is missing"));
        });
    }

    public void testReadFromPropertiesElasticsearchVersionEmpty() throws Exception {
        assertBothDescriptors(writer -> {
            var e = expectThrows(IllegalArgumentException.class, () -> writer.write("elasticsearch.version", " "));
            assertThat(e.getMessage(), containsString("[elasticsearch.version] is missing"));
        });
    }

    public void testReadFromPropertiesJavaVersionMissing() throws Exception {
        assertBothDescriptors(writer -> {
            var e = expectThrows(IllegalArgumentException.class, () -> writer.write("java.version", null));
            assertThat(e.getMessage(), containsString("[java.version] is missing"));
        });
    }

    public void testReadFromPropertiesBadJavaVersionFormat() throws Exception {
        assertBothDescriptors(writer -> {
            var e = expectThrows(IllegalArgumentException.class, () -> writer.write("java.version", "1.7.0_80"));
            assertThat(e.getMessage(), equalTo("Invalid version string: '1.7.0_80'"));
        });
    }

    public void testReadFromPropertiesJvmMissingClassname() throws Exception {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> mockInternalDescriptor("classname", null));
        assertThat(e.getMessage(), containsString("property [classname] is missing"));
    }

    public void testReadFromPropertiesModulenameFallback() throws Exception {
        PluginDescriptor info = mockInternalDescriptor("modulename", null);
        assertThat(info.getModuleName(), isEmpty());
        assertThat(info.isModular(), is(false));
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testReadFromPropertiesModulenameEmpty() throws Exception {
        PluginDescriptor info = mockInternalDescriptor("modulename", " ");
        assertThat(info.getModuleName(), isEmpty());
        assertThat(info.isModular(), is(false));
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testExtendedPluginsSingleExtension() throws Exception {
        PluginDescriptor info = mockInternalDescriptor("extended.plugins", "foo");
        assertThat(info.getExtendedPlugins(), contains("foo"));
    }

    public void testExtendedPluginsMultipleExtensions() throws Exception {
        PluginDescriptor info = mockInternalDescriptor("extended.plugins", "foo,bar,baz");
        assertThat(info.getExtendedPlugins(), contains("foo", "bar", "baz"));
    }

    public void testExtendedPluginsEmpty() throws Exception {
        PluginDescriptor info = mockInternalDescriptor("extended.plugins", "");
        assertThat(info.getExtendedPlugins(), empty());
    }

    public void testIsModular() throws Exception {
        PluginDescriptor info = mockStableDescriptor("modular", "false");
        assertThat(info.isModular(), is(false));
    }

    public void testSerialize() throws Exception {
        PluginDescriptor info = new PluginDescriptor(
            "c",
            "foo",
            "dummy",
            Build.current().version(),
            "1.8",
            "dummyclass",
            null,
            Collections.singletonList("foo"),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            false
        );
        BytesStreamOutput output = new BytesStreamOutput();
        info.writeTo(output);
        ByteBuffer buffer = ByteBuffer.wrap(output.bytes().toBytesRef().bytes);
        ByteBufferStreamInput input = new ByteBufferStreamInput(buffer);
        PluginDescriptor info2 = new PluginDescriptor(input);
        assertThat(info2.toString(), equalTo(info.toString()));
    }

    public void testSerializeWithModuleName() throws Exception {
        PluginDescriptor info = new PluginDescriptor(
            "c",
            "foo",
            "dummy",
            Build.current().version(),
            "1.8",
            "dummyclass",
            "some.module",
            Collections.singletonList("foo"),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            false
        );
        BytesStreamOutput output = new BytesStreamOutput();
        info.writeTo(output);
        ByteBuffer buffer = ByteBuffer.wrap(output.bytes().toBytesRef().bytes);
        ByteBufferStreamInput input = new ByteBufferStreamInput(buffer);
        PluginDescriptor info2 = new PluginDescriptor(input);
        assertThat(info2.toString(), equalTo(info.toString()));
    }

    public void testSerializeStablePluginDescriptor() throws Exception {
        PluginDescriptor info = mockStableDescriptor();
        BytesStreamOutput output = new BytesStreamOutput();
        info.writeTo(output);
        ByteBuffer buffer = ByteBuffer.wrap(output.bytes().toBytesRef().bytes);
        ByteBufferStreamInput input = new ByteBufferStreamInput(buffer);
        PluginDescriptor info2 = new PluginDescriptor(input);
        assertThat(info2.toString(), equalTo(info.toString()));
    }

    PluginDescriptor newMockDescriptor(String name) {
        return new PluginDescriptor(
            name,
            "foo",
            "dummy",
            Build.current().version(),
            "1.8",
            "dummyclass",
            null,
            List.of(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            false
        );
    }

    public void testPluginListSorted() {
        List<PluginRuntimeInfo> plugins = new ArrayList<>();
        plugins.add(new PluginRuntimeInfo(newMockDescriptor("c")));
        plugins.add(new PluginRuntimeInfo(newMockDescriptor("b")));
        plugins.add(new PluginRuntimeInfo(newMockDescriptor("e")));
        plugins.add(new PluginRuntimeInfo(newMockDescriptor("a")));
        plugins.add(new PluginRuntimeInfo(newMockDescriptor("d")));
        PluginsAndModules pluginsInfo = new PluginsAndModules(plugins, Collections.emptyList());

        final List<PluginRuntimeInfo> infos = pluginsInfo.getPluginInfos();
        List<String> names = infos.stream().map(p -> p.descriptor().getName()).toList();
        assertThat(names, contains("a", "b", "c", "d", "e"));
    }

    public void testUnknownProperties() throws Exception {
        assertBothDescriptors(writer -> {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> writer.write("extra", "property"));
            assertThat(e.getMessage(), containsString("Unknown properties for plugin [my_plugin] in plugin descriptor"));
        });
    }

    /**
     * This is important because {@link PluginsUtils#getPluginBundles(Path)} will
     * use the hashcode to catch duplicate names
     */
    public void testPluginEqualityAndHash() {
        var isStable = randomBoolean();
        var classname = isStable ? null : "dummyclass";
        PluginDescriptor descriptor1 = new PluginDescriptor(
            "c",
            "foo",
            "dummy",
            Build.current().version(),
            "1.8",
            classname,
            null,
            Collections.singletonList("foo"),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            isStable
        );
        // everything but name is different from descriptor1
        PluginDescriptor descriptor2 = new PluginDescriptor(
            descriptor1.getName(),
            randomValueOtherThan(descriptor1.getDescription(), () -> randomAlphaOfLengthBetween(4, 12)),
            randomValueOtherThan(descriptor1.getVersion(), () -> randomAlphaOfLengthBetween(4, 12)),
            "8.0.0",
            randomValueOtherThan(descriptor1.getJavaVersion(), () -> randomAlphaOfLengthBetween(4, 12)),
            descriptor1.isStable() ? randomAlphaOfLengthBetween(4, 12) : null,
            descriptor1.isStable() ? randomAlphaOfLength(6) : null,
            Collections.singletonList(
                randomValueOtherThanMany(v -> descriptor1.getExtendedPlugins().contains(v), () -> randomAlphaOfLengthBetween(4, 12))
            ),
            descriptor1.hasNativeController() == false,
            descriptor1.isLicensed() == false,
            descriptor1.isModular() == false,
            descriptor1.isStable() == false
        );
        // only name is different from descriptor1
        PluginDescriptor descriptor3 = new PluginDescriptor(
            randomValueOtherThan(descriptor1.getName(), () -> randomAlphaOfLengthBetween(4, 12)),
            descriptor1.getDescription(),
            descriptor1.getVersion(),
            descriptor1.getElasticsearchVersion(),
            descriptor1.getJavaVersion(),
            classname,
            descriptor1.getModuleName().orElse(null),
            descriptor1.getExtendedPlugins(),
            descriptor1.hasNativeController(),
            descriptor1.isLicensed(),
            descriptor1.isModular(),
            descriptor1.isStable()
        );

        assertThat(descriptor1, equalTo(descriptor2));
        assertThat(descriptor1.hashCode(), equalTo(descriptor2.hashCode()));

        assertThat(descriptor1, not(equalTo(descriptor3)));
        assertThat(descriptor1.hashCode(), not(equalTo(descriptor3.hashCode())));
    }

}
