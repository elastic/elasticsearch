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

package org.elasticsearch.plugins.repository;

import com.google.common.base.Charsets;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.repository.PluginDescriptor;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.elasticsearch.plugins.repository.PluginDescriptor.Builder.pluginDescriptor;
import static org.hamcrest.Matchers.*;

/**
 * Unit tests class for {@link org.elasticsearch.plugins.repository.PluginDescriptor}
 */
public class PluginDescriptorTests extends ElasticsearchTestCase {

    @Test
    public void testPluginDescriptorFqn() {
        PluginDescriptor descriptor = pluginDescriptor(null).build();
        assertNull(descriptor.organisation());
        assertNull(descriptor.name());
        assertNull(descriptor.version());
        assertNull(descriptor.artifact());

        descriptor = pluginDescriptor("").build();
        assertNull(descriptor.organisation());
        assertThat(descriptor.name(), equalTo(""));
        assertNull(descriptor.version());

        descriptor = pluginDescriptor("my-plugin").build();
        assertNull(descriptor.organisation());
        assertThat(descriptor.name(), equalTo("my-plugin"));
        assertNull(descriptor.version());

        descriptor = pluginDescriptor("elasticsearch-my-plugin").build();
        assertNull(descriptor.organisation());
        assertThat(descriptor.name(), equalTo("my-plugin"));
        assertNull(descriptor.version());

        descriptor = pluginDescriptor("my-org/my-plugin").build();
        assertThat(descriptor.organisation(), equalTo("my-org"));
        assertThat(descriptor.name(), equalTo("my-plugin"));
        assertNull(descriptor.version());

        descriptor = pluginDescriptor("my-org/my-plugin/my-version").build();
        assertThat(descriptor.organisation(), equalTo("my-org"));
        assertThat(descriptor.name(), equalTo("my-plugin"));
        assertThat(descriptor.version(), equalTo("my-version"));
    }

    @Test
    public void testPluginDescriptorBuilder() {
        PluginDescriptor.Builder builder = pluginDescriptor(null);
        builder.organisation("first-org");
        assertThat(builder.build().organisation(), equalTo("first-org"));

        builder = pluginDescriptor(null);
        builder.name("first-plugin");
        assertThat(builder.build().name(), equalTo("first-plugin"));

        builder = pluginDescriptor(null);
        builder.name("elasticsearch-first-plugin");
        assertThat(builder.build().name(), equalTo("first-plugin"));

        builder = pluginDescriptor(null);
        builder.version("first-version");
        assertThat(builder.build().version(), equalTo("first-version"));

        builder = pluginDescriptor(null);
        builder.description("first-description");
        assertThat(builder.build().description(), equalTo("first-description"));

        builder = pluginDescriptor(null).organisation("second-org").name("second-plugin").version("second-version").description("second-description");
        assertThat(builder.build().organisation(), equalTo("second-org"));
        assertThat(builder.build().name(), equalTo("second-plugin"));
        assertThat(builder.build().version(), equalTo("second-version"));
        assertThat(builder.build().description(), equalTo("second-description"));

        PluginDescriptor descriptor = pluginDescriptor("plugin-with-deps").dependency("dep-a").build();
        assertNotNull(descriptor.dependencies());
        assertThat(descriptor.dependencies().size(), equalTo(1));
        assertThat(descriptor.dependencies(), contains(pluginDescriptor("dep-a").build()));

        descriptor = pluginDescriptor("plugin-with-deps")
                .dependency("dep-a")
                .dependency("org-b/dep-b")
                .dependency("org-c/dep-c/vers-c")
                .build();
        assertNotNull(descriptor.dependencies());
        assertThat(descriptor.dependencies().size(), equalTo(3));

        PluginDescriptor depA = pluginDescriptor(null).name("dep-a").build();
        PluginDescriptor depB = pluginDescriptor("dep-b").organisation("org-b").build();
        PluginDescriptor depC = pluginDescriptor(null).organisation("org-c").name("dep-c").version("vers-c").build();

        assertThat(descriptor.dependencies(), containsInAnyOrder(depC, depB, depA));

        descriptor = pluginDescriptor("plugin-with-deps")
                .dependency(pluginDescriptor("dep-a").build())
                .dependency(pluginDescriptor("org-b/dep-b").build())
                .build();
        assertNotNull(descriptor.dependencies());
        assertThat(descriptor.dependencies().size(), equalTo(2));

        depA = pluginDescriptor(null).name("dep-a").build();
        depB = pluginDescriptor("dep-b").organisation("org-b").build();

        assertThat(descriptor.dependencies(), containsInAnyOrder(depB, depA));
    }

    @Test
    public void testLoadFromProperties() throws IOException {
        String props = "plugin=org.elasticsearch.plugin.foo.FooPlugin" + System.lineSeparator()
                        + "version=1.2.3" + System.lineSeparator()
                        + "description=This is plugin Foo" + System.lineSeparator();

        PluginDescriptor descriptor = null;
        try (InputStream is = new ByteArrayInputStream(props.getBytes(Charsets.UTF_8))) {
            descriptor = pluginDescriptor(null).loadFromProperties(is).build();
        }

        assertNotNull(descriptor);
        assertThat(descriptor.version(), equalTo("1.2.3"));
        assertThat(descriptor.description(), equalTo("This is plugin Foo"));
    }

    @Test
    public void testLoadFromMeta() {
        Settings settings = ImmutableSettings.builder().loadFromSource("organisation: elasticsearch\n" +
                        "name: awesome-plugin\n" +
                        "version: 1.0.0\n" +
                        "description: Awesome plugin for elasticsearch\n" +
                        "\n" +
                        "dependencies:\n" +
                        "    - organisation: elasticsearch\n" +
                        "      name: analysis-icu\n" +
                        "      version: 2.4.2\n" +
                        "\n" +
                        "    - organisation: elasticsearch\n" +
                        "      name: cloud-azure\n" +
                        "      version: 1.3.0\n"

        ).build();

        PluginDescriptor descriptor = pluginDescriptor(null).loadFromMeta(settings).build();
        assertNotNull(descriptor);
        assertThat(descriptor.organisation(), equalTo("elasticsearch"));
        assertThat(descriptor.name(), equalTo("awesome-plugin"));
        assertThat(descriptor.version(), equalTo("1.0.0"));
        assertThat(descriptor.description(), equalTo("Awesome plugin for elasticsearch"));
        assertNotNull(descriptor.dependencies());
        assertThat(descriptor.dependencies().size(), equalTo(2));

        PluginDescriptor dep1 = pluginDescriptor("elasticsearch/analysis-icu/2.4.2").build();
        PluginDescriptor dep2 = pluginDescriptor("elasticsearch/cloud-azure/1.3.0").build();

        assertThat(descriptor.dependencies(), containsInAnyOrder(dep1, dep2));
    }

    @Test
    public void testLoadFromZip() throws IOException {

        // Creates a fake zipped plugin with the following content:
        //
        //  |
        //  |-- _site
        //  |     +-- es-plugin.properties (contains description)
        //  +-- elasticsearch-zipped-plugin-2.3.4.jar
        //  |     +-- meta.yml
        //  +-- es-plugin.properties (contains version)
        //
        Path zipped = newTempFilePath();
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(zipped))) {

            ZipEntry site = new ZipEntry("_site/");
            zip.putNextEntry(site);

            ZipEntry pluginTestSiteEsPlugin = new ZipEntry("_site/es-plugin.properties");
            zip.putNextEntry(pluginTestSiteEsPlugin);
            String esPlugin = "description=This is a zipped plugin" + System.lineSeparator();
            zip.write(esPlugin.getBytes(Charsets.UTF_8));

            ZipEntry pluginTestJar = new ZipEntry("elasticsearch-zipped-plugin-2.3.4.jar");
            zip.putNextEntry(pluginTestJar);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (ZipOutputStream jar = new ZipOutputStream(out)) {
                ZipEntry metaFile = new ZipEntry("meta.yml");
                jar.putNextEntry(metaFile);
                String meta = "organisation: elasticsearch\n" +
                                "name: zipped-plugin\n" +
                                "\n" +
                                "dependencies:\n" +
                                "    - organisation: elasticsearch\n" +
                                "      name: analysis-icu\n" +
                                "      version: 1.3.0\n";

                jar.write(meta.getBytes(Charsets.UTF_8));
                jar.closeEntry();
            }
            zip.write(out.toByteArray());

            ZipEntry pluginTestEsPlugin = new ZipEntry("es-plugin.properties");
            zip.putNextEntry(pluginTestEsPlugin);
            esPlugin = "version=2.3.4" + System.lineSeparator();
            zip.write(esPlugin.getBytes(Charsets.UTF_8));
        }


        PluginDescriptor descriptor = pluginDescriptor(null).loadFromZip(zipped).build();
        assertNotNull(descriptor);
        assertThat(descriptor.organisation(), equalTo("elasticsearch"));
        assertThat(descriptor.name(), equalTo("zipped-plugin"));
        assertThat(descriptor.version(), equalTo("2.3.4"));
        assertThat(descriptor.description(), equalTo("This is a zipped plugin"));
        assertNotNull(descriptor.dependencies());
        assertThat(descriptor.dependencies().size(), equalTo(1));
        assertThat(descriptor.dependencies(), hasItem(pluginDescriptor("elasticsearch/analysis-icu/1.3.0").build()));
    }
}
