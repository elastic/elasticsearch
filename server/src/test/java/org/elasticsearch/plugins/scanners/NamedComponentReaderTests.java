/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins.scanners;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class NamedComponentReaderTests extends ESTestCase {
    ExtensiblesRegistry extensiblesRegistry = new ExtensiblesRegistry("/test_extensible.json");
    NamedComponentReader namedComponentReader = new NamedComponentReader(extensiblesRegistry);

    @SuppressForbidden(reason = "test resource")
    public void testReadNamedComponentsFromFile() throws IOException {
        final String resource = this.getClass().getClassLoader().getResource("named_components.json").getPath();
        Path namedComponentPath = PathUtils.get(resource);

        Map<String, NameToPluginInfo> namedComponents = namedComponentReader.readFromFile(
            namedComponentPath,
            NamedComponentReaderTests.class.getClassLoader()
        );

        assertThat(
            namedComponents.get("org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface")
                .getForPluginName("test_named_component"),
            equalTo(
                new PluginInfo(
                    "test_named_component",
                    "org.elasticsearch.plugins.scanners.named_components_test_classes.TestNamedComponent",
                    NamedComponentReaderTests.class.getClassLoader()
                )
            )
        );
    }

    public void testUnknownExtensible() throws IOException {
        final Path tmp = createTempDir();
        final Path pluginDir = tmp.resolve("plugin-dir");
        Files.createDirectories(pluginDir);
        Path namedComponentFile = pluginDir.resolve("named_components.json");
        Files.writeString(namedComponentFile, """
            {
              "org.elasticsearch.plugins.scanners.extensible_test_classes.UnknownExtensible": {
                "a_component": "p.A",
                "b_component": "p.B"
              }
            }
            """);

        ClassLoader classLoader = NamedComponentReaderTests.class.getClassLoader();
        expectThrows(IllegalStateException.class, () -> namedComponentReader.findNamedComponents(pluginDir, classLoader));
    }

    public void testFindNamedComponentInJarWithNamedComponentscacheFile() throws IOException {
        final Path tmp = createTempDir();
        final Path pluginDir = tmp.resolve("plugin-dir");
        Files.createDirectories(pluginDir);
        Path namedComponentFile = pluginDir.resolve("named_components.json");
        Files.writeString(namedComponentFile, """
            {
              "org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface": {
                "a_component": "p.A",
                "b_component": "p.B"
              }
            }
            """);

        ClassLoader classLoader = NamedComponentReaderTests.class.getClassLoader();
        Map<String, NameToPluginInfo> namedComponents = namedComponentReader.findNamedComponents(pluginDir, classLoader);

        assertThat(
            namedComponents.get("org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface")
                .getForPluginName("b_component"),
            equalTo(new PluginInfo("b_component", "p.B", classLoader))
        );
        assertThat(
            namedComponents.get("org.elasticsearch.plugins.scanners.extensible_test_classes.ExtensibleInterface")
                .getForPluginName("a_component"),
            equalTo(new PluginInfo("a_component", "p.A", classLoader))
        );
    }

    private URL toURL(Path p) {
        try {
            return p.toUri().toURL();
        } catch (MalformedURLException e) {
            throw new RuntimeException(e);
        }
    }
}
