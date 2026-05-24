/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.plugins;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class ExtensionLoaderTests extends ESTestCase {
    public interface TestService {
        int getValue();
    }

    private URLClassLoader buildProviderJar(Map<String, CharSequence> sources) throws Exception {
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        for (var entry : sources.entrySet()) {
            var classname = entry.getKey();
            var filename = classname.replace(".", "/") + ".class";
            jarEntries.put(filename, classToBytes.get(classname));
        }
        String serviceFile = String.join("\n", sources.keySet());
        jarEntries.put(
            "META-INF/services/org.elasticsearch.plugins.ExtensionLoaderTests$TestService",
            serviceFile.getBytes(StandardCharsets.UTF_8)
        );

        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("provider.jar");
        JarUtils.createJarWithEntries(jar, jarEntries);
        URL[] urls = new URL[] { jar.toUri().toURL() };

        return URLClassLoader.newInstance(urls, this.getClass().getClassLoader());
    }

    private String defineProvider(String name, int value) {
        return String.format(Locale.ROOT, """
            package p;
            import org.elasticsearch.plugins.ExtensionLoaderTests.TestService;
            public class %s implements TestService {
                @Override
                public int getValue() {
                    return %d;
                }
            }
            """, name, value);
    }

    public void testNoProvider() {
        Optional<TestService> service = ExtensionLoader.loadSingleton(ServiceLoader.load(TestService.class));
        assertThat(service, isEmpty());
    }

    public void testOneProvider() throws Exception {
        Map<String, CharSequence> sources = Map.of("p.FooService", defineProvider("FooService", 1));
        try (var loader = buildProviderJar(sources)) {
            TestService service = ExtensionLoader.loadSingleton(ServiceLoader.load(TestService.class, loader))
                .orElseThrow(AssertionError::new);
            assertThat(service, not(nullValue()));
            assertThat(service.getValue(), equalTo(1));
        }
    }

    public void testManyProviders() throws Exception {
        Map<String, CharSequence> sources = Map.of(
            "p.FooService",
            defineProvider("FooService", 1),
            "p.BarService",
            defineProvider("BarService", 2)
        );
        try (var loader = buildProviderJar(sources)) {
            var e = expectThrows(
                IllegalStateException.class,
                () -> ExtensionLoader.loadSingleton(ServiceLoader.load(TestService.class, loader))
            );
            assertThat(e.getMessage(), containsString("More than one extension found"));
            assertThat(e.getMessage(), containsString("TestService"));
        }
    }
}
