/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.jar;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.test.jar.JarUtils.createJar;
import static org.elasticsearch.test.jar.JarUtils.createJarWithEntries;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@SuppressForbidden(reason = "requires access to JarFile to assert jar contents")
public class JarUtilsTests extends ESTestCase {

    public void testCreateJarWithContents() throws IOException {
        Path dir = createTempDir();

        Map<String, byte[]> jarEntries = Map.of(
            "a/b/c/hello.txt",
            "hello ".getBytes(UTF_8),
            "a/b/c/d/world.txt",
            "world!".getBytes(UTF_8),
            "META-INF/MANIFEST.MF",
            "Multi-Release: true\n".getBytes(UTF_8)
        );
        Path jar = dir.resolve("foo.jar");
        createJarWithEntries(jar, jarEntries);

        try (JarFile jarFile = new JarFile(jar.toFile())) {
            Map<String, byte[]> entries = jarFile.stream().collect(Collectors.toMap(JarEntry::getName, e -> entryContents(e, jarFile)));
            assertThat(
                entries,
                allOf(
                    hasEntry(is("a/b/c/hello.txt"), is("hello ".getBytes(UTF_8))),
                    hasEntry(is("a/b/c/d/world.txt"), is("world!".getBytes(UTF_8)))
                )
            );
            assertThat(jarFile.getManifest().getMainAttributes().getValue("Multi-Release"), equalTo("true"));
        }
    }

    public void testCreateJarWithoutContents() throws IOException {
        Path dir = createTempDir();
        Manifest manifest = new Manifest(new ByteArrayInputStream("""
            Manifest-Version: 1.0
            Automatic-Module-Name: foo.bar
            """.getBytes(UTF_8)));
        Path jarPath = createJar(
            dir,
            "impl.jar",
            manifest,
            "module-info.class",
            "p/Foo.class",
            "q/Bar.class",
            "META-INF/services/a.b.c.Foo"
        );
        try (JarFile jarFile = new JarFile(jarPath.toFile())) {
            Set<String> entries = jarFile.stream().map(JarEntry::getName).collect(Collectors.toSet());
            assertThat(
                entries,
                containsInAnyOrder(
                    is("module-info.class"),
                    is("p/Foo.class"),
                    is("q/Bar.class"),
                    is("META-INF/services/a.b.c.Foo"),
                    is("META-INF/MANIFEST.MF")
                )
            );
            assertThat(jarFile.getManifest().getMainAttributes().getValue("Manifest-Version"), equalTo("1.0"));
            assertThat(jarFile.getManifest().getMainAttributes().getValue("Automatic-Module-Name"), equalTo("foo.bar"));
        }
    }

    public void testCreateJarNoManifest() throws IOException {
        Path dir = createTempDir();
        Path jarPath = createJar(dir, "bar.jar", null, "q/Bar.class");
        try (JarFile jarFile = new JarFile(jarPath.toFile())) {
            Set<String> entries = jarFile.stream().map(JarEntry::getName).collect(Collectors.toSet());
            assertThat(entries, containsInAnyOrder(is("q/Bar.class")));
            assertThat(jarFile.getManifest(), nullValue());
        }
    }

    private static byte[] entryContents(JarEntry je, JarFile jf) {
        try {
            return jf.getInputStream(je).readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
