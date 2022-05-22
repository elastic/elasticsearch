/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.ByteArrayInputStream;
import java.lang.module.InvalidModuleDescriptorException;
import java.lang.module.ModuleDescriptor.Version;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.jar.Manifest;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.exportsOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.opensOf;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class EmbeddedModulePathTests extends ESTestCase {

    static final Class<IllegalArgumentException> IAE = IllegalArgumentException.class;

    public void testVersion() {
        Optional<Version> over;
        over = EmbeddedModulePath.version("foo.jar");
        assertThat(over, isEmpty());

        over = EmbeddedModulePath.version("foo-1.2.jar");
        assertThat(over, isPresent());
        assertThat(over.get(), is(Version.parse("1.2")));

        over = EmbeddedModulePath.version("foo-bar-1.2.3-SNAPSHOT.jar");
        assertThat(over, isPresent());
        assertThat(over.get(), is(Version.parse("1.2.3-SNAPSHOT")));

        over = EmbeddedModulePath.version("elasticsearch-8.3.0-SNAPSHOT.jar");
        assertThat(over, isPresent());
        assertThat(over.get(), is(Version.parse("8.3.0-SNAPSHOT")));

        expectThrows(IAE, () -> EmbeddedModulePath.version(""));
        expectThrows(IAE, () -> EmbeddedModulePath.version("foo"));
        expectThrows(IAE, () -> EmbeddedModulePath.version("foo."));
        expectThrows(IAE, () -> EmbeddedModulePath.version("foo.ja"));
    }

    public void testExplicitModuleDescriptorForEmbeddedJar() throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("module-info", "module m { exports p;  opens q; }");
        sources.put("p.Foo", "package p; public class Foo extends q.Bar { }");
        sources.put("q.Bar", "package q; public class Bar { }");
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("/a/b/m.jar/module-info.class", classToBytes.get("module-info"));
        jarEntries.put("/a/b/m.jar/p/Foo.class", classToBytes.get("p.Foo"));
        jarEntries.put("/a/b/m.jar/q/Bar.class", classToBytes.get("q.Bar"));
        jarEntries.put("/a/b/m.jar/r/R.class", "<empty>".getBytes(UTF_8));

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(topLevelDir.resolve("impl.jar"), jarEntries);

        try (FileSystem fileSystem = FileSystems.newFileSystem(outerJar, Map.of(), EmbeddedModulePathTests.class.getClassLoader())) {
            Path mRoot = fileSystem.getPath("/a/b/m.jar");
            var md = EmbeddedModulePath.descriptorFor(mRoot);
            assertThat(md.isAutomatic(), is(false));
            assertThat(md.name(), is("m"));
            assertThat(md.exports(), containsInAnyOrder(exportsOf("p")));
            assertThat(md.opens(), containsInAnyOrder(opensOf("q")));
            assertThat(md.packages(), containsInAnyOrder(is("p"), is("q"), is("r")));
        }
    }

    static final Class<InvalidModuleDescriptorException> IMDE = InvalidModuleDescriptorException.class;

    public void testToPackageNamePath() {
        String separator = PathUtils.get("foo").getFileSystem().getSeparator();
        Path p = PathUtils.get("a").resolve("b").resolve("Foo.class");
        assertThat(EmbeddedModulePath.toPackageName(p, separator).get(), is("a.b"));

        assertThat(EmbeddedModulePath.toPackageName(PathUtils.get("module-info.class"), separator), isEmpty());
        assertThat(EmbeddedModulePath.toPackageName(PathUtils.get("foo.txt"), separator), isEmpty());
        assertThat(EmbeddedModulePath.toPackageName(PathUtils.get("META-INF").resolve("MANIFEST.MF"), separator), isEmpty());

        expectThrows(IMDE, () -> EmbeddedModulePath.toPackageName(PathUtils.get("Foo.class"), separator));
    }

    public void testToPackageNameString() {
        assertThat(EmbeddedModulePath.toPackageName("a/b/Foo.class", "/").get(), is("a.b"));
        assertThat(EmbeddedModulePath.toPackageName("a/b/c/Foo$1.class", "/").get(), is("a.b.c"));
        assertThat(EmbeddedModulePath.toPackageName("a/b/c/d/Foo$Bar.class", "/").get(), is("a.b.c.d"));

        assertThat(EmbeddedModulePath.toPackageName("module-info.class", "/"), isEmpty());
        assertThat(EmbeddedModulePath.toPackageName("foo.txt", "/"), isEmpty());
        assertThat(EmbeddedModulePath.toPackageName("META-INF/MANIFEST.MF", "/"), isEmpty());
        assertThat(EmbeddedModulePath.toPackageName("a/b/c/1d/Foo$Bar.class", "/"), isEmpty());

        expectThrows(IMDE, () -> EmbeddedModulePath.toPackageName("Foo.class", "/"));
    }

    public void testScanBasic() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJar(topLevelDir, "impl.jar", null, "module-info.class", "p/Foo.class", "q/Bar.class", "META-INF/services/a.b.c.Foo");

        try (FileSystem zipFileSystem = FileSystems.newFileSystem(outerJar, Map.of(), EmbeddedModulePathTests.class.getClassLoader())) {
            Path jarRoot = zipFileSystem.getPath("/");
            EmbeddedModulePath.ScanResult scanResult = EmbeddedModulePath.scan(jarRoot);
            assertThat(
                scanResult.classFiles(),
                containsInAnyOrder(equalTo("module-info.class"), equalTo("p/Foo.class"), equalTo("q/Bar.class"))
            );
            assertThat(scanResult.serviceFiles(), contains(equalTo("META-INF/services/a.b.c.Foo")));
        }
    }

    public void testExplodedPackages() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jarPath = JarUtils.createJar(
            topLevelDir,
            "impl.jar",
            null,
            "META-INF/MANIFEST.MF",
            "module-info.class",
            "a/b/c/Foo.class",
            "d/e/f/Bar.class",
            "g/h/i/Baz.class",
            "g/h/i/res1.txt",
            "x/y/z/res2.txt",
            "res3.txt"
        );
        try (FileSystem zipFileSystem = FileSystems.newFileSystem(jarPath, Map.of(), EmbeddedModulePathTests.class.getClassLoader())) {
            Path jarRoot = zipFileSystem.getPath("/");
            Set<String> pkgs = EmbeddedModulePath.explodedPackages(jarRoot);
            assertThat(pkgs, containsInAnyOrder("a.b.c", "d.e.f", "g.h.i", "x.y.z"));
        }
    }

    public void testExplodedPackagesMultiRelease() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Manifest manifest = new Manifest(new ByteArrayInputStream("Multi-Release: true\n".getBytes(UTF_8)));
        Path jarPath = JarUtils.createJar(
            topLevelDir,
            "impl.jar",
            manifest,
            "META-INF/versions/9/module-info.class",
            "META-INF/versions/11/a/b/c/Foo.class",
            "META-INF/versions/12/d/e/f/Bar.class",
            "META-INF/versions/17/g/h/i/Baz.class",
            "g/h/i/res1.txt",
            "/x/y/z/res2.txt",
            "res3.txt"
        );
        try (FileSystem zipFileSystem = FileSystems.newFileSystem(jarPath, Map.of(), EmbeddedModulePathTests.class.getClassLoader())) {
            Path jarRoot = zipFileSystem.getPath("/");
            Set<String> pkgs = EmbeddedModulePath.explodedPackages(jarRoot);
            assertThat(pkgs, containsInAnyOrder("a.b.c", "d.e.f", "g.h.i", "x.y.z"));
        }
    }

    public void testServicesBasic() throws Exception {
        Map<String, String> jarEntries = new HashMap<>();
        jarEntries.put("/META-INF/services/a.b.c.Foo", """
            # service implementation of Foo
            #
             #

            d.e.f.FooImpl # impl of Foo

            """);

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntriesUTF(topLevelDir.resolve("impl.jar"), jarEntries);

        try (FileSystem zipFileSystem = FileSystems.newFileSystem(outerJar, Map.of(), EmbeddedModulePathTests.class.getClassLoader())) {
            Path jarRoot = zipFileSystem.getPath("/");

            Set<String> serviceFiles = Set.of("META-INF/services/a.b.c.Foo");
            Map<String, List<String>> services = EmbeddedModulePath.services(serviceFiles, jarRoot);
            assertThat(services, is(aMapWithSize(1)));
            assertThat(services, hasEntry(is("a.b.c.Foo"), hasItem("d.e.f.FooImpl")));
        }
    }

    public void testServicesMultiple() throws Exception {
        Map<String, String> jarEntries = new HashMap<>();
        jarEntries.put("/META-INF/services/a.b.c.Foo", """
            # service implementation of Foo
            d.e.f.FooImpl # impl of Foo
            """);
        jarEntries.put("/META-INF/services/foo.bar.Baz", """
            # service implementation of Baz
            # impl of Baz
            foo.bar.internal.BazImpl
            """);

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntriesUTF(topLevelDir.resolve("impl.jar"), jarEntries);

        try (FileSystem zipFileSystem = FileSystems.newFileSystem(outerJar, Map.of(), EmbeddedModulePathTests.class.getClassLoader())) {
            Path jarRoot = zipFileSystem.getPath("/");

            Set<String> serviceFiles = Set.of("META-INF/services/a.b.c.Foo", "META-INF/services/foo.bar.Baz");
            Map<String, List<String>> services = EmbeddedModulePath.services(serviceFiles, jarRoot);
            assertThat(services, is(aMapWithSize(2)));
            assertThat(services, hasEntry(is("a.b.c.Foo"), hasItem("d.e.f.FooImpl")));
            assertThat(services, hasEntry(is("foo.bar.Baz"), hasItem("foo.bar.internal.BazImpl")));
        }
    }

    public void testToServiceName() {
        assertThat(EmbeddedModulePath.toServiceName("META-INF/services/a.b.Foo").get(), is("a.b.Foo"));
        assertThat(EmbeddedModulePath.toServiceName("META-INF/services/a.b.Foo$1").get(), is("a.b.Foo$1"));
        assertThat(EmbeddedModulePath.toServiceName("META-INF/services/Bar$Foo").get(), is("Bar$Foo"));

        assertThat(EmbeddedModulePath.toServiceName("META-INF/services/a.1b.c"), isEmpty());
        assertThat(EmbeddedModulePath.toServiceName("META-INF/services/"), isEmpty());

        expectThrows(IAE, () -> EmbeddedModulePath.toServiceName("META-INF/serv"));
        expectThrows(IAE, () -> EmbeddedModulePath.toServiceName("blah"));
    }

    public void testIsJavaIdentifier() {
        assertThat(EmbeddedModulePath.isJavaIdentifier("abc"), is(true));
        assertThat(EmbeddedModulePath.isJavaIdentifier("_abc"), is(true));
        assertThat(EmbeddedModulePath.isJavaIdentifier("$abc"), is(true));
        assertThat(EmbeddedModulePath.isJavaIdentifier("a1b2c"), is(true));
        assertThat(EmbeddedModulePath.isJavaIdentifier("ab$c"), is(true));
        assertThat(EmbeddedModulePath.isJavaIdentifier("a_b_c"), is(true));
        assertThat(EmbeddedModulePath.isJavaIdentifier("a1_$b2_c"), is(true));

        assertThat(EmbeddedModulePath.isJavaIdentifier("a.b.c"), is(false));
        assertThat(EmbeddedModulePath.isJavaIdentifier("1abc"), is(false));
        assertThat(EmbeddedModulePath.isJavaIdentifier(" abc"), is(false));
    }

    public void testIsTypeName() {
        assertThat(EmbeddedModulePath.isTypeName("abc"), is(true));
        assertThat(EmbeddedModulePath.isTypeName("a.b.C"), is(true));
        assertThat(EmbeddedModulePath.isTypeName("a.b.C$D"), is(true));

        assertThat(EmbeddedModulePath.isTypeName("a.b.1C$D"), is(false));
        assertThat(EmbeddedModulePath.isTypeName("a. b.C$D"), is(false));
    }

    public void testIsClassName() {
        assertThat(EmbeddedModulePath.isClassName("Foo"), is(true));
        assertThat(EmbeddedModulePath.isClassName("FooBar"), is(true));
        assertThat(EmbeddedModulePath.isClassName("FooBar$Baz"), is(true));
        assertThat(EmbeddedModulePath.isClassName("p.Foo"), is(true));
        assertThat(EmbeddedModulePath.isClassName("a.b.c.FooBar"), is(true));
        assertThat(EmbeddedModulePath.isClassName("x.y.z.FooBar$Baz"), is(true));

        assertThat(EmbeddedModulePath.isClassName("a.b.1C$D"), is(false));
        assertThat(EmbeddedModulePath.isClassName("a. b.C$D"), is(false));
    }

    public void testIsPackageName() {
        assertThat(EmbeddedModulePath.isPackageName("a.b.c"), is(true));
        assertThat(EmbeddedModulePath.isPackageName("foo.bar.baz"), is(true));

        assertThat(EmbeddedModulePath.isPackageName("a.b.1.c"), is(false));
        assertThat(EmbeddedModulePath.isPackageName("a.b. .c"), is(false));
        assertThat(EmbeddedModulePath.isPackageName("a-b.c"), is(false));
    }

    public void testModuleNameFromManifestOrNull() throws Exception {
        Path dir = createTempDir(getTestName());
        Files.createDirectories(dir.resolve("META-INF"));
        Path manifest = dir.resolve("META-INF").resolve("MANIFEST.MF");

        Files.writeString(manifest, """
            Automatic-Module-Name: bar
            """);
        String mn = EmbeddedModulePath.moduleNameFromManifestOrNull(dir);
        assertThat(mn, is("bar"));

        Files.writeString(manifest, """
            """);
        mn = EmbeddedModulePath.moduleNameFromManifestOrNull(dir);
        assertThat(mn, nullValue());

        Files.writeString(manifest, """
            Manifest-Version: 1.0
            Module-Origin: https://github.com/ChrisHegarty/elasticsearch.git
            """);
        mn = EmbeddedModulePath.moduleNameFromManifestOrNull(dir);
        assertThat(mn, nullValue());

        Files.writeString(manifest, """
            Manifest-Version: 1.0
            Module-Origin: https://github.com/ChrisHegarty/elasticsearch.git
            Automatic-Module-Name: foo.bar
            """);
        mn = EmbeddedModulePath.moduleNameFromManifestOrNull(dir);
        assertThat(mn, is("foo.bar"));
    }
}
