/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;

import java.lang.module.ModuleDescriptor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.basePrefix;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.exportsOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.opensOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class EmbeddedImplClassLoaderTests extends ESTestCase {

    public void testBasePrefix() {
        assertThat(basePrefix("IMPL-JARS/x-content/jackson-core-2.13.2.jar"), equalTo("IMPL-JARS/x-content/jackson-core-2.13.2.jar"));
        assertThat(
            basePrefix("IMPL-JARS/x-content/jackson-core-2.13.2.jar/META-INF/versions/9"),
            equalTo("IMPL-JARS/x-content/jackson-core-2.13.2.jar")
        );
        assertThat(
            basePrefix("IMPL-JARS/x-content/jackson-core-2.13.2.jar/META-INF/versions/100"),
            equalTo("IMPL-JARS/x-content/jackson-core-2.13.2.jar")
        );
    }

    public void testLoadAsModuleEmbeddedJar() throws Exception {
        // test scenario setup, compile source, create jar file, and parent loader
        Map<String, CharSequence> sources = Map.of(
            "module-info",
            "module x.foo.impl { exports p; opens q; provides java.util.function.Supplier with p.FooSupplier; }",
            "p.FooSupplier",
            "package p; public class FooSupplier implements java.util.function.Supplier<String> {\n" +
                "        @Override public String get() { return \"Hello from FooSupplier!\"; } }",
            "q.Bar",
            "package q; public class Bar { }"
        );
        var classToBytes = InMemoryJavaCompiler.compile(sources);
        Path topLevelDir = createTempDir();

        Map<String, byte[]> jarEntries = Map.of(
            "IMPL-JARS/x-foo/LISTING.TXT",
            "x-foo-impl.jar".getBytes(UTF_8),
            "IMPL-JARS/x-foo/x-foo-impl.jar/module-info.class",
            classToBytes.get("module-info"),
            "IMPL-JARS/x-foo/x-foo-impl.jar/p/FooSupplier.class",
            classToBytes.get("p.FooSupplier"),
            "IMPL-JARS/x-foo/x-foo-impl.jar/q/Bar.class",
            classToBytes.get("q.Bar"),
            "IMPL-JARS/x-foo/x-foo-impl.jar/r/R.class",
            "<empty>".getBytes(UTF_8)
        );
        Path outerJar = topLevelDir.resolve("x-foo-impl.jar");
        JarUtils.createJarFile(topLevelDir.resolve("x-foo-impl.jar"), jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(new URL[] {outerJar.toUri().toURL()}, EmbeddedImplClassLoaderTests.class.getClassLoader());

        // test scenario
        ProviderLocator<Supplier<String>> locator =
            new ProviderLocator(this.getClass().getModule(), "x-foo", Supplier.class, parent, "x.foo.impl", Set.of(), true);
        Supplier<String> impl = locator.get();
        String msg = impl.get();
        assertThat(msg, equalTo("Hello from FooSupplier!"));

        Class<?> imlpClass = impl.getClass();
        assertThat(imlpClass.getName(), equalTo("p.FooSupplier"));

        Module implMod = imlpClass.getModule();
        assertThat(implMod.getName(), equalTo("x.foo.impl"));

        ModuleDescriptor md = implMod.getDescriptor();
        assertThat(md.isAutomatic(), equalTo(false));
        assertThat(md.name(), equalTo("x.foo.impl"));
        assertThat(md.exports(), containsInAnyOrder(exportsOf("p")));
        assertThat(md.opens(), containsInAnyOrder(opensOf("q")));
        assertThat(md.packages(), containsInAnyOrder(equalTo("p"), equalTo("q"), equalTo("r")));
    }

    // variant of testLoadAsModuleEmbeddedJar, but as a non-module
    public void testLoadAsNonModuleEmbeddedJar() throws Exception {
        // test scenario setup, compile source, create jar file, and parent loader
        Map<String, CharSequence> sources = Map.of(
            "p.FooSupplier",
            "package p; public class FooSupplier implements java.util.function.Supplier<String> {\n" +
                "        @Override public String get() { return \"Hello from FooSupplier - non-modular!\"; } }"
        );
        var classToBytes = InMemoryJavaCompiler.compile(sources);
        Path topLevelDir = createTempDir();

        Map<String, byte[]> jarEntries = Map.of(
            "IMPL-JARS/x-foo/LISTING.TXT",
            "x-foo-nm-impl.jar".getBytes(UTF_8),
            "META-INF/services/java.util.function.Supplier",
            "p.FooSupplier".getBytes(UTF_8),
            "IMPL-JARS/x-foo/x-foo-nm-impl.jar/p/FooSupplier.class",
            classToBytes.get("p.FooSupplier")
        );
        Path outerJar = topLevelDir.resolve("x-foo-nm-impl.jar");
        JarUtils.createJarFile(topLevelDir.resolve("x-foo-nm-impl.jar"), jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(new URL[] {outerJar.toUri().toURL()}, EmbeddedImplClassLoaderTests.class.getClassLoader());

        // test scenario
        ProviderLocator<Supplier<String>> locator =
            new ProviderLocator(this.getClass().getModule(), "x-foo", Supplier.class, parent, "", Set.of(), false);
        Supplier<String> impl = locator.get();
        String msg = impl.get();
        assertThat(msg, equalTo("Hello from FooSupplier - non-modular!"));
        assertThat(impl.getClass().getName(), equalTo("p.FooSupplier"));
        assertThat(impl.getClass().getModule().isNamed(), is(false));
    }

    // variant of testLoadAsModuleEmbeddedJar, but exploded file paths
    public void testLoadAsNonModuleExplodedPath() throws Exception {
        // test scenario setup, compile source, create jar file, and parent loader
        Map<String, CharSequence> sources = Map.of(
            "pb.BarSupplier",
            """
                package pb;
                public class BarSupplier implements java.util.function.Supplier<String> {
                  @Override public String get() {
                    return "Hello from BarSupplier - exploded non-modular!";
                  }
                }
                """
        );
        var classToBytes = InMemoryJavaCompiler.compile(sources);
        Path topLevelDir = createTempDir();

        Path yBar = Files.createDirectories(topLevelDir.resolve("IMPL-JARS").resolve("y-bar"));
        Files.writeString(yBar.resolve("LISTING.TXT"), "y-bar-nm-impl.jar");
        Path barRoot = Files.createDirectories(yBar.resolve("y-bar-nm-impl.jar"));
        Path sr = Files.createDirectories(barRoot.resolve("META-INF").resolve("services"));
        Files.writeString(sr.resolve("java.util.function.Supplier"), "pb.BarSupplier");
        Path pb = Files.createDirectories(barRoot.resolve("pb"));
        Files.write(pb.resolve("BarSupplier.class"), classToBytes.get("pb.BarSupplier"));

        URLClassLoader parent = URLClassLoader.newInstance(new URL[] {topLevelDir.toUri().toURL()}, EmbeddedImplClassLoaderTests.class.getClassLoader());

        // test scenario
        ProviderLocator<Supplier<String>> locator =
            new ProviderLocator(this.getClass().getModule(), "y-bar", Supplier.class, parent, "", Set.of(), false);
        Supplier<String> impl = locator.get();
        String msg = impl.get();
        assertThat(msg, equalTo("Hello from BarSupplier - exploded non-modular!"));
        assertThat(impl.getClass().getName(), equalTo("pb.BarSupplier"));
        assertThat(impl.getClass().getModule().isNamed(), is(false));
    }

    // todo, test MRJAR
}
