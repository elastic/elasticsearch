/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.lang.module.ModuleDescriptor;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.exportsOf;
import static org.elasticsearch.test.hamcrest.ModuleDescriptorMatchers.opensOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ProviderLocatorTests extends ESTestCase {

    /*
     * Tests that a modular provider is retrievable from an embedded jar implementation, with a
     * module-info in the root.
     */
    public void testExplicitModuleEmbeddedJar() throws Exception {
        testLoadAsModuleEmbeddedJarVersionSpecific(-1); // versionless - module-info in the root
    }

    /*
     * Tests that a modular provider is retrievable from an embedded jar implementation, with a
     * version specific, 9, module-info entry.
     */
    public void testLoadAsModuleEmbeddedJar9() throws Exception {
        testLoadAsModuleEmbeddedJarVersionSpecific(9); // META-INF/versions/9/module-info.class
    }

    /*
     * Tests that a modular provider is retrievable from an embedded jar implementation, with a
     * version specific, 11, module-info entry.
     */
    public void testLoadAsModuleEmbeddedJar11() throws Exception {
        testLoadAsModuleEmbeddedJarVersionSpecific(11); // META-INF/versions/11/module-info.class
    }

    /*
     * Tests that a modular provider is retrievable from an embedded jar implementation, with a
     * version specific, 17, module-info entry.
     */
    public void testLoadAsModuleEmbeddedJar17() throws Exception {
        testLoadAsModuleEmbeddedJarVersionSpecific(17); // META-INF/versions/17/module-info.class
    }

    /*
     * Tests that a modular provider is located and retrievable from an embedded jar implementation.
     * First generates and compiles the code, then packages it, and lastly gets the provider
     * implementation.
     *
     * @param version the runtime version number of the module-info entry
     */
    void testLoadAsModuleEmbeddedJarVersionSpecific(int version) throws Exception {
        // test scenario setup, compile source, create jar file, and parent loader
        Map<String, CharSequence> sources = Map.of(
            "module-info",
            "module x.foo.impl { exports p; opens q; provides java.util.function.IntSupplier with p.FooIntSupplier; }",
            "p.FooIntSupplier",
            """
                package p;
                public class FooIntSupplier implements java.util.function.IntSupplier {
                    @Override public int getAsInt() {
                        return 12;
                    }
                    @Override public String toString() {
                        return "Hello from FooIntSupplier - modular!";
                    }
                }
                """,
            "q.Bar",
            "package q; public class Bar { }"
        );
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        final String prefix = "IMPL-JARS/x-foo/x-foo-impl.jar";
        String moduleInfoPath = prefix + "/module-info.class";
        if (version >= 8) {
            moduleInfoPath = prefix + "/META-INF/versions/" + version + "/module-info.class";
        }

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/x-foo/LISTING.TXT", bytes("x-foo-impl.jar"));
        jarEntries.put(moduleInfoPath, classToBytes.get("module-info"));
        jarEntries.put(prefix + "/p/FooIntSupplier.class", classToBytes.get("p.FooIntSupplier"));
        jarEntries.put(prefix + "/q/Bar.class", classToBytes.get("q.Bar"));
        jarEntries.put(prefix + "/r/R.class", bytes("<empty>"));
        if (version >= 8) {
            // enable multi-release jar in the manifest
            jarEntries.put(prefix + "/META-INF/MANIFEST.MF", bytes("Multi-Release: true\n"));
            // locate a bad module-info in the root, to ensure not accessed
            jarEntries.put(prefix + "/module-info.class", bytes("bad"));
        }

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { outerJar.toUri().toURL() },
            ProviderLocatorTests.class.getClassLoader()
        );

        try {
            // test scenario
            ProviderLocator<IntSupplier> locator = new ProviderLocator<>("x-foo", IntSupplier.class, parent, "x.foo.impl", Set.of(), true);
            IntSupplier impl = locator.get();
            assertThat(impl.getAsInt(), is(12));
            assertThat(impl.toString(), equalTo("Hello from FooIntSupplier - modular!"));
            assertThat(impl.getClass().getName(), equalTo("p.FooIntSupplier"));

            Module implMod = impl.getClass().getModule();
            assertThat(implMod.getName(), equalTo("x.foo.impl"));

            ModuleDescriptor md = implMod.getDescriptor();
            assertThat(md.isAutomatic(), equalTo(false));
            assertThat(md.name(), equalTo("x.foo.impl"));
            assertThat(md.exports(), containsInAnyOrder(exportsOf("p")));
            assertThat(md.opens(), containsInAnyOrder(opensOf("q")));
            assertThat(md.packages(), containsInAnyOrder(equalTo("p"), equalTo("q"), equalTo("r")));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    static byte[] bytes(String str) {
        return str.getBytes(UTF_8);
    }

    /* Variant of testLoadAsModuleEmbeddedJar, but as a non-module. */
    public void testLoadAsNonModuleEmbeddedJar() throws Exception {
        // test scenario setup, compile source, create jar file, and parent loader
        Map<String, CharSequence> sources = Map.of("p.FooLongSupplier", """
            package p;
            public class FooLongSupplier implements java.util.function.LongSupplier {
              @Override public long getAsLong() {
                return 55L;
              }
              @Override public String toString() {
                return "Hello from FooLongSupplier - non-modular!";
              }
            }
            """);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/x-foo/LISTING.TXT", bytes("x-foo-nm-impl.jar"));
        jarEntries.put("IMPL-JARS/x-foo/x-foo-nm-impl.jar/META-INF/services/java.util.function.LongSupplier", bytes("p.FooLongSupplier"));
        jarEntries.put("IMPL-JARS/x-foo/x-foo-nm-impl.jar/p/FooLongSupplier.class", classToBytes.get("p.FooLongSupplier"));

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { outerJar.toUri().toURL() },
            ProviderLocatorTests.class.getClassLoader()
        );

        try {
            // test scenario
            ProviderLocator<LongSupplier> locator = new ProviderLocator<>("x-foo", LongSupplier.class, parent, "", Set.of(), false);
            LongSupplier impl = locator.get();
            assertThat(impl.getAsLong(), is(55L));
            assertThat(impl.toString(), equalTo("Hello from FooLongSupplier - non-modular!"));
            assertThat(impl.getClass().getName(), equalTo("p.FooLongSupplier"));
            assertThat(impl.getClass().getModule().isNamed(), is(false));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    /* Variant of testLoadAsModuleEmbeddedJar, but exploded file paths. */
    public void testLoadAsNonModuleExplodedPath() throws Exception {
        // test scenario setup, compile source, create jar file, and parent loader
        Map<String, CharSequence> sources = Map.of("pb.BarIntSupplier", """
            package pb;
            public class BarIntSupplier implements java.util.function.IntSupplier {
              @Override public int getAsInt() {
                return 42;
              }
              @Override public String toString() {
                return "Hello from BarIntSupplier - exploded non-modular!";
              }
            }
            """);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Path topLevelDir = createTempDir(getTestName());
        Path yBar = Files.createDirectories(topLevelDir.resolve("IMPL-JARS").resolve("y-bar"));
        Files.writeString(yBar.resolve("LISTING.TXT"), "y-bar-nm-impl.jar");
        Path barRoot = Files.createDirectories(yBar.resolve("y-bar-nm-impl.jar"));
        Path sr = Files.createDirectories(barRoot.resolve("META-INF").resolve("services"));
        Files.writeString(sr.resolve("java.util.function.IntSupplier"), "pb.BarIntSupplier");
        Path pb = Files.createDirectories(barRoot.resolve("pb"));
        Files.write(pb.resolve("BarIntSupplier.class"), classToBytes.get("pb.BarIntSupplier"));

        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { topLevelDir.toUri().toURL() },
            ProviderLocatorTests.class.getClassLoader()
        );

        try {
            // test scenario
            ProviderLocator<IntSupplier> locator = new ProviderLocator<>("y-bar", IntSupplier.class, parent, "", Set.of(), false);
            IntSupplier impl = locator.get();
            assertThat(impl.getAsInt(), is(42));
            assertThat(impl.toString(), equalTo("Hello from BarIntSupplier - exploded non-modular!"));
            assertThat(impl.getClass().getName(), equalTo("pb.BarIntSupplier"));
            assertThat(impl.getClass().getModule().isNamed(), is(false));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }
}
