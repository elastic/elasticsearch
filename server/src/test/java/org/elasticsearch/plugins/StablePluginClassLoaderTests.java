/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * How do we test a classloader?
 *
 * Prior art:
 * {/@link org.elasticsearch.core.internal.provider.EmbeddedImplClassLoaderTests}
 *   - creates jars for tests
 */
public class StablePluginClassLoaderTests extends ESTestCase {

    // This test is just to see that there's a jar with a compiled class in it
    // TODO: remove
    public void testJarWithURLClassLoader() throws Exception {

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("modular.jar");
        boolean modular = randomBoolean();
        createJar(outerJar);

        // loading it with a URL classloader (just checking the jar, remove
        // this block)
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, StablePluginClassLoaderTests.class.getClassLoader());
        try {
            PrivilegedAction<URLClassLoader> pa = () -> URLClassLoader.newInstance(urls, parent);
            URLClassLoader loader = AccessController.doPrivileged(pa);
            Class<?> c = loader.loadClass("p.Modular");
            Object instance = c.getConstructor().newInstance();
            assertThat(instance.toString(), equalTo(modular ? "Modular" : "NonModular"));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    // We should be able to pass a URI for the jar and load a class from it.
    public void testLoadFromModularizedJar() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("modular.jar");
        createJar(jar);

        // load it with a module -- we don't actually want to do this, remove
        ModuleFinder moduleFinder = ModuleFinder.of(jar);
        ModuleLayer mparent = ModuleLayer.boot();
        Configuration cf = mparent.configuration().resolve(moduleFinder, ModuleFinder.of(), Set.of("p"));
        var resolvedModule = cf.findModule("p").orElseThrow();
        // we have the module, but how do we load the class?

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            resolvedModule.reference()
        );

        URL location = loader.findResource("p/Modular.class");
        assertThat(location, notNullValue());
        Class<?> c = loader.loadClass("p.Modular");
        assertThat(c, notNullValue());
        Object instance = c.getConstructor().newInstance();
        assertThat("Modular", equalTo(instance.toString()));
    }

    public void testLoadFromNonModularizedJar() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("non-modular.jar");
        createJar(jar);

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoader.class.getClassLoader(),
            jar
        );

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> loader.findResource("p/NonModular" +
            ".class"));

        assertThat(e, notNullValue());
        /*
        URL location = loader.findResource("p/Modular.class");
        assertThat(location, notNullValue());
        Class<?> c = loader.loadClass("p.Modular");
        assertThat(c, notNullValue());
        Object instance = c.getConstructor().newInstance();
        assertThat("Modular", equalTo(instance.toString()));
        */
    }

    private static void createJar(Path outerJar) throws IOException {
        String name = "NonModular";
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + name, String.format(Locale.ENGLISH, """
            package p;
            public class %s {
                @Override
                public String toString() {
                    return "%s";
                }
            }
            """, name, name));
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/Modular.class", classToBytes.get("p.Modular"));
        JarUtils.createJarWithEntries(outerJar, jarEntries);
    }

    // test that we don't use parent-first delegation (load from package if module has it)
}
