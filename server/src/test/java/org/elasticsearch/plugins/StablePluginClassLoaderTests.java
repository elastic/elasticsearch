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

import java.lang.module.Configuration;
import java.lang.module.ModuleFinder;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

    // Test loading a modularized jar...
    // We should be able to pass a URI for the jar and load a class from it.
    public void testLoadFromModularizedJar() throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p.Modular", """
            package p;
            public class Modular {
                @Override
                public String toString() {
                    return "Modular";
                }
            }
            """);
        sources.put("module-info", """
            module p {
              exports p;
            }
            """);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/Modular.class", classToBytes.get("p.Modular"));
        jarEntries.put("module-info.class", classToBytes.get("module-info"));
        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("test.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);

        // loading it with a URL classloader (just checking the jar, remove
        // this block)
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, StablePluginClassLoaderTests.class.getClassLoader());
        try {
            PrivilegedAction<URLClassLoader> pa = () -> URLClassLoader.newInstance(urls, parent);
            URLClassLoader loader = AccessController.doPrivileged(pa);
            Class<?> c = loader.loadClass("p.Modular");
            Object instance = c.getConstructor().newInstance();
            assertThat("Modular", equalTo(instance.toString()));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }

        // load it with a module
        ModuleFinder moduleFinder = ModuleFinder.of(outerJar);
        ModuleLayer mparent = ModuleLayer.boot();
        Configuration cf = mparent.configuration().resolve(moduleFinder, ModuleFinder.of(), Set.of("p"));
        var resolvedModule = cf.findModule("p").orElseThrow();
        // we have the module, but how do we load the class?

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(parent, resolvedModule);

        URL location = loader.findResource("p/Modular.class");
        assertThat(location, notNullValue());
        Class<?> c = loader.loadClass("p.Modular");
        assertThat(c, notNullValue());
        Object instance = c.getConstructor().newInstance();
        assertThat("Modular", equalTo(instance.toString()));
    }
}
