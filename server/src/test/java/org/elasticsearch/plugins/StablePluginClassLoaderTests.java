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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

/**
 * How do we test a classloader?
 * <p>
 * Prior art:
 * {/@link org.elasticsearch.core.internal.provider.EmbeddedImplClassLoaderTests}
 *   - creates jars for tests
 */
@ESTestCase.WithoutSecurityManager
public class StablePluginClassLoaderTests extends ESTestCase {

    // This test is just to see that there's a jar with a compiled class in it
    // TODO: remove
    public void testJarWithURLClassLoader() throws Exception {

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("my-jar.jar");
        createJar(outerJar, "MyClass");

        // loading it with a URL classloader (just checking the jar, remove
        // this block)
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, StablePluginClassLoaderTests.class.getClassLoader());
        try {
            PrivilegedAction<URLClassLoader> pa = () -> URLClassLoader.newInstance(urls, parent);
            URLClassLoader loader = AccessController.doPrivileged(pa);
            Class<?> c = loader.loadClass("p.MyClass");
            Object instance = c.getConstructor().newInstance();
            assertThat(instance.toString(), equalTo("MyClass"));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    // lets me look inside the module system classloader to see how it works
    // TODO: remove (also remove WithoutSecurityManager annotation)
    public void testLoadWithModuleLayer() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar.jar");
        createModularJar(jar, "MyClass");

        // load it with a module
        ModuleFinder moduleFinder = ModuleFinder.of(jar);
        ModuleLayer mparent = ModuleLayer.boot();
        Configuration cf = mparent.configuration().resolve(moduleFinder, ModuleFinder.of(), Set.of("p"));
        // we have the module, but how do we load the class?

        PrivilegedAction<ClassLoader> pa =
            () -> ModuleLayer.defineModulesWithOneLoader(cf, List.of(mparent), this.getClass().getClassLoader()).layer().findLoader("p");
        ClassLoader loader = AccessController.doPrivileged(pa);
        Class<?> c = loader.loadClass("p.MyClass");
        assertThat(c, notNullValue());
        Object instance = c.getConstructor().newInstance();
        assertThat("MyClass", equalTo(instance.toString()));

    }

    // We should be able to pass a URI for the jar and load a class from it.
    public void testLoadFromJar() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar.jar");
        createJar(jar, "MyClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            jar
        );

        URL location = loader.findResource("p/MyClass.class");
        assertThat(location, notNullValue());
        Class<?> c = loader.loadClass("p.MyClass");
        assertThat(c, notNullValue());
        Object instance = c.getConstructor().newInstance();
        assertThat(instance.toString(), equalTo("MyClass"));
        assertThat(c.getModule().getName(), equalTo("synthetic"));
    }

    // Test our loadClass methods
    public void testSingleJarLoadClass() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        // TODO: add more resources than just the class
        createJar(jar, "MyClass");

        {
            StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
                StablePluginClassLoaderTests.class.getClassLoader(),
                jar
            );
            Class<?> c = loader.findClass("p.MyClass");
            assertThat(c, notNullValue());
            c = loader.findClass("p.DoesNotExist");
            assertThat(c, nullValue());
        }

        {
            StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
                StablePluginClassLoaderTests.class.getClassLoader(),
                jar
            );
            Class<?> c = loader.findClass("synthetic", "p.MyClass");
            assertThat(c, notNullValue());
            c = loader.findClass("synthetic", "p.DoesNotExist");
            assertThat(c, nullValue());
            c = loader.findClass("does-not-exist", "p.MyClass");
            assertThat(c, nullValue());
            c = loader.findClass(null, "p.MyClass");
            assertThat(c, nullValue());
        }
    }

    public void testSingleJarFindResources() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        // TODO: add more resources than just the class
        createJar(jar, "MyClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            jar
        );

        {
            URL location = loader.findResource("p/MyClass.class");
            assertThat(location, notNullValue());
            location = loader.findResource("p/DoesNotExist.class");
            assertThat(location, nullValue());
        }

        {
            URL location = loader.findResource("synthetic", "p/MyClass.class");
            assertThat(location, notNullValue());
            location = loader.findResource("synthetic", "p/DoesNotExist.class");
            assertThat(location, nullValue());
            location = loader.findResource("does-not-exist", "p/MyClass.class");
            assertThat(location, nullValue());
            location = loader.findResource(null, "p/MyClass.class");
            assertThat(location, nullValue());
        }

        {
            Enumeration<URL> locations = loader.findResources("p/MyClass.class");
            assertTrue(locations.hasMoreElements());
            locations = loader.findResources("p/DoesNotExist.class");
            assertFalse(locations.hasMoreElements());
        }
    }

    // test that we don't use parent-first searching
    public void testNoParentFirstSearch() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path overlappingJar = topLevelDir.resolve("my-overlapping-jar.jar");
        String className = "MyClass";
        createOverlappingJar(overlappingJar, className, "Wrong class found!");

        Path jar = topLevelDir.resolve("my-jar.jar");
        createJar(jar, className);

        URL[] urls = new URL[] { overlappingJar.toUri().toURL() };
        URLClassLoader parent;
        PrivilegedAction<URLClassLoader> pa = () -> URLClassLoader.newInstance(urls, StablePluginClassLoaderTests.class.getClassLoader());
        parent = AccessController.doPrivileged(pa);

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(parent, jar);

        Class<?> c = loader.loadClass("p.MyClass");
        Object instance = c.getConstructor().newInstance();
        assertThat(instance.toString(), equalTo("MyClass"));

        Class<?> c2 = parent.loadClass("p.MyClass");
        Object instance2 = c2.getConstructor().newInstance();
        assertThat(instance2.toString(), equalTo("MyClass"));
    }

    private static void createJar(Path outerJar, String className) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + className, String.format(Locale.ENGLISH, """
            package p;
            public class %s {
                @Override
                public String toString() {
                    return "%s";
                }
            }
            """, className, className));
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + className + ".class", classToBytes.get("p." + className));
        JarUtils.createJarWithEntries(outerJar, jarEntries);
    }

    private static void createOverlappingJar(Path jar, String className, String message) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + className, String.format(Locale.ENGLISH, """
            package p;
            public class %s {
                @Override
                public String toString() {
                    return "%s";
                }
            }
            """, className, message));
        sources.put("q.OtherClass", """
            package q;
            public class OtherClass {
                @Override
                public String toString() {
                    return "OtherClass";
                }
            }
            """);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + className + ".class", classToBytes.get("p." + className));
        jarEntries.put("q/" + className + ".class", classToBytes.get("q.OtherClass"));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    // TODO: remove
    private static void createModularJar(Path jar, String className) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + className, String.format(Locale.ENGLISH, """
            package p;
            public class %s {
                @Override
                public String toString() {
                    return "%s";
                }
            }
            """, className, className));
        sources.put("module-info", """
            module p {exports p;}
            """);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + className + ".class", classToBytes.get("p." + className));
        jarEntries.put("module-info.class", classToBytes.get("module-info"));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    // test that we don't use parent-first delegation (load from package if module has it)
}
