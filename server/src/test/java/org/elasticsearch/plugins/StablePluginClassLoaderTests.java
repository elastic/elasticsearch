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

import static org.hamcrest.Matchers.containsString;
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
        createJarWithSingleClass(outerJar, "MyClass");

        // loading it with a URL classloader (just checking the jar, remove
        // this block)
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, StablePluginClassLoaderTests.class.getClassLoader());
        PrivilegedAction<URLClassLoader> pa = () -> URLClassLoader.newInstance(urls, parent);
        try (@SuppressWarnings("removal")
        URLClassLoader loader = AccessController.doPrivileged(pa)) {
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

        PrivilegedAction<ClassLoader> pa = () -> ModuleLayer.defineModulesWithOneLoader(
            cf,
            List.of(mparent),
            this.getClass().getClassLoader()
        ).layer().findLoader("p");
        @SuppressWarnings("removal")
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
        createJarWithSingleClass(jar, "MyClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar)
        );

        URL location = loader.findResource("p/MyClass.class");
        assertThat(location, notNullValue());
        Class<?> c = loader.loadClass("p.MyClass");
        assertThat(c, notNullValue());
        Object instance = c.getConstructor().newInstance();
        assertThat(instance.toString(), equalTo("MyClass"));
        assertThat(c.getModule().getName(), equalTo("synthetic"));
    }

    public void testSingleJarFindClass() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        // TODO: add more resources than just the class
        createJarWithSingleClass(jar, "MyClass");

        {
            StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
                StablePluginClassLoaderTests.class.getClassLoader(),
                List.of(jar)
            );
            Class<?> c = loader.findClass("p.MyClass");
            assertThat(c, notNullValue());
            c = loader.findClass("p.DoesNotExist");
            assertThat(c, nullValue());
        }

        {
            StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
                StablePluginClassLoaderTests.class.getClassLoader(),
                List.of(jar)
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

    public void testSingleJarLoadClass() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        // TODO: add more resources than just the class
        createJarWithSingleClass(jar, "MyClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar)
        );
        Class<?> c = loader.loadClass("p.MyClass");
        assertThat(c, notNullValue());
        ClassNotFoundException e = expectThrows(ClassNotFoundException.class, () -> loader.loadClass("p.DoesNotExist"));
        assertThat(e.getMessage(), equalTo("p.DoesNotExist"));
    }

    public void testSingleJarFindResources() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        // TODO: add more resources than just the class
        createJarWithSingleClass(jar, "MyClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar)
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
        Path tempDir = createTempDir(getTestName());
        Path overlappingJar = tempDir.resolve("my-overlapping-jar.jar");
        String className = "MyClass";
        createOverlappingJar(overlappingJar, className);

        Path jar = tempDir.resolve("my-jar.jar");
        createJarWithSingleClass(jar, className);

        URL[] urls = new URL[] { overlappingJar.toUri().toURL() };
        PrivilegedAction<URLClassLoader> pa = () -> URLClassLoader.newInstance(urls, StablePluginClassLoaderTests.class.getClassLoader());
        @SuppressWarnings("removal")
        URLClassLoader parent = AccessController.doPrivileged(pa);

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(parent, List.of(jar));

        // stable plugin loader gives us the good class...
        Class<?> c = loader.loadClass("p.MyClass");
        Object instance = c.getConstructor().newInstance();
        assertThat(instance.toString(), equalTo("MyClass"));

        // we can get the "bad one" from the parent loader
        Class<?> c2 = parent.loadClass("p.MyClass");
        Object instance2 = c2.getConstructor().newInstance();
        assertThat(instance2.toString(), equalTo("Wrong class found!"));

        // stable plugin loader delegates to parent for other packages
        Class<?> c3 = loader.loadClass("q.OtherClass");
        Object instance3 = c3.getConstructor().newInstance();
        assertThat(instance3.toString(), equalTo("OtherClass"));
    }

    public void testMultipleJarLoadClass() throws Exception {
        // TODO: put classes in different packages, add specific test for split packages
        Path tempDir = createTempDir(getTestName());
        Path jar1 = tempDir.resolve("my-jar-1.jar");
        Path jar2 = tempDir.resolve("my-jar-2.jar");
        Path jar3 = tempDir.resolve("my-jar-3.jar");

        createJarWithSingleClass(jar1, "FirstClass");
        createJarWithSingleClass(jar2, "SecondClass");
        createJarWithSingleClass(jar3, "ThirdClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar1, jar2, jar3)
        );

        Class<?> c1 = loader.loadClass("p.FirstClass");
        Object instance1 = c1.getConstructor().newInstance();
        assertThat(instance1.toString(), equalTo("FirstClass"));

        Class<?> c2 = loader.loadClass("p.SecondClass");
        Object instance2 = c2.getConstructor().newInstance();
        assertThat(instance2.toString(), equalTo("SecondClass"));

        Class<?> c3 = loader.loadClass("p.ThirdClass");
        Object instance3 = c3.getConstructor().newInstance();
        assertThat(instance3.toString(), equalTo("ThirdClass"));
    }

    public void testModuleDenyList() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        // TODO: add more resources than just the class
        createJarWithSingleClass(jar, "MyImportingClass", """
            package p;
            import java.lang.management.ThreadInfo;
            import java.sql.ResultSet;
            public class MyImportingClass {
                @Override
                public String toString() {
                    return "MyImportingClass[imports " + ThreadInfo.class.getSimpleName() + "," + ResultSet.class.getSimpleName() + "]";
                }
            }
            """);

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar),
            Set.of("java.sql")
        );
        Class<?> c = loader.loadClass("p.MyImportingClass");
        assertThat(c, notNullValue());
        Object instance = c.getConstructor().newInstance();
        IllegalAccessError e = expectThrows(IllegalAccessError.class, instance::toString);
        assertThat(e.getMessage(), containsString("cannot access class java.sql.ResultSet (in module java.sql)"));
    }

    private static void createJarWithSingleClass(Path jar, String className) throws IOException {
        createJarWithSingleClass(jar, className, getSimpleSourceString("p", className, className));
    }

    private static void createJarWithSingleClass(Path jar, String className, String source) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + className, source);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + className + ".class", classToBytes.get("p." + className));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    private static void createOverlappingJar(Path jar, String className) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + className, getSimpleSourceString("p", className, "Wrong class found!"));
        sources.put("q.OtherClass", getSimpleSourceString("q", "OtherClass", "OtherClass"));
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + className + ".class", classToBytes.get("p." + className));
        jarEntries.put("q/OtherClass.class", classToBytes.get("q.OtherClass"));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    private static String getSimpleSourceString(String packageName, String className, String toStringOutput) {
        return String.format(Locale.ENGLISH, """
            package %s;
            public class %s {
                @Override
                public String toString() {
                    return "%s";
                }
            }
            """, packageName, className, toStringOutput);
    }

    // TODO: remove
    private static void createModularJar(Path jar, String className) throws IOException {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + className, getSimpleSourceString("p", className, className));
        sources.put("module-info", "module p {exports p;}");
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + className + ".class", classToBytes.get("p." + className));
        jarEntries.put("module-info.class", classToBytes.get("module-info"));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }
}
