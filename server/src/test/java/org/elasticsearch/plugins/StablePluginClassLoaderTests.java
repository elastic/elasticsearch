/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.plugins;

import joptsimple.internal.Strings;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Arrays.stream;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class StablePluginClassLoaderTests extends ESTestCase {

    /**
     * Test the loadClass method, which is the real entrypoint for users of the classloader
     */
    public void testLoadFromJar() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar.jar");
        createJarWithSingleClass(jar, "p.MyClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar)
        );

        {
            Class<?> c = loader.loadClass("p.MyClass");
            assertThat(c, notNullValue());
            Object instance = c.getConstructor().newInstance();
            assertThat(instance.toString(), equalTo("MyClass"));
            assertThat(c.getModule().getName(), equalTo("synthetic"));
        }

        {
            ClassNotFoundException e = expectThrows(ClassNotFoundException.class, () -> loader.loadClass("p.DoesNotExist"));
            assertThat(e.getMessage(), equalTo("p.DoesNotExist"));
        }
    }

    /**
     * Test the findClass method, which we overrode but which will not be called by
     * users of the classloader
     */
    public void testSingleJarFindClass() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        createJarWithSingleClass(jar, "p.MyClass");

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

    public void testSingleJarFindResources() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");

        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p." + "MyClass", getSimpleSourceString("p", "MyClass", "MyClass"));
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/" + "MyClass" + ".class", classToBytes.get("p." + "MyClass"));
        jarEntries.put("META-INF/resource.txt", "my resource".getBytes(StandardCharsets.UTF_8));
        JarUtils.createJarWithEntries(jar, jarEntries);

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar)
        );

        {
            URL location = loader.findResource("p/MyClass.class");
            assertThat(location, notNullValue());
            location = loader.findResource("p/DoesNotExist.class");
            assertThat(location, nullValue());
            location = loader.findResource("META-INF/resource.txt");
            assertThat(location, notNullValue());
            location = loader.findResource("META-INF/does_not_exist.txt");
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
            locations = loader.findResources("META-INF/resource.txt");
            assertTrue(locations.hasMoreElements());
            locations = loader.findResources("META-INF/does_not_exist.txt");
            assertFalse(locations.hasMoreElements());
        }
    }

    public void testHideSplitPackageInParentClassloader() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path overlappingJar = tempDir.resolve("my-split-package.jar");
        createOverlappingJar(overlappingJar, "ParentJarClassInPackageP");

        Path jar = tempDir.resolve("my-jar.jar");
        createJarWithSingleClass(jar, "p.MyClassInPackageP");

        URL[] urls = new URL[] { overlappingJar.toUri().toURL() };
        PrivilegedAction<URLClassLoader> pa = () -> URLClassLoader.newInstance(urls, StablePluginClassLoaderTests.class.getClassLoader());
        @SuppressWarnings("removal")
        URLClassLoader parent = AccessController.doPrivileged(pa);

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(parent, List.of(jar));

        // stable plugin loader gives us the good class...
        Class<?> c = loader.loadClass("p.MyClassInPackageP");
        Object instance = c.getConstructor().newInstance();
        assertThat(instance.toString(), equalTo("MyClassInPackageP"));

        // but stable plugin loader can't find the class from the split package in the parent loader
        ClassNotFoundException e = expectThrows(ClassNotFoundException.class, () -> loader.loadClass("p.ParentJarClassInPackageP"));
        assertThat(e.getMessage(), equalTo("p.ParentJarClassInPackageP"));

        // we can get the "bad one" from the parent loader
        Class<?> c2 = parent.loadClass("p.ParentJarClassInPackageP");
        Object instance2 = c2.getConstructor().newInstance();
        assertThat(instance2.toString(), equalTo("Wrong class found!"));

        // stable plugin loader delegates to parent for other packages
        Class<?> c3 = loader.loadClass("q.OtherClass");
        Object instance3 = c3.getConstructor().newInstance();
        assertThat(instance3.toString(), equalTo("OtherClass"));
    }

    public void testNoParentFirstSearch() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path overlappingJar = tempDir.resolve("my-overlapping-jar.jar");
        String className = "MyClass";
        createOverlappingJar(overlappingJar, className);

        Path jar = tempDir.resolve("my-jar.jar");
        createJarWithSingleClass(jar, "p." + className);

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

    public void testMultipleJarSinglePackageLoadClass() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path jar1 = tempDir.resolve("my-jar-1.jar");
        Path jar2 = tempDir.resolve("my-jar-2.jar");
        Path jar3 = tempDir.resolve("my-jar-3.jar");

        createJarWithSingleClass(jar1, "p.FirstClass");
        createJarWithSingleClass(jar2, "p.SecondClass");
        createJarWithSingleClass(jar3, "p.ThirdClass");

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

    public void testSplitPackageJarLoadClass() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path jar1 = tempDir.resolve("my-jar-1.jar");
        Path jar2 = tempDir.resolve("my-jar-2.jar");
        Path jar3 = tempDir.resolve("my-jar-3.jar");

        createJarWithSingleClass(jar1, "p.a.FirstClass");
        createJarWithSingleClass(jar2, "p.split.SecondClass");
        createJarWithSingleClass(jar3, "p.split.ThirdClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar1, jar2, jar3)
        );

        Class<?> c1 = loader.loadClass("p.a.FirstClass");
        Object instance1 = c1.getConstructor().newInstance();
        assertThat(instance1.toString(), equalTo("FirstClass"));

        Class<?> c2 = loader.loadClass("p.split.SecondClass");
        Object instance2 = c2.getConstructor().newInstance();
        assertThat(instance2.toString(), equalTo("SecondClass"));

        Class<?> c3 = loader.loadClass("p.split.ThirdClass");
        Object instance3 = c3.getConstructor().newInstance();
        assertThat(instance3.toString(), equalTo("ThirdClass"));
    }

    public void testPackagePerJarLoadClass() throws Exception {
        Path tempDir = createTempDir(getTestName());
        Path jar1 = tempDir.resolve("my-jar-1.jar");
        Path jar2 = tempDir.resolve("my-jar-2.jar");
        Path jar3 = tempDir.resolve("my-jar-3.jar");

        createJarWithSingleClass(jar1, "p.a.FirstClass");
        createJarWithSingleClass(jar2, "p.b.SecondClass");
        createJarWithSingleClass(jar3, "p.c.ThirdClass");

        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar1, jar2, jar3)
        );

        Class<?> c1 = loader.loadClass("p.a.FirstClass");
        Object instance1 = c1.getConstructor().newInstance();
        assertThat(instance1.toString(), equalTo("FirstClass"));

        Class<?> c2 = loader.loadClass("p.b.SecondClass");
        Object instance2 = c2.getConstructor().newInstance();
        assertThat(instance2.toString(), equalTo("SecondClass"));

        Class<?> c3 = loader.loadClass("p.c.ThirdClass");
        Object instance3 = c3.getConstructor().newInstance();
        assertThat(instance3.toString(), equalTo("ThirdClass"));
    }

    public void testModuleDenyList() throws Exception {
        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar-with-resources.jar");
        createJarWithSingleClass(jar, "p", "MyImportingClass", """
            package p;
            import java.lang.management.ThreadInfo;
            import java.sql.ResultSet;
            public class MyImportingClass {
                @Override
                public String toString() {
                    return "MyImportingClass[imports " + ResultSet.class.getSimpleName() + "]";
                }
            }
            """);

        StablePluginClassLoader denyListLoader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar),
            Set.of("java.sql")
        );
        Class<?> denyListed = denyListLoader.loadClass("p.MyImportingClass");
        assertThat(denyListed, notNullValue());
        Object instance1 = denyListed.getConstructor().newInstance();
        IllegalAccessError e = expectThrows(IllegalAccessError.class, instance1::toString);
        assertThat(e.getMessage(), containsString("cannot access class java.sql.ResultSet (in module java.sql)"));

        StablePluginClassLoader unrestrictedLoader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar)
        );
        Class<?> unrestricted = unrestrictedLoader.loadClass("p.MyImportingClass");
        assertThat(unrestricted, notNullValue());
        Object instance2 = unrestricted.getConstructor().newInstance();
        assertThat(instance2.toString(), containsString("MyImportingClass[imports ResultSet]"));
    }

    public void testMultiReleaseJar() throws Exception {
        Object fooBar = newFooBar(true, 7, 9, 11, 15);
        assertThat(fooBar.toString(), equalTo("FooBar 15"));

        fooBar = newFooBar(false);
        assertThat(fooBar.toString(), equalTo("FooBar 0"));

        fooBar = newFooBar(true, 10_000);
        assertThat(fooBar.toString(), equalTo("FooBar 0"));
    }

    /*
     * Instantiates an instance of FooBar. First generates and compiles the code, then packages it,
     * and lastly loads the FooBar class with an embedded loader.
     *
     * @param enableMulti whether to set the multi-release attribute
     * @param versions the runtime version number of the entries to create in the jar
     */
    private Object newFooBar(boolean enableMulti, int... versions) throws Exception {
        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("p/FooBar.class", classBytesForVersion(0)); // root version is always 0
        if (enableMulti) {
            jarEntries.put("META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(StandardCharsets.UTF_8));
        }
        stream(versions).forEach(v -> jarEntries.put("META-INF/versions/" + v + "/p/FooBar.class", classBytesForVersion(v)));

        Path topLevelDir = createTempDir(getTestName());
        Path jar = topLevelDir.resolve("my-jar.jar");
        JarUtils.createJarWithEntries(jar, jarEntries);
        StablePluginClassLoader loader = StablePluginClassLoader.getInstance(
            StablePluginClassLoaderTests.class.getClassLoader(),
            List.of(jar)
        );
        Class<?> c = loader.loadClass("p.FooBar");
        return c.getConstructor().newInstance();
    }

    /* Creates a FooBar class that reports the given version in its toString. */
    static byte[] classBytesForVersion(int version) {
        return InMemoryJavaCompiler.compile("p.FooBar", String.format(Locale.ENGLISH, """
            package p;
            public class FooBar {
                @Override public String toString() {
                    return "FooBar %d";
                }
            }
            """, version));
    }

    private static void createJarWithSingleClass(Path jar, String className) throws Exception {
        if (className.contains(".") == false) {
            createJarWithSingleClass(jar, "", className, getSimpleSourceString("", className, className));
            return;
        }
        int lastIndex = className.lastIndexOf(".");
        String simpleClassName = className.substring(lastIndex + 1);
        String packageName = className.substring(0, lastIndex);
        createJarWithSingleClass(jar, packageName, simpleClassName, getSimpleSourceString(packageName, simpleClassName,
            simpleClassName));
    }

    private static void createJarWithSingleClass(Path jar, String packageName, String className, String source)
        throws IOException {
        List<String> packagePathElements = Arrays.stream(packageName.split("\\.")).toList();
        Map<String, CharSequence> sources = new HashMap<>();
        String canonicalName = toBinaryName(packagePathElements) + "." + className;
        // TODO: windows
        String jarPath = toFilesystemPath(packagePathElements) + "/" + className + ".class";
        sources.put(canonicalName, source);
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put(jarPath, classToBytes.get(canonicalName));
        JarUtils.createJarWithEntries(jar, jarEntries);
    }

    private static String toFilesystemPath(List<String> packagePathElements) {
        // TODO: windows
        return Strings.join(packagePathElements, "/");
    }

    private static String toBinaryName(List<String> packagePathElements) {
        return Strings.join(packagePathElements, ".");
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
            %s
            public class %s {
                @Override
                public String toString() {
                    return "%s";
                }
            }
            """, Strings.isNullOrEmpty(packageName) ? "" : "package " + packageName + ";", className, toStringOutput);

    }
}
