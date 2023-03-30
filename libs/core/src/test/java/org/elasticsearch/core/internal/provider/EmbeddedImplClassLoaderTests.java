/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import org.elasticsearch.core.Strings;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.CompoundEnumeration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.PrivilegedOperations;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.basePrefix;
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.rootURI;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.startsWith;

public class EmbeddedImplClassLoaderTests extends ESTestCase {

    public void testBasePrefix() {
        assertThat(
            basePrefix("IMPL-JARS/x-content/jackson-core-2.13.2-SNAPSHOT.jar"),
            equalTo("IMPL-JARS/x-content/jackson-core-2.13.2-SNAPSHOT.jar")
        );
        assertThat(
            basePrefix("IMPL-JARS/x-content/jackson-core-2.13.2.jar/META-INF/versions/9"),
            equalTo("IMPL-JARS/x-content/jackson-core-2.13.2.jar")
        );
        assertThat(
            basePrefix("IMPL-JARS/x-content/jackson-core-2.13.2.jar/META-INF/versions/100"),
            equalTo("IMPL-JARS/x-content/jackson-core-2.13.2.jar")
        );
    }

    /*
     * Tests that the root version of a class is loaded, when the multi-release attribute is absent.
     */
    public void testLoadWithoutMultiReleaseDisabled() throws Exception {
        Object foobar = newFooBar(false, 0);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(false, 9);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(false, 11);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(false, 17);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));
    }

    /*
     * Tests that the root version of a class is loaded, when the multi-release attribute is present,
     * but the versioned entry is greater than the runtime version.
     */
    public void testLoadMegaVersionWithMultiReleaseEnabled() throws Exception {
        assumeTrue("JDK version not less than 10_000", Runtime.version().feature() < 10_000);
        Object foobar = newFooBar(true, 10_000);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(true, 10_001);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(true, 100_000);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));
    }

    /*
     * Tests that the specific, 9, version of a class is loaded, when the multi-release attribute
     * is present and the versioned entry is less than or equal to the runtime version.
     */
    public void testLoadWithMultiReleaseEnabled9() throws Exception {
        assumeTrue("JDK version not greater than or equal to 9", Runtime.version().feature() >= 9);
        Object foobar = newFooBar(true, 9);
        // expect 9 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 9));

        foobar = newFooBar(true, 9, 8);
        assertThat(foobar.toString(), equalTo("FooBar " + 9));
    }

    /*
     * Tests that the specific, 11, version of a class is loaded, when the multi-release attribute
     * is present and the versioned entry is less than or equal to the runtime version.
     */
    public void testLoadWithMultiReleaseEnabled11() throws Exception {
        assumeTrue("JDK version not greater than or equal to 11", Runtime.version().feature() >= 11);
        Object foobar = newFooBar(true, 11);
        // expect 11 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 11));

        foobar = newFooBar(true, 11, 10, 9, 8);
        assertThat(foobar.toString(), equalTo("FooBar " + 11));
    }

    /*
     * Tests that the specific, 17, version of a class is loaded, when the multi-release attribute
     * is present and the versioned entry is less than or equal to the runtime version.
     */
    public void testLoadWithMultiReleaseEnabled17() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        Object foobar = newFooBar(true, 17);
        // expect 17 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 17));

        foobar = newFooBar(true, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8);
        assertThat(foobar.toString(), equalTo("FooBar " + 17));
    }

    /*
     * Tests that the greatest specific version of a class is loaded, when the multi-release attribute
     * is present and the versioned entry is less than or equal to the runtime version.
     */
    public void testLoadWithMultiReleaseEnabledALL() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);

        Object foobar = newFooBar(true, 16, 13, 11, 9);
        // expect 16 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 16));

        foobar = newFooBar(true, 13, 12, 11, 10, 9, 8);
        // expect 13 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 13));

        foobar = newFooBar(true, 10, 9, 8);
        // expect 10 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 10));
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

    /*
     * Instantiates an instance of FooBar. First generates and compiles the code, then packages it,
     * and lastly loads the FooBar class with an embedded loader.
     *
     * @param enableMulti whether to set the multi-release attribute
     * @param versions the runtime version number of the entries to create in the jar
     */
    private Object newFooBar(boolean enableMulti, int... versions) throws Exception {
        String prefix = "IMPL-JARS/x-foo/x-foo-impl.jar/";
        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/x-foo/LISTING.TXT", bytes("x-foo-impl.jar"));
        jarEntries.put(prefix + "p/FooBar.class", classBytesForVersion(0)); // root version is always 0
        if (enableMulti) {
            jarEntries.put(prefix + "META-INF/MANIFEST.MF", bytes("Multi-Release: true\n"));
        }
        stream(versions).forEach(v -> jarEntries.put(prefix + "META-INF/versions/" + v + "/p/FooBar.class", classBytesForVersion(v)));

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, EmbeddedImplClassLoaderTests.class.getClassLoader());
        try {
            EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "x-foo");
            Class<?> c = loader.loadClass("p.FooBar");
            return c.getConstructor().newInstance();
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    public void testRootURI() throws Exception {
        assertThat(
            rootURI(new URL("jar:file:/xxx/distro/lib/elasticsearch-x-content-8.2.0-SNAPSHOT.jar!/IMPL-JARS/x-content/xlib-2.10.4.jar")),
            equalTo(URI.create("jar:file:/xxx/distro/lib/elasticsearch-x-content-8.2.0-SNAPSHOT.jar"))
        );

        assertThat(
            rootURI(new URL("file:/x/git/es_modules/libs/x-content/build/generated-resources/impl/IMPL-JARS/x-content/xlib-2.10.4.jar")),
            equalTo(URI.create("file:/x/git/es_modules/libs/x-content/build/generated-resources/impl"))
        );
    }

    public void testCompoundEnumeration() {
        @SuppressWarnings("unchecked")
        var enumerations = (Enumeration<String>[]) new Enumeration<?>[] {
            Collections.enumeration(List.of("hello")),
            Collections.enumeration(List.of("world")) };
        var compoundEnumeration = new CompoundEnumeration<>(enumerations);

        List<String> l = new ArrayList<>();
        while (compoundEnumeration.hasMoreElements()) {
            l.add(compoundEnumeration.nextElement());
        }
        assertThat(l, contains(is("hello"), is("world")));
        expectThrows(NoSuchElementException.class, () -> compoundEnumeration.nextElement());
    }

    /* Basic test for resource loading. */
    public void testResourcesBasic() throws Exception {
        Path topLevelDir = createTempDir(getTestName());

        Map<String, String> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/res/LISTING.TXT", "res-impl.jar\nzoo-impl.jar");
        jarEntries.put("IMPL-JARS/res/res-impl.jar/p/res.txt", "Hello World");
        jarEntries.put("IMPL-JARS/res/res-impl.jar/A-C/res.txt", "Res resource");
        jarEntries.put("IMPL-JARS/res/zoo-impl.jar/A-C/res.txt", "Zoo resource");

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntriesUTF(outerJar, jarEntries);
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, EmbeddedImplClassLoaderTests.class.getClassLoader());
        try {
            EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "res");
            // resource in a valid java package dir
            URL url = loader.findResource("p/res.txt");
            assertThat(url.toString(), endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/p/res.txt"));

            var resources = loader.findResources("p/res.txt");
            assertThat(Collections.list(resources), contains(hasToString(endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/p/res.txt"))));

            // resource in a non-package dir
            url = loader.findResource("A-C/res.txt");
            assertThat(
                url.toString(),
                anyOf(
                    endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/A-C/res.txt"),
                    endsWith("impl.jar!/IMPL-JARS/res/zoo-impl.jar/A-C/res.txt")
                )
            );
            assertThat(urlToString(url), is(oneOf("Res resource", "Zoo resource")));

            resources = loader.findResources("A-C/res.txt");
            assertThat(
                Collections.list(resources),
                containsInAnyOrder(
                    hasToString(endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/A-C/res.txt")),
                    hasToString(endsWith("impl.jar!/IMPL-JARS/res/zoo-impl.jar/A-C/res.txt"))
                )
            );
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    /* Tests resource loading including from parent, for non-java package names. */
    public void testResourcesParentNonPkgName() throws Exception {
        testResourcesParent("p-u/q/r/T.txt");
    }

    /* Tests resource loading including from parent, for valid java package names. */
    public void testResourcesParentJavaPkgName() throws Exception {
        testResourcesParent("p/q/r/T.txt");
    }

    /* Test resource loading from parent. */
    private void testResourcesParent(String resourcePath) throws Exception {
        Path topLevelDir = createTempDir(getTestName());

        ClassLoader embedLoader, parentLoader;
        List<URLClassLoader> closeables = new ArrayList<>();
        try {
            {   // load content with URLClassLoader - this will be the parent
                Map<String, String> jarEntries = new HashMap<>();
                jarEntries.put(resourcePath, "Parent Resource");
                Path fooJar = topLevelDir.resolve("parent.jar");
                JarUtils.createJarWithEntriesUTF(fooJar, jarEntries);
                var urlClassLoader = URLClassLoader.newInstance(
                    new URL[] { fooJar.toUri().toURL() },
                    EmbeddedImplClassLoaderTests.class.getClassLoader()
                );
                closeables.add(urlClassLoader);
                parentLoader = urlClassLoader;
            }
            {   // load similarly named resource with EmbeddedImplClassLoader
                Map<String, String> jarEntries = new HashMap<>();
                jarEntries.put("IMPL-JARS/res/LISTING.TXT", "res-impl.jar");
                jarEntries.put("IMPL-JARS/res/res-impl.jar/" + resourcePath, "Embedded Resource");
                Path outerJar = topLevelDir.resolve("embedded.jar");
                JarUtils.createJarWithEntriesUTF(outerJar, jarEntries);
                URLClassLoader outter = URLClassLoader.newInstance(new URL[] { outerJar.toUri().toURL() }, parentLoader);
                closeables.add(outter);
                embedLoader = EmbeddedImplClassLoader.getInstance(outter, "res");
            }

            List<URL> urls = Collections.list(embedLoader.getResources(resourcePath));
            assertThat(urls, hasSize(2));
            assertThat(
                urls.stream().map(EmbeddedImplClassLoaderTests::urlToString).toList(),
                containsInAnyOrder("Parent Resource", "Embedded Resource")
            );
        } finally {
            for (URLClassLoader closeable : closeables) {
                PrivilegedOperations.closeURLClassLoader(closeable);
            }
        }
    }

    /*
     * Tests that the root version of a resource is located, when the multi-release attribute is absent.
     */
    public void testResourcesWithoutMultiRelease() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        testResourcesVersioned(false, 0, 9);
        testResourcesVersioned(false, 0, 15);
        testResourcesVersioned(false, 0, 17);
        testResourcesVersioned(false, 0, 11, 10, 9, 8);
    }

    /*
     * Tests that the specific, 9, version of a resource is located, when the multi-release attribute
     * is present and the versioned entry is less than or equal to the runtime version.
     */
    public void testResourcesVersioned9() throws Exception {
        assumeTrue("JDK version not greater than or equal to 9", Runtime.version().feature() >= 9);
        testResourcesVersioned(true, 9, 9);
        testResourcesVersioned(true, 9, 9, 8);
    }

    /*
     * Tests that the specific, 11, version of a resource is located, when the multi-release attribute
     * is present and the versioned entry is less than or equal to the runtime version.
     */
    public void testResourcesVersioned11() throws Exception {
        assumeTrue("JDK version not greater than or equal to 11", Runtime.version().feature() >= 11);
        testResourcesVersioned(true, 11, 11);
        testResourcesVersioned(true, 11, 11, 10, 9, 8);
    }

    /*
     * Tests that the specific, 17, version of a resource is located, when the multi-release attribute
     * is present and the versioned entry is less than or equal to the runtime version.
     */
    public void testResourcesVersioned17() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        testResourcesVersioned(true, 17, 17);
        testResourcesVersioned(true, 17, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8);
    }

    static byte[] bytes(String str) {
        return str.getBytes(UTF_8);
    }

    private void testResourcesVersioned(boolean enableMulti, int expectedVersion, int... versions) throws Exception {
        testResourcesVersioned("p/res.txt", enableMulti, expectedVersion, versions);
        // "A-B" is not a valid java package name - triggers the slow path
        testResourcesVersioned("A-B/res.txt", enableMulti, expectedVersion, versions);
    }

    /*
     * Tests that the expected version of a resource is located.
     *
     * A jar archive is created with a resource, and optionally additional version specific entries
     * of said resource. Both a URLClassLoader and an embedded loader are created to locate the
     * resource. The resource located by embedded loader is compared to that of the equivalent
     * resource located by the URLClassLoader.
     *
     * @param resourcePath the resource path
     * @param enableMulti whether to set the multi-release attribute
     * @param expectedVersion the expected resource version to locate
     * @param versions the runtime version number of the entries to create in the jar
     */
    private void testResourcesVersioned(String resourcePath, boolean enableMulti, int expectedVersion, int... versions) throws Exception {
        Path topLevelDir = createTempDir(getTestName());

        ClassLoader embedLoader, urlcLoader;
        List<URLClassLoader> closeables = new ArrayList<>();
        try {
            {   // load content with URLClassLoader
                Map<String, String> jarEntries = new HashMap<>();
                jarEntries.put(resourcePath, "Hello World0");
                if (enableMulti) {
                    jarEntries.put("META-INF/MANIFEST.MF", "Multi-Release: true\n");
                }
                stream(versions).forEach(v -> jarEntries.put("META-INF/versions/" + v + "/" + resourcePath, "Hello World" + v));
                Path fooJar = topLevelDir.resolve("foo.jar");
                JarUtils.createJarWithEntriesUTF(fooJar, jarEntries);
                var urlClassLoader = URLClassLoader.newInstance(
                    new URL[] { fooJar.toUri().toURL() },
                    EmbeddedImplClassLoaderTests.class.getClassLoader()
                );
                closeables.add(urlClassLoader);
                urlcLoader = urlClassLoader;
            }
            {   // load EQUIVALENT content with EmbeddedImplClassLoader
                String prefix = "IMPL-JARS/res/res-impl.jar/";
                Map<String, String> jarEntries = new HashMap<>();
                jarEntries.put("IMPL-JARS/res/LISTING.TXT", "res-impl.jar");
                jarEntries.put(prefix + resourcePath, "Hello World0");
                if (enableMulti) {
                    jarEntries.put(prefix + "META-INF/MANIFEST.MF", "Multi-Release: true\n");
                }
                stream(versions).forEach(v -> jarEntries.put(prefix + "META-INF/versions/" + v + "/" + resourcePath, ("Hello World" + v)));
                Path outerJar = topLevelDir.resolve("impl.jar");
                JarUtils.createJarWithEntriesUTF(outerJar, jarEntries);
                URLClassLoader parent = URLClassLoader.newInstance(
                    new URL[] { outerJar.toUri().toURL() },
                    EmbeddedImplClassLoaderTests.class.getClassLoader()
                );
                closeables.add(parent);
                embedLoader = EmbeddedImplClassLoader.getInstance(parent, "res");
            }

            // finding resources with the different loaders should give EQUIVALENT results

            String expectedURLSuffix = resourcePath;
            if (enableMulti) {
                expectedURLSuffix = "META-INF/versions/" + expectedVersion + "/" + resourcePath;
            }

            // getResource
            URL url1 = urlcLoader.getResource(resourcePath);
            assertThat(url1.toString(), endsWith("!/" + expectedURLSuffix));
            URL url2 = embedLoader.getResource(resourcePath);
            assertThat(url2.toString(), endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/" + expectedURLSuffix));
            assertThat(suffix(url2), endsWith(suffix(url1)));

            // getResources
            var urls1 = Collections.list(urlcLoader.getResources(resourcePath)).stream().map(URL::toString).toList();
            var urls2 = Collections.list(embedLoader.getResources(resourcePath)).stream().map(URL::toString).toList();
            assertThat(Strings.format("urls1=%s, urls2=%s", urls1, urls2), urls2, hasSize(1));
            assertThat(urls1.get(0), endsWith("!/" + expectedURLSuffix));
            assertThat(urls2.get(0), endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/" + expectedURLSuffix));

            // getResourceAsStream
            try (var is = embedLoader.getResourceAsStream(resourcePath)) {
                assertThat(new String(is.readAllBytes(), UTF_8), is("Hello World" + expectedVersion));
            }
        } finally {
            for (URLClassLoader closeable : closeables) {
                PrivilegedOperations.closeURLClassLoader(closeable);
            }
        }
    }

    private static String suffix(URL url) {
        String urlStr = url.toString();
        int idx = urlStr.indexOf("!/");
        assert idx > 0;
        return urlStr.substring(idx + 2);
    }

    static final Class<NullPointerException> NPE = NullPointerException.class;
    static final Class<ClassNotFoundException> CNFE = ClassNotFoundException.class;

    /*
     * Tests locating classes and resource not in the embedded loader.
     * As well as additional null checks.
     */
    public void testIDontHaveIt() throws Exception {
        Path topLevelDir = createTempDir(getTestName());

        ClassLoader embedLoader;
        Map<String, String> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/res/LISTING.TXT", "res-impl.jar");
        jarEntries.put("IMPL-JARS/res/res-impl.jar/p/res.txt", "Hello World0");
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntriesUTF(outerJar, jarEntries);
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, EmbeddedImplClassLoaderTests.class.getClassLoader());
        try {
            embedLoader = EmbeddedImplClassLoader.getInstance(parent, "res");

            Class<?> c = embedLoader.loadClass("java.lang.Object");
            assertThat(c, is(java.lang.Object.class));
            assertThat(c.getClassLoader(), not(equalTo(embedLoader)));

            // not found
            expectThrows(CNFE, () -> embedLoader.loadClass("a.b.c.Unknown"));
            assertThat(embedLoader.getResource("a/b/c/Unknown.class"), nullValue());
            assertThat(embedLoader.getResources("a/b/c/Unknown.class").hasMoreElements(), is(false));
            assertThat(embedLoader.getResourceAsStream("a/b/c/Unknown.class"), nullValue());
            assertThat(embedLoader.resources("a/b/c/Unknown.class").toList(), emptyCollectionOf(URL.class));

            // nulls
            expectThrows(NPE, () -> embedLoader.getResource(null));
            expectThrows(NPE, () -> embedLoader.getResources(null));
            expectThrows(NPE, () -> embedLoader.getResourceAsStream(null));
            expectThrows(NPE, () -> embedLoader.resources(null));
            expectThrows(NPE, () -> embedLoader.loadClass(null));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    /*
     * Tests class loading with dependencies across multiple embedded jars.
     */
    public void testLoadWithJarDependencies() throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p.Foo", "package p; public class Foo extends q.Bar { }");
        sources.put("q.Bar", "package q; public class Bar extends r.Baz { }");
        sources.put("r.Baz", "package r; public class Baz { }");
        var classToBytes = InMemoryJavaCompiler.compile(sources);

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/blah/LISTING.TXT", "foo.jar\nbar.jar\nbaz.jar".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/blah/foo.jar/p/Foo.class", classToBytes.get("p.Foo"));
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/MANIFEST.MF", "Multi-Release: True\n".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/versions/9/q/Bar.class", classToBytes.get("q.Bar"));
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/versions/11/r/Baz.class", classToBytes.get("r.Baz"));

        Path topLevelDir = createTempDir(getTestName());
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URL[] urls = new URL[] { outerJar.toUri().toURL() };

        URLClassLoader parent = URLClassLoader.newInstance(urls, EmbeddedImplClassLoaderTests.class.getClassLoader());
        try {
            EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "blah");
            Class<?> c = loader.loadClass("p.Foo");
            Object obj = c.getConstructor().newInstance();
            assertThat(obj.toString(), startsWith("p.Foo"));
            assertThat(c.getSuperclass().getName(), is("q.Bar"));
            assertThat(c.getSuperclass().getSuperclass().getName(), is("r.Baz"));

            // assert CNFE from unknown class in a package known to the loader
            expectThrows(CNFE, () -> loader.loadClass("p.Unknown"));
            expectThrows(CNFE, () -> loader.loadClass("q.Unknown"));
            expectThrows(CNFE, () -> loader.loadClass("r.Unknown"));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    /*
     * Tests resource lookup across multiple embedded jars.
     */
    public void testResourcesWithMultipleJars() throws Exception {
        Path topLevelDir = createTempDir(getTestName());

        Map<String, String> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/blah/LISTING.TXT", "foo.jar\nbar.jar\nbaz.jar");
        jarEntries.put("IMPL-JARS/blah/foo.jar/res.txt", "fooRes");
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/MANIFEST.MF", "Multi-Release: TRUE\n");
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/versions/9/res.txt", "barRes");
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/MANIFEST.MF", "Multi-Release: trUE\n");
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/versions/11/res.txt", "bazRes");

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntriesUTF(outerJar, jarEntries);
        URL[] urls = new URL[] { outerJar.toUri().toURL() };
        URLClassLoader parent = URLClassLoader.newInstance(urls, EmbeddedImplClassLoaderTests.class.getClassLoader());
        try {
            EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "blah");
            var res = Collections.list(loader.getResources("res.txt"));
            assertThat(res, hasSize(3));
            List<String> l = res.stream().map(EmbeddedImplClassLoaderTests::urlToString).toList();
            assertThat(l, containsInAnyOrder("fooRes", "barRes", "bazRes"));
        } finally {
            PrivilegedOperations.closeURLClassLoader(parent);
        }
    }

    @SuppressForbidden(reason = "file urls")
    static String urlToString(URL url) {
        try {
            var urlc = url.openConnection();
            urlc.setUseCaches(false);
            try (var is = urlc.getInputStream()) {
                return new String(is.readAllBytes(), UTF_8);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
