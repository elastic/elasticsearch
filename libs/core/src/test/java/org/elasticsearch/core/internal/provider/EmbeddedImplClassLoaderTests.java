/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core.internal.provider;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.CompoundEnumeration;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.compiler.InMemoryJavaCompiler;
import org.elasticsearch.test.jar.JarUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.basePrefix;
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.rootURI;
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
import static org.hamcrest.Matchers.startsWith;

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

    public void testLoadWithoutMultiReleaseDisabled() throws Exception {
        // expect root FooBar to be loaded each time

        Object foobar = newFooBar(false, 0);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(false, 9);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(false, 11);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(false, 17);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));
    }

    public void testLoadMegaVersionWithMultiReleaseEnabled() throws Exception {
        // expect root FooBar to be loaded each time

        assumeTrue("JDK version not less than 10_000", Runtime.version().feature() < 10_000);
        Object foobar = newFooBar(true, 10_000);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(true, 10_001);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(true, 100_000);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));
    }

    public void testLoadWithMultiReleaseEnabled9() throws Exception {
        assumeTrue("JDK version not greater than or equal to 9", Runtime.version().feature() >= 9);
        Object foobar = newFooBar(true, 9);
        // expect 9 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 9));
    }

    public void testLoadWithMultiReleaseEnabled11() throws Exception {
        assumeTrue("JDK version not greater than or equal to 11", Runtime.version().feature() >= 11);
        Object foobar = newFooBar(true, 11);
        // expect 11 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 11));
    }

    public void testLoadWithMultiReleaseEnabled17() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        Object foobar = newFooBar(true, 17);
        // expect 17 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 17));
    }

    public void testLoadWithMultiReleaseEnabledALL() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        Object foobar = newFooBar(true, 17, 16, 15, 14, 13, 12, 10, 9);
        // expect 17 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 17));
    }

    // Creates a FooBar class that reports the given version in its toString
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

    static Object newFooBar(boolean enableMulti, int... versions) throws Exception {
        Path topLevelDir = createTempDir();

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/x-foo/LISTING.TXT", "x-foo-impl.jar".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/x-foo/x-foo-impl.jar/p/FooBar.class", classBytesForVersion(0)); // root version is always 0
        if (enableMulti) {
            jarEntries.put("IMPL-JARS/x-foo/x-foo-impl.jar/META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(UTF_8));
        }
        Arrays.stream(versions)
            .forEach(
                v -> jarEntries.put("IMPL-JARS/x-foo/x-foo-impl.jar/META-INF/versions/" + v + "/p/FooBar.class", classBytesForVersion(v))
            );

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { outerJar.toUri().toURL() },
            EmbeddedImplClassLoaderTests.class.getClassLoader()
        );

        EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "x-foo");
        Class<?> c = loader.loadClass("p.FooBar");
        return c.getConstructor().newInstance();
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

    public void testResourcesBasic() throws Exception {
        Path topLevelDir = createTempDir();

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/res/LISTING.TXT", "res-impl.jar".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/res/res-impl.jar/p/res.txt", "Hello World".getBytes(UTF_8));

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { outerJar.toUri().toURL() },
            EmbeddedImplClassLoaderTests.class.getClassLoader()
        );

        EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "res");
        URL url = loader.findResource("p/res.txt");
        assertThat(url.toString(), endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/p/res.txt"));

        var resources = loader.findResources("p/res.txt");
        assertThat(Collections.list(resources), contains(hasToString(endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/p/res.txt"))));
    }

    public void testResourcesWithoutMultiRelease() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        testResourcesVersioned(false, 0, 9);
        testResourcesVersioned(false, 0, 15);
        testResourcesVersioned(false, 0, 17);
        testResourcesVersioned(false, 0, 11, 10, 9, 8);
    }

    public void testResourcesVersioned9() throws Exception {
        assumeTrue("JDK version not greater than or equal to 9", Runtime.version().feature() >= 9);
        testResourcesVersioned(true, 9, 9);
        testResourcesVersioned(true, 9, 9, 8);
    }

    public void testResourcesVersioned11() throws Exception {
        assumeTrue("JDK version not greater than or equal to 11", Runtime.version().feature() >= 11);
        testResourcesVersioned(true, 11, 11);
        testResourcesVersioned(true, 11, 11, 10, 9, 8);
    }

    public void testResourcesVersioned17() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        testResourcesVersioned(true, 17, 17);
        testResourcesVersioned(true, 17, 17, 16, 15, 14, 13, 12, 11, 10, 9, 8);
    }

    private void testResourcesVersioned(boolean enableMulti, int expectedVersion, int... versions) throws Exception {
        Path topLevelDir = createTempDir();

        ClassLoader embedLoader, urlcLoader;
        {   // load content with URLClassLoader
            Map<String, byte[]> jarEntries = new HashMap<>();
            jarEntries.put("p/res.txt", "Hello World0".getBytes(UTF_8));
            if (enableMulti) {
                jarEntries.put("META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(UTF_8));
            }
            Arrays.stream(versions)
                .forEach(v -> jarEntries.put("META-INF/versions/" + v + "/p/res.txt", ("Hello World" + v).getBytes(UTF_8)));
            Path fooJar = topLevelDir.resolve("foo.jar");
            JarUtils.createJarWithEntries(fooJar, jarEntries);
            urlcLoader = URLClassLoader.newInstance(
                new URL[] { fooJar.toUri().toURL() },
                EmbeddedImplClassLoaderTests.class.getClassLoader()
            );
        }
        {   // load EQUIVALENT content with EmbeddedImplClassLoader
            Map<String, byte[]> jarEntries = new HashMap<>();
            jarEntries.put("IMPL-JARS/res/LISTING.TXT", "res-impl.jar".getBytes(UTF_8));
            jarEntries.put("IMPL-JARS/res/res-impl.jar/p/res.txt", "Hello World0".getBytes(UTF_8));
            if (enableMulti) {
                jarEntries.put("IMPL-JARS/res/res-impl.jar/META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(UTF_8));
            }
            Arrays.stream(versions)
                .forEach(
                    v -> jarEntries.put(
                        "IMPL-JARS/res/res-impl.jar/META-INF/versions/" + v + "/p/res.txt",
                        ("Hello World" + v).getBytes(UTF_8)
                    )
                );
            Path outerJar = topLevelDir.resolve("impl.jar");
            JarUtils.createJarWithEntries(outerJar, jarEntries);
            URLClassLoader parent = URLClassLoader.newInstance(
                new URL[] { outerJar.toUri().toURL() },
                EmbeddedImplClassLoaderTests.class.getClassLoader()
            );
            embedLoader = EmbeddedImplClassLoader.getInstance(parent, "res");
        }

        // finding resources with the different loaders should give EQUIVALENT results

        String expectedURLSuffix = "p/res.txt";
        if (enableMulti) {
            expectedURLSuffix = "META-INF/versions/" + expectedVersion + "/p/res.txt";
        }

        // getResource
        URL url1 = urlcLoader.getResource("p/res.txt");
        assertThat(url1.toString(), endsWith("!/" + expectedURLSuffix));
        URL url2 = embedLoader.getResource("p/res.txt");
        assertThat(url2.toString(), endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/" + expectedURLSuffix));
        assertThat(suffix(url2), endsWith(suffix(url1)));

        // findResources
        var urls1 = Collections.list(urlcLoader.getResources("p/res.txt")).stream().map(URL::toString).toList();
        var urls2 = Collections.list(embedLoader.getResources("p/res.txt")).stream().map(URL::toString).toList();
        assertThat("urls1=%s, urls2=%s".formatted(urls1, urls2), urls2, hasSize(1));
        assertThat(urls1.get(0), endsWith("!/" + expectedURLSuffix));
        assertThat(urls2.get(0), endsWith("impl.jar!/IMPL-JARS/res/res-impl.jar/" + expectedURLSuffix));

        // getResourceAsStream
        assertThat(new String(embedLoader.getResourceAsStream("p/res.txt").readAllBytes(), UTF_8), is("Hello World" + expectedVersion));
    }

    private static String suffix(URL url) {
        String urlStr = url.toString();
        int idx = urlStr.indexOf("!/");
        assert idx > 0;
        return urlStr.substring(idx + 2);
    }

    static final Class<NullPointerException> NPE = NullPointerException.class;
    static final Class<ClassNotFoundException> CNFE = ClassNotFoundException.class;

    public void testIDontHaveIt() throws Exception {
        Path topLevelDir = createTempDir();

        ClassLoader embedLoader;
        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/res/LISTING.TXT", "res-impl.jar".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/res/res-impl.jar/p/res.txt", "Hello World0".getBytes(UTF_8));
        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { outerJar.toUri().toURL() },
            EmbeddedImplClassLoaderTests.class.getClassLoader()
        );
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
    }

    public void testLoadWithJarDependencies() throws Exception {
        Map<String, CharSequence> sources = new HashMap<>();
        sources.put("p.Foo", "package p; public class Foo extends q.Bar { }");
        sources.put("q.Bar", "package q; public class Bar extends r.Baz { }");
        sources.put("r.Baz", "package r; public class Baz { }");

        var classToBytes = InMemoryJavaCompiler.compile(sources);
        Path topLevelDir = createTempDir();

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/blah/LISTING.TXT", "foo.jar\nbar.jar\nbaz.jar".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/blah/foo.jar/p/Foo.class", classToBytes.get("p.Foo"));
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/MANIFEST.MF", "Multi-Release: True\n".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/versions/9/q/Bar.class", classToBytes.get("q.Bar"));
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/versions/11/r/Baz.class", classToBytes.get("r.Baz"));

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries);
        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { outerJar.toUri().toURL() },
            EmbeddedImplClassLoaderTests.class.getClassLoader()
        );

        EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "blah");
        Class<?> c = loader.loadClass("p.Foo");
        Object obj = c.getConstructor().newInstance();
        assertThat(obj.toString(), startsWith("p.Foo"));
        assertThat(c.getSuperclass().getName(), is("q.Bar"));
        assertThat(c.getSuperclass().getSuperclass().getName(), is("r.Baz"));
    }

    public void testResourcesWithMultipleJars() throws Exception {
        Path topLevelDir = createTempDir();

        Map<String, String> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/blah/LISTING.TXT", "foo.jar\nbar.jar\nbaz.jar");
        jarEntries.put("IMPL-JARS/blah/foo.jar/res.txt", "fooRes");
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/MANIFEST.MF", "Multi-Release: TRUE\n");
        jarEntries.put("IMPL-JARS/blah/bar.jar/META-INF/versions/9/res.txt", "barRes");
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/MANIFEST.MF", "Multi-Release: trUE\n");
        jarEntries.put("IMPL-JARS/blah/baz.jar/META-INF/versions/11/res.txt", "bazRes");

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarWithEntries(outerJar, jarEntries, UTF_8);
        URLClassLoader parent = URLClassLoader.newInstance(
            new URL[] { outerJar.toUri().toURL() },
            EmbeddedImplClassLoaderTests.class.getClassLoader()
        );

        EmbeddedImplClassLoader loader = EmbeddedImplClassLoader.getInstance(parent, "blah");
        var res = Collections.list(loader.getResources("res.txt"));

        assertThat(res, hasSize(3));
        List<String> l = res.stream().map(EmbeddedImplClassLoaderTests::urlToString).toList();
        assertThat(l, containsInAnyOrder("fooRes", "barRes", "bazRes"));
    }

    @SuppressForbidden(reason = "file urls")
    static String urlToString(URL url) {
        try {
            return new String(url.openStream().readAllBytes(), UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
