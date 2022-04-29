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
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.basePrefix;
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.rootURI;
import static org.hamcrest.Matchers.contains;
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

    public void testLoadWithMultiReleaseDisabled() throws Exception {
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
            // enable multi-release in the manifest
            jarEntries.put("IMPL-JARS/x-foo/x-foo-impl.jar/META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(UTF_8));
            for (int version : versions) {
                jarEntries.put(
                    "IMPL-JARS/x-foo/x-foo-impl.jar/META-INF/versions/" + version + "/p/FooBar.class",
                    classBytesForVersion(version)
                );
            }
        }

        Path outerJar = topLevelDir.resolve("impl.jar");
        JarUtils.createJarFile(outerJar, jarEntries);
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
        var enumerations = new Enumeration[] { Collections.enumeration(List.of("hello")), Collections.enumeration(List.of("world")) };
        var compoundEnumeration = new EmbeddedImplClassLoader.CompoundEnumeration<>(enumerations);

        List<String> l = new ArrayList<>();
        while (compoundEnumeration.hasMoreElements()) {
            l.add((String) compoundEnumeration.nextElement());
        }
        assertThat(l, contains(is("hello"), is("world")));
        expectThrows(NoSuchElementException.class, () -> compoundEnumeration.nextElement());
    }
}
