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

import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.core.internal.provider.EmbeddedImplClassLoader.basePrefix;
import static org.hamcrest.Matchers.equalTo;

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

        Object foobar = newFooBar(0, false);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(9, false);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(11, false);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(17, false);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));
    }

    public void testLoadMegaVersionWithMultiReleaseEnabled() throws Exception {
        // expect root FooBar to be loaded each time

        assumeTrue("JDK version not less than 10_000", Runtime.version().feature() < 10_000);
        Object foobar = newFooBar(10_000, true);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(10_001, true);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));

        foobar = newFooBar(100_000, true);
        assertThat(foobar.toString(), equalTo("FooBar " + 0));
    }

    public void testLoadWithMultiReleaseEnabled9() throws Exception {
        assumeTrue("JDK version not greater than or equal to 9", Runtime.version().feature() >= 9);
        Object foobar = newFooBar(9, true);
        // expect 9 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 9));
    }

    public void testLoadWithMultiReleaseEnabled11() throws Exception {
        assumeTrue("JDK version not greater than or equal to 11", Runtime.version().feature() >= 11);
        Object foobar = newFooBar(11, true);
        // expect 11 version of FooBar to be loaded
        assertThat(foobar.toString(), equalTo("FooBar " + 11));
    }

    public void testLoadWithMultiReleaseEnabled17() throws Exception {
        assumeTrue("JDK version not greater than or equal to 17", Runtime.version().feature() >= 17);
        Object foobar = newFooBar(17, true);
        // expect 11 version of FooBar to be loaded
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

    static Object newFooBar(int version, boolean enableMulti) throws Exception {
        Path topLevelDir = createTempDir();

        Map<String, byte[]> jarEntries = new HashMap<>();
        jarEntries.put("IMPL-JARS/x-foo/LISTING.TXT", "x-foo-impl.jar".getBytes(UTF_8));
        jarEntries.put("IMPL-JARS/x-foo/x-foo-impl.jar/p/FooBar.class", classBytesForVersion(0)); // root version is always 0
        if (enableMulti) {
            // enable multi-release in the manifest
            jarEntries.put("IMPL-JARS/x-foo/x-foo-impl.jar/META-INF/MANIFEST.MF", "Multi-Release: true\n".getBytes(UTF_8));
            jarEntries.put(
                "IMPL-JARS/x-foo/x-foo-impl.jar/META-INF/versions/" + version + "/p/FooBar.class",
                classBytesForVersion(version)
            );
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
}
