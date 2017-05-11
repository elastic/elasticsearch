/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.bootstrap;

import org.elasticsearch.Version;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class JarHellTests extends ESTestCase {

    URL makeJar(Path dir, String name, Manifest manifest, String... files) throws IOException {
        Path jarpath = dir.resolve(name);
        ZipOutputStream out;
        if (manifest == null) {
            out = new JarOutputStream(Files.newOutputStream(jarpath, StandardOpenOption.CREATE));
        } else {
            out = new JarOutputStream(Files.newOutputStream(jarpath, StandardOpenOption.CREATE), manifest);
        }
        for (String file : files) {
            out.putNextEntry(new ZipEntry(file));
        }
        out.close();
        return jarpath.toUri().toURL();
    }

    URL makeFile(Path dir, String name) throws IOException {
        Path filepath = dir.resolve(name);
        Files.newOutputStream(filepath, StandardOpenOption.CREATE).close();
        return dir.toUri().toURL();
    }

    public void testDifferentJars() throws Exception {
        Path dir = createTempDir();
        Set<URL> jars = asSet(makeJar(dir, "foo.jar", null, "DuplicateClass.class"),
                              makeJar(dir, "bar.jar", null, "DuplicateClass.class"));
        try {
            JarHell.checkJarHell(jars);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("foo.jar"));
            assertTrue(e.getMessage().contains("bar.jar"));
        }
    }

    public void testDirsOnClasspath() throws Exception {
        Path dir1 = createTempDir();
        Path dir2 = createTempDir();
        Set<URL> dirs = asSet(makeFile(dir1, "DuplicateClass.class"),
                              makeFile(dir2, "DuplicateClass.class"));
        try {
            JarHell.checkJarHell(dirs);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains(dir1.toString()));
            assertTrue(e.getMessage().contains(dir2.toString()));
        }
    }

    public void testDirAndJar() throws Exception {
        Path dir1 = createTempDir();
        Path dir2 = createTempDir();
        Set<URL> dirs = asSet(makeJar(dir1, "foo.jar", null, "DuplicateClass.class"),
                              makeFile(dir2, "DuplicateClass.class"));
        try {
            JarHell.checkJarHell(dirs);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("foo.jar"));
            assertTrue(e.getMessage().contains(dir2.toString()));
        }
    }

    public void testWithinSingleJar() throws Exception {
        // the java api for zip file does not allow creating duplicate entries (good!) so
        // this bogus jar had to be with https://github.com/jasontedor/duplicate-classes
        Set<URL> jars = Collections.singleton(JarHellTests.class.getResource("duplicate-classes.jar"));
        try {
            JarHell.checkJarHell(jars);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("duplicate-classes.jar"));
            assertTrue(e.getMessage().contains("exists multiple times in jar"));
        }
    }

    public void testXmlBeansLeniency() throws Exception {
        Set<URL> jars = Collections.singleton(JarHellTests.class.getResource("duplicate-xmlbeans-classes.jar"));
        JarHell.checkJarHell(jars);
    }

    public void testRequiredJDKVersionTooOld() throws Exception {
        Path dir = createTempDir();
        List<Integer> current = JavaVersion.current().getVersion();
        List<Integer> target = new ArrayList<>(current.size());
        for (int i = 0; i < current.size(); i++) {
            target.add(current.get(i) + 1);
        }
        JavaVersion targetVersion = JavaVersion.parse(Strings.collectionToDelimitedString(target, "."));


        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), targetVersion.toString());
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        try {
            JarHell.checkJarHell(jars);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("requires Java " + targetVersion.toString()));
            assertTrue(e.getMessage().contains("your system: " + JavaVersion.current().toString()));
        }
    }

    public void testBadJDKVersionInJar() throws Exception {
        Path dir = createTempDir();
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), "bogus");
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        try {
            JarHell.checkJarHell(jars);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().equals("version string must be a sequence of nonnegative decimal integers separated by \".\"'s and may have leading zeros but was bogus"));
        }
    }

    public void testRequiredJDKVersionIsOK() throws Exception {
        Path dir = createTempDir();
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), "1.7");
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        JarHell.checkJarHell(jars);
    }

    /** make sure if a plugin is compiled against the same ES version, it works */
    public void testGoodESVersionInJar() throws Exception {
        Path dir = createTempDir();
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Elasticsearch-Version"), Version.CURRENT.toString());
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        JarHell.checkJarHell(jars);
    }

    /** make sure if a plugin is compiled against a different ES version, it fails */
    public void testBadESVersionInJar() throws Exception {
        Path dir = createTempDir();
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Elasticsearch-Version"), "1.0-bogus");
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        try {
            JarHell.checkJarHell(jars);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("requires Elasticsearch 1.0-bogus"));
        }
    }

    public void testValidVersions() {
        String[] versions = new String[]{"1.7", "1.7.0", "0.1.7", "1.7.0.80"};
        for (String version : versions) {
            try {
                JarHell.checkVersionFormat(version);
            } catch (IllegalStateException e) {
                fail(version + " should be accepted as a valid version format");
            }
        }
    }

    public void testInvalidVersions() {
        String[] versions = new String[]{"", "1.7.0_80", "1.7."};
        for (String version : versions) {
            try {
                JarHell.checkVersionFormat(version);
                fail("\"" + version + "\"" + " should be rejected as an invalid version format");
            } catch (IllegalStateException e) {
            }
        }
    }

    // classpath testing is system specific, so we just write separate tests for *nix and windows cases

    /**
     * Parse a simple classpath with two elements on unix
     */
    public void testParseClassPathUnix() throws Exception {
        assumeTrue("test is designed for unix-like systems only", ":".equals(System.getProperty("path.separator")));
        assumeTrue("test is designed for unix-like systems only", "/".equals(System.getProperty("file.separator")));

        Path element1 = createTempDir();
        Path element2 = createTempDir();

        Set<URL> expected = asSet(element1.toUri().toURL(), element2.toUri().toURL());
        assertEquals(expected, JarHell.parseClassPath(element1.toString() + ":" + element2.toString()));
    }

    /**
     * Make sure an old unix classpath with an empty element (implicitly CWD: i'm looking at you 1.x ES scripts) fails
     */
    public void testEmptyClassPathUnix() throws Exception {
        assumeTrue("test is designed for unix-like systems only", ":".equals(System.getProperty("path.separator")));
        assumeTrue("test is designed for unix-like systems only", "/".equals(System.getProperty("file.separator")));

        try {
            JarHell.parseClassPath(":/element1:/element2");
            fail("should have hit exception");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("should not contain empty elements"));
        }
    }

    /**
     * Parse a simple classpath with two elements on windows
     */
    public void testParseClassPathWindows() throws Exception {
        assumeTrue("test is designed for windows-like systems only", ";".equals(System.getProperty("path.separator")));
        assumeTrue("test is designed for windows-like systems only", "\\".equals(System.getProperty("file.separator")));

        Path element1 = createTempDir();
        Path element2 = createTempDir();

        Set<URL> expected = asSet(element1.toUri().toURL(), element2.toUri().toURL());
        assertEquals(expected, JarHell.parseClassPath(element1.toString() + ";" + element2.toString()));
    }

    /**
     * Make sure an old windows classpath with an empty element (implicitly CWD: i'm looking at you 1.x ES scripts) fails
     */
    public void testEmptyClassPathWindows() throws Exception {
        assumeTrue("test is designed for windows-like systems only", ";".equals(System.getProperty("path.separator")));
        assumeTrue("test is designed for windows-like systems only", "\\".equals(System.getProperty("file.separator")));

        try {
            JarHell.parseClassPath(";c:\\element1;c:\\element2");
            fail("should have hit exception");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("should not contain empty elements"));
        }
    }

    /**
     * Make sure a "bogus" windows classpath element is accepted, java's classpath parsing accepts it,
     * therefore eclipse OSGI code does it :)
     */
    public void testCrazyEclipseClassPathWindows() throws Exception {
        assumeTrue("test is designed for windows-like systems only", ";".equals(System.getProperty("path.separator")));
        assumeTrue("test is designed for windows-like systems only", "\\".equals(System.getProperty("file.separator")));

        Set<URL> expected = asSet(
            PathUtils.get("c:\\element1").toUri().toURL(),
            PathUtils.get("c:\\element2").toUri().toURL(),
            PathUtils.get("c:\\element3").toUri().toURL(),
            PathUtils.get("c:\\element 4").toUri().toURL()
        );
        Set<URL> actual = JarHell.parseClassPath("c:\\element1;c:\\element2;/c:/element3;/c:/element 4");
        assertEquals(expected, actual);
    }
}
