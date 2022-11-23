/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.jdk;

import org.elasticsearch.common.Strings;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.lang.Runtime.Version;
import java.lang.module.ModuleFinder;
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
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;

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
        Set<URL> jars = asSet(makeJar(dir, "foo.jar", null, "DuplicateClass.class"), makeJar(dir, "bar.jar", null, "DuplicateClass.class"));
        try {
            JarHell.checkJarHell(jars, logger::debug);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("foo.jar"));
            assertTrue(e.getMessage().contains("bar.jar"));
        }
    }

    public void testModuleInfo() throws Exception {
        Path dir = createTempDir();
        JarHell.checkJarHell(
            asSet(makeJar(dir, "foo.jar", null, "module-info.class"), makeJar(dir, "bar.jar", null, "module-info.class")),
            logger::debug
        );
    }

    public void testModuleInfoPackage() throws Exception {
        Path dir = createTempDir();
        JarHell.checkJarHell(
            asSet(makeJar(dir, "foo.jar", null, "foo/bar/module-info.class"), makeJar(dir, "bar.jar", null, "foo/bar/module-info.class")),
            logger::debug
        );
    }

    public void testDirsOnClasspath() throws Exception {
        Path dir1 = createTempDir();
        Path dir2 = createTempDir();
        Set<URL> dirs = asSet(makeFile(dir1, "DuplicateClass.class"), makeFile(dir2, "DuplicateClass.class"));
        try {
            JarHell.checkJarHell(dirs, logger::debug);
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
        Set<URL> dirs = asSet(makeJar(dir1, "foo.jar", null, "DuplicateClass.class"), makeFile(dir2, "DuplicateClass.class"));
        try {
            JarHell.checkJarHell(dirs, logger::debug);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("foo.jar"));
            assertTrue(e.getMessage().contains(dir2.toString()));
        }
    }

    public void testNonJDKModuleURLs() throws Throwable {
        var bootLayer = ModuleLayer.boot();

        Path fooDir = createTempDir(getTestName());
        Path fooJar = PathUtils.get(makeJar(fooDir, "foo.jar", null, "p/Foo.class").toURI());
        var fooConfiguration = bootLayer.configuration().resolve(ModuleFinder.of(), ModuleFinder.of(fooJar), List.of("foo"));
        Set<URL> urls = JarHell.nonJDKModuleURLs(fooConfiguration).collect(Collectors.toSet());
        assertThat(urls.size(), equalTo(1));
        assertThat(urls.stream().findFirst().get().toString(), endsWith("foo.jar"));

        Path barDir = createTempDir();
        Path barJar = PathUtils.get(makeJar(barDir, "bar.jar", null, "q/Bar.class").toURI());
        var barConfiguration = fooConfiguration.resolve(ModuleFinder.of(), ModuleFinder.of(barJar), List.of("bar"));
        urls = JarHell.nonJDKModuleURLs(barConfiguration).collect(Collectors.toSet());
        assertThat(urls.size(), equalTo(2));
        assertThat(urls.stream().map(URL::toString).toList(), hasItems(endsWith("foo.jar"), endsWith("bar.jar")));
    }

    public void testWithinSingleJar() throws Exception {
        // the java api for zip file does not allow creating duplicate entries (good!) so
        // this bogus jar had to be with https://github.com/jasontedor/duplicate-classes
        Set<URL> jars = Collections.singleton(JarHellTests.class.getResource("duplicate-classes.jar"));
        try {
            JarHell.checkJarHell(jars, logger::debug);
            fail("did not get expected exception");
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("duplicate-classes.jar"));
            assertTrue(e.getMessage().contains("exists multiple times in jar"));
        }
    }

    public void testRequiredJDKVersionTooOld() throws Exception {
        Path dir = createTempDir();
        List<Integer> current = Runtime.version().version();
        List<Integer> target = new ArrayList<>(current.size());
        for (int i = 0; i < current.size(); i++) {
            target.add(current.get(i) + 1);
        }
        Version targetVersion = Version.parse(Strings.collectionToDelimitedString(target, "."));

        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), targetVersion.toString());
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        var e = expectThrows(IllegalStateException.class, () -> JarHell.checkJarHell(jars, logger::debug));
        assertThat(e.getMessage(), containsString("requires Java " + targetVersion));
        assertThat(e.getMessage(), containsString("your system: " + Runtime.version().toString()));
    }

    public void testBadJDKVersionInJar() throws Exception {
        Path dir = createTempDir();
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), "bogus");
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        var e = expectThrows(IllegalArgumentException.class, () -> JarHell.checkJarHell(jars, logger::debug));
        assertThat(e.getMessage(), equalTo("Invalid version string: 'bogus'"));
    }

    public void testRequiredJDKVersionIsOK() throws Exception {
        Path dir = createTempDir();
        Manifest manifest = new Manifest();
        Attributes attributes = manifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("X-Compile-Target-JDK"), "1.7");
        Set<URL> jars = Collections.singleton(makeJar(dir, "foo.jar", manifest, "Foo.class"));
        JarHell.checkJarHell(jars, logger::debug);
    }

    public void testInvalidVersions() {
        String[] versions = new String[] { "", "1.7.0_80", "1.7." };
        for (String version : versions) {
            expectThrows(IllegalArgumentException.class, () -> JarHell.checkJavaVersion("foo", version));
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
