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

import org.elasticsearch.test.ElasticsearchTestCase;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class JarHellTests extends ElasticsearchTestCase {

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
        return filepath.toUri().toURL();
    }

    public void testDifferentJars() throws Exception {
        Path dir = createTempDir();
        URL[] jars = {makeJar(dir, "foo.jar", null, "DuplicateClass.class"), makeJar(dir, "bar.jar", null, "DuplicateClass.class")};
        try {
            JarHell.checkJarHell(jars);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("foo.jar"));
            assertTrue(e.getMessage().contains("bar.jar"));
        }
    }

    public void testBootclasspathLeniency() throws Exception {
        Path dir = createTempDir();
        String previousJavaHome = System.getProperty("java.home");
        System.setProperty("java.home", dir.toString());
        URL[] jars = {makeJar(dir, "foo.jar", null, "DuplicateClass.class"), makeJar(dir, "bar.jar", null, "DuplicateClass.class")};
        try {
            JarHell.checkJarHell(jars);
        } finally {
            System.setProperty("java.home", previousJavaHome);
        }
    }

    public void testDuplicateClasspathLeniency() throws Exception {
        Path dir = createTempDir();
        URL jar = makeJar(dir, "foo.jar", null, "Foo.class");
        URL[] jars = {jar, jar};
        JarHell.checkJarHell(jars);
    }

    public void testDirsOnClasspath() throws Exception {
        Path dir1 = createTempDir();
        Path dir2 = createTempDir();
        URL[] dirs = {makeFile(dir1, "DuplicateClass.class"), makeFile(dir2, "DuplicateClass.class")};
        try {
            JarHell.checkJarHell(dirs);
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
        URL[] dirs = {makeJar(dir1, "foo.jar", null, "DuplicateClass.class"), makeFile(dir2, "DuplicateClass.class")};
        try {
            JarHell.checkJarHell(dirs);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("foo.jar"));
            assertTrue(e.getMessage().contains(dir2.toString()));
        }
    }

    public void testLog4jLeniency() throws Exception {
        Path dir = createTempDir();
        URL[] jars = {makeJar(dir, "foo.jar", null, "org/apache/log4j/DuplicateClass.class"), makeJar(dir, "bar.jar", null, "org/apache/log4j/DuplicateClass.class")};
        JarHell.checkJarHell(jars);
    }

    public void testBaseDateTimeLeniency() throws Exception {
        Path dir = createTempDir();
        URL[] jars = {makeJar(dir, "foo.jar", null, "org/joda/time/base/BaseDateTime.class"), makeJar(dir, "bar.jar", null, "org/joda/time/base/BaseDateTime.class")};
        JarHell.checkJarHell(jars);
    }

    public void testWithinSingleJar() throws Exception {
        // the java api for zip file does not allow creating duplicate entries (good!) so
        // this bogus jar had to be constructed with ant
        URL[] jars = {JarHellTests.class.getResource("duplicate-classes.jar")};
        try {
            JarHell.checkJarHell(jars);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("jar hell!"));
            assertTrue(e.getMessage().contains("DuplicateClass"));
            assertTrue(e.getMessage().contains("duplicate-classes.jar"));
            assertTrue(e.getMessage().contains("exists multiple times in jar"));
        }
    }

    public void testXmlBeansLeniency() throws Exception {
        URL[] jars = {JarHellTests.class.getResource("duplicate-xmlbeans-classes.jar")};
        JarHell.checkJarHell(jars);
    }

    public void testRequiredJDKVersionTooOld() throws Exception {
        Path dir = createTempDir();
        String previousJavaVersion = System.getProperty("java.specification.version");
        System.setProperty("java.specification.version", "1.7");

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(new Attributes.Name("X-Compile-Target-JDK"), "1.8");
        URL[] jars = {makeJar(dir, "foo.jar", manifest, "Foo.class")};
        try {
            JarHell.checkJarHell(jars);
        } catch (IllegalStateException e) {
            assertTrue(e.getMessage().contains("requires java 1.8"));
            assertTrue(e.getMessage().contains("your system: 1.7"));
        } finally {
            System.setProperty("java.specification.version", previousJavaVersion);
        }
    }

    public void testRequiredJDKVersionIsOK() throws Exception {
        Path dir = createTempDir();
        String previousJavaVersion = System.getProperty("java.specification.version");
        System.setProperty("java.specification.version", "1.7");

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(new Attributes.Name("X-Compile-Target-JDK"), "1.7");
        URL[] jars = {makeJar(dir, "foo.jar", manifest, "Foo.class")};
        try {
            JarHell.checkJarHell(jars);
        } finally {
            System.setProperty("java.specification.version", previousJavaVersion);
        }
    }

    public void testBadJDKVersionProperty() throws Exception {
        Path dir = createTempDir();
        String previousJavaVersion = System.getProperty("java.specification.version");
        System.setProperty("java.specification.version", "bogus");

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(new Attributes.Name("X-Compile-Target-JDK"), "1.7");
        URL[] jars = {makeJar(dir, "foo.jar", manifest, "Foo.class")};
        try {
            JarHell.checkJarHell(jars);
        } finally {
            System.setProperty("java.specification.version", previousJavaVersion);
        }
    }

    public void testBadJDKVersionInJar() throws Exception {
        Path dir = createTempDir();
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(new Attributes.Name("X-Compile-Target-JDK"), "bogus");
        URL[] jars = {makeJar(dir, "foo.jar", manifest, "Foo.class")};
        JarHell.checkJarHell(jars);
    }
}
