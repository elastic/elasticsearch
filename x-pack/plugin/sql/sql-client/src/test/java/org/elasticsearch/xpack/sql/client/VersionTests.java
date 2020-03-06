/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.test.ESTestCase;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class VersionTests extends ESTestCase {
    public void test70Version() {
        byte[] ver = Version.from("7.0.0-alpha");
        assertEquals(7, ver[0]);
        assertEquals(0, ver[1]);
        assertEquals(0, ver[2]);
    }

    public void test712Version() {
        byte[] ver = Version.from("7.1.2");
        assertEquals(7, ver[0]);
        assertEquals(1, ver[1]);
        assertEquals(2, ver[2]);
    }

    public void testCurrent() {
        Version ver = Version.fromString(org.elasticsearch.Version.CURRENT.toString());
        assertEquals(org.elasticsearch.Version.CURRENT.major, ver.major);
        assertEquals(org.elasticsearch.Version.CURRENT.minor, ver.minor);
        assertEquals(org.elasticsearch.Version.CURRENT.revision, ver.revision);
    }

    public void testFromString() {
        Version ver = Version.fromString("1.2.3");
        assertEquals(1, ver.major);
        assertEquals(2, ver.minor);
        assertEquals(3, ver.revision);
        assertEquals("Unknown", ver.hash);
        assertEquals("1.2.3", ver.version);
    }

    public void testInvalidVersion() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> Version.from("7.1"));
        assertEquals("Invalid version 7.1", err.getMessage());
    }
    
    public void testVersionFromJarInJar() throws IOException {
        final String JDBC_JAR_NAME = "es-sql-jdbc.jar";
        final String JAR_PATH_SEPARATOR = "!/";
        
        Path dir = createTempDir();
        Path jarPath = dir.resolve("uberjar.jar");          // simulated uberjar containing the jdbc driver
        Path innerJarPath = dir.resolve(JDBC_JAR_NAME); // simulated ES JDBC driver file

        Manifest jdbcJarManifest = new Manifest();
        Attributes attributes = jdbcJarManifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("Change"), "abc");
        attributes.put(new Attributes.Name("X-Compile-Elasticsearch-Version"), "1.2.3");

        // create the jdbc driver file
        try (JarOutputStream jdbc = new JarOutputStream(Files.newOutputStream(innerJarPath, StandardOpenOption.CREATE), jdbcJarManifest)) {}

        // create the uberjar and embed the jdbc driver one into it
        try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(innerJarPath));
                JarOutputStream out = new JarOutputStream(Files.newOutputStream(jarPath, StandardOpenOption.CREATE), new Manifest())) {
            JarEntry entry = new JarEntry(JDBC_JAR_NAME + JAR_PATH_SEPARATOR);
            out.putNextEntry(entry);

            byte[] buffer = new byte[1024];
            while (true) {
                int count = in.read(buffer);
                if (count == -1) {
                    break;
                }
                out.write(buffer, 0, count);
            }
        }
        
        URL jarInJar = new URL("jar:" + jarPath.toUri().toURL().toString() + JAR_PATH_SEPARATOR + JDBC_JAR_NAME + JAR_PATH_SEPARATOR);
        
        Version version = Version.extractVersion(jarInJar);
        assertEquals(1, version.major);
        assertEquals(2, version.minor);
        assertEquals(3, version.revision);
        assertEquals("abc", version.hash);
        assertEquals("1.2.3", version.version);
    }
}
