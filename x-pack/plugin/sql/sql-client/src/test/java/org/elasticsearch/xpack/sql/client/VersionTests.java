/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.client;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.sql.proto.SqlVersion;

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

    public void testCurrent() {
        SqlVersion ver = SqlVersion.fromString(org.elasticsearch.Version.CURRENT.toString());
        assertEquals(ver, ClientVersion.CURRENT);
    }

    public void testInvalidVersion() {
        IllegalArgumentException err = expectThrows(IllegalArgumentException.class, () -> SqlVersion.fromString("7.1"));
        assertEquals("Invalid version format [7.1]", err.getMessage());
    }


    private static final String JAR_PATH_SEPARATOR = "!/";

    private static String versionString(byte[] parts) {
        StringBuffer version = new StringBuffer();
        for (byte part : parts) {
            version.append(".");
            version.append(part);
        }
        return version.substring(1);
    }

    private static byte[] randomVersion() {
        byte[] parts = new byte[3];
        for (int i = 0; i < parts.length; i ++) {
            parts[i] = (byte) randomIntBetween(0, SqlVersion.REVISION_MULTIPLIER - 1);
        }
        return parts;
    }

    private static Path createDriverJar(byte[] parts) throws IOException {
        final String JDBC_JAR_NAME = "es-sql-jdbc.jar";

        Path dir = createTempDir();
        Path jarPath = dir.resolve(JDBC_JAR_NAME); // simulated ES JDBC driver file

        Manifest jdbcJarManifest = new Manifest();
        Attributes attributes = jdbcJarManifest.getMainAttributes();
        attributes.put(Attributes.Name.MANIFEST_VERSION, "1.0.0");
        attributes.put(new Attributes.Name("Change"), "abc");
        attributes.put(new Attributes.Name("X-Compile-Elasticsearch-Version"), versionString(parts));

        // create the jdbc driver file
        try (JarOutputStream __ = new JarOutputStream(Files.newOutputStream(jarPath, StandardOpenOption.CREATE), jdbcJarManifest)) {}

        return jarPath;
    }

    public void testVersionFromFileJar() throws IOException {
        byte[] parts = randomVersion();
        Path jarPath = createDriverJar(parts);

        URL fileUrl = new URL(jarPath.toUri().toURL().toString());
        SqlVersion version = ClientVersion.extractVersion(fileUrl);

        assertEquals(parts[0], version.major);
        assertEquals(parts[1], version.minor);
        assertEquals(parts[2], version.revision);
        assertEquals(versionString(parts), version.version);
    }

    public void testVersionFromJar() throws IOException {
        byte[] parts = randomVersion();
        Path jarPath = createDriverJar(parts);

        URL jarUrl = new URL("jar:" + jarPath.toUri().toURL().toString() + JAR_PATH_SEPARATOR);
        SqlVersion version = ClientVersion.extractVersion(jarUrl);

        assertEquals(parts[0], version.major);
        assertEquals(parts[1], version.minor);
        assertEquals(parts[2], version.revision);
        assertEquals(versionString(parts), version.version);
    }

    public void testVersionFromJarInJar() throws IOException {
        byte[] parts = randomVersion();
        Path dir = createTempDir();
        Path jarPath = dir.resolve("uberjar.jar");          // simulated uberjar containing the jdbc driver
        Path innerJarPath = createDriverJar(parts);

        // create the uberjar and embed the jdbc driver one into it
        try (BufferedInputStream in = new BufferedInputStream(Files.newInputStream(innerJarPath));
                JarOutputStream out = new JarOutputStream(Files.newOutputStream(jarPath, StandardOpenOption.CREATE), new Manifest())) {
            JarEntry entry = new JarEntry(innerJarPath.getFileName() + JAR_PATH_SEPARATOR);
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

        URL jarInJar = new URL("jar:" + jarPath.toUri().toURL().toString() + JAR_PATH_SEPARATOR + innerJarPath.getFileName() +
            JAR_PATH_SEPARATOR);

        SqlVersion version = ClientVersion.extractVersion(jarInJar);
        assertEquals(parts[0], version.major);
        assertEquals(parts[1], version.minor);
        assertEquals(parts[2], version.revision);
        assertEquals(versionString(parts), version.version);
    }
}
