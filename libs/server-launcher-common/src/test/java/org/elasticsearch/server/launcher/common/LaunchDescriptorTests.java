/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.server.launcher.common;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class LaunchDescriptorTests extends ESTestCase {

    /**
     * Round-trip via DataOutputStream/DataInputStream: write descriptor to bytes, read back, assert all fields match.
     */
    public void testRoundTripViaStreams() throws IOException {
        LaunchDescriptor original = descriptorWithSampleData();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            original.writeTo(out);
        }
        LaunchDescriptor readBack;
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            readBack = LaunchDescriptor.readFrom(in);
        }
        assertDescriptorEquals(original, readBack);
    }

    /**
     * Round-trip via Path: write descriptor to a temp file, read back, assert all fields match.
     */
    public void testRoundTripViaPath() throws IOException {
        LaunchDescriptor original = descriptorWithSampleData();
        Path path = createTempDir().resolve(LaunchDescriptor.DESCRIPTOR_FILENAME);
        original.writeTo(path);
        LaunchDescriptor readBack = LaunchDescriptor.readFrom(path);
        assertDescriptorEquals(original, readBack);
    }

    /**
     * Minimal descriptor with empty lists, empty map, and small server args round-trips correctly.
     */
    public void testRoundTripMinimalDescriptor() throws IOException {
        LaunchDescriptor original = new LaunchDescriptor(
            "/usr/bin/java",
            List.of(),
            List.of(),
            Map.of(),
            "/var/log/elasticsearch",
            "/tmp/elasticsearch",
            false,
            new byte[] { 0x00, 0x01 }
        );
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            original.writeTo(out);
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            LaunchDescriptor readBack = LaunchDescriptor.readFrom(in);
            assertDescriptorEquals(original, readBack);
        }
    }

    /**
     * Reading from a stream with wrong magic number throws IOException with a clear message.
     */
    public void testInvalidMagicThrows() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (DataOutputStream out = new DataOutputStream(baos)) {
            out.writeInt(0xDEADBEEF);
            out.writeUTF("/usr/bin/java");
        }
        try (DataInputStream in = new DataInputStream(new ByteArrayInputStream(baos.toByteArray()))) {
            IOException e = expectThrows(IOException.class, () -> LaunchDescriptor.readFrom(in));
            assertThat(e, hasToString(containsString("bad magic number")));
        }
    }

    private static LaunchDescriptor descriptorWithSampleData() {
        return new LaunchDescriptor(
            "/opt/elasticsearch/jdk/bin/java",
            List.of("-Xms1g", "-Xmx1g", "-XX:+UseG1GC"),
            List.of("--module-path", "/opt/elasticsearch/lib", "-m", "org.elasticsearch.server/org.elasticsearch.bootstrap.Elasticsearch"),
            Map.of("ES_PATH_CONF", "/etc/elasticsearch", "ES_JAVA_OPTS", ""),
            "/var/log/elasticsearch",
            "/tmp/elasticsearch-12345",
            true,
            new byte[] { 1, 2, 3, 4, 5 }
        );
    }

    private static void assertDescriptorEquals(LaunchDescriptor expected, LaunchDescriptor actual) {
        assertThat(actual.command(), equalTo(expected.command()));
        assertThat(actual.jvmOptions(), equalTo(expected.jvmOptions()));
        assertThat(actual.jvmArgs(), equalTo(expected.jvmArgs()));
        assertThat(actual.environment(), equalTo(expected.environment()));
        assertThat(actual.workingDir(), equalTo(expected.workingDir()));
        assertThat(actual.tempDir(), equalTo(expected.tempDir()));
        assertThat(actual.daemonize(), equalTo(expected.daemonize()));
        assertTrue("serverArgsBytes mismatch", Arrays.equals(expected.serverArgsBytes(), actual.serverArgsBytes()));
    }
}
