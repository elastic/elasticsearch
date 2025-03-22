/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

public class LocallyMountedSecretsTests extends ESTestCase {
    Environment env;

    private static String testJSONDepricated = """
        {
             "metadata": {
                 "version": "1",
                 "compatibility": "8.4.0"
             },
             "secrets": {
                 "aaa": "bbb",
                 "ccc": "ddd"
             }
        }""";

    // "ZmZm" is the base64 encoding of "fff"
    private static String testJSON = """
        {
             "metadata": {
                 "version": "1",
                 "compatibility": "8.4.0"
             },
             "string_secrets": {
                 "aaa": "bbb",
                 "ccc": "ddd"
             },
             "file_secrets": {
                 "eee": "ZmZm"
             }
        }""";

    // "ZmZm" is the base64 encoding of "fff"
    private static String testJSONDuplicateKeys = """
        {
             "metadata": {
                 "version": "1",
                 "compatibility": "8.4.0"
             },
             "string_secrets": {
                 "aaa": "bbb",
                 "ccc": "ddd",
                 "eee": "fff"
             },
             "file_secrets": {
                 "eee": "ZmZm"
             }
        }""";

    private static String noMetadataJSON = """
        {
             "secrets": {
                 "aaa": "bbb",
                 "ccc": "ddd"
             }
        }""";

    @Before
    public void setupEnv() {
        env = newEnvironment();
    }

    public void testCreate() {
        SecureSettings secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
    }

    public void testProcessSettingsFile() throws Exception {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
        assertThat(secrets.getVersion(), equalTo(1L));
        assertThat(secrets.getSettingNames(), containsInAnyOrder("aaa", "ccc", "eee"));
        assertEquals("bbb", secrets.getString("aaa").toString());
        assertEquals("ddd", secrets.getString("ccc").toString());

        assertEquals("fff", new String(secrets.getFile("eee").readAllBytes(), StandardCharsets.UTF_8));
    }

    public void testProcessDeprecatedSettingsFile() throws Exception {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), testJSONDepricated);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
        assertThat(secrets.getVersion(), equalTo(1L));
        assertThat(secrets.getSettingNames(), containsInAnyOrder("aaa", "ccc"));
        assertEquals("bbb", secrets.getString("aaa").toString());
        assertEquals("ddd", secrets.getString("ccc").toString());
    }

    public void testDuplicateSettingKeys() throws Exception {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), testJSONDuplicateKeys);
        Exception e = expectThrows(Exception.class, () -> new LocallyMountedSecrets(env));
        assertThat(e, instanceOf(XContentParseException.class));
        assertThat(e.getMessage(), containsString("failed to parse field"));

        Throwable cause1 = e.getCause();
        assertThat(cause1, instanceOf(XContentParseException.class));
        assertThat(cause1.getMessage(), containsString("Failed to build [locally_mounted_secrets]"));

        Throwable cause2 = cause1.getCause();
        assertThat(cause2, instanceOf(IllegalStateException.class));
        assertThat(cause2.getMessage(), containsString("Some settings were defined as both string and file settings"));
    }

    public void testSettingsGetFile() throws IOException, GeneralSecurityException {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
        assertThat(secrets.getSettingNames(), containsInAnyOrder("aaa", "ccc", "eee"));

        // we can read string settings as "files"
        try (InputStream stream = secrets.getFile("aaa")) {
            for (int i = 0; i < 3; ++i) {
                int got = stream.read();
                if (got < 0) {
                    fail("Expected 3 bytes but read " + i);
                }
                assertEquals('b', got);
            }
            assertEquals(-1, stream.read()); // nothing left
        }

        // but really we want to read file settings
        try (InputStream stream = secrets.getFile("eee")) {
            for (int i = 0; i < 3; ++i) {
                int got = stream.read();
                if (got < 0) {
                    fail("Expected 3 bytes but read " + i);
                }
                assertEquals('f', got);
            }
            assertEquals(-1, stream.read()); // nothing left
        }
    }

    public void testSettingsSHADigest() throws IOException, GeneralSecurityException {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
        assertThat(secrets.getSettingNames(), containsInAnyOrder("aaa", "ccc", "eee"));

        final byte[] stringSettingHash = MessageDigest.getInstance("SHA-256").digest("bbb".getBytes(StandardCharsets.UTF_8));
        assertThat(secrets.getSHA256Digest("aaa"), equalTo(stringSettingHash));

        final byte[] fileSettingHash = MessageDigest.getInstance("SHA-256").digest("fff".getBytes(StandardCharsets.UTF_8));
        assertThat(secrets.getSHA256Digest("eee"), equalTo(fileSettingHash));
    }

    public void testProcessBadSettingsFile() throws IOException {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), noMetadataJSON);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new LocallyMountedSecrets(env)).getMessage(),
            containsString("Required [metadata]")
        );
    }

    public void testSerializationWithSecrets() throws Exception {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);

        final BytesStreamOutput out = new BytesStreamOutput();
        secrets.writeTo(out);
        final LocallyMountedSecrets fromStream = new LocallyMountedSecrets(out.bytes().streamInput());

        assertThat(fromStream.getSettingNames(), hasSize(3));
        assertThat(fromStream.getSettingNames(), containsInAnyOrder("aaa", "ccc", "eee"));

        assertEquals(secrets.getString("aaa"), fromStream.getString("aaa"));
        assertThat(secrets.getFile("eee").readAllBytes(), equalTo(fromStream.getFile("eee").readAllBytes()));
        assertTrue(fromStream.isLoaded());

    }

    public void testSerializationNewlyCreated() throws Exception {
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);

        final BytesStreamOutput out = new BytesStreamOutput();
        secrets.writeTo(out);
        final LocallyMountedSecrets fromStream = new LocallyMountedSecrets(out.bytes().streamInput());

        assertTrue(fromStream.isLoaded());
    }

    public void testClose() throws IOException {
        writeTestFile(env.configDir().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertEquals("bbb", secrets.getString("aaa").toString());
        assertEquals("ddd", secrets.getString("ccc").toString());
        secrets.close();
        assertNull(secrets.getString("aaa"));
        assertNull(secrets.getString("ccc"));
    }

    public void testResolveSecretsDir() {
        assertTrue(LocallyMountedSecrets.resolveSecretsDir(env).endsWith("config/" + LocallyMountedSecrets.SECRETS_DIRECTORY));
    }

    public void testResolveSecretsFile() {
        assertTrue(
            LocallyMountedSecrets.resolveSecretsFile(env)
                .endsWith("config/" + LocallyMountedSecrets.SECRETS_DIRECTORY + "/" + LocallyMountedSecrets.SECRETS_FILE_NAME)
        );
    }

    private void writeTestFile(Path path, String contents) throws IOException {
        Path tempFilePath = createTempFile();

        Files.writeString(tempFilePath, contents);
        Files.createDirectories(path.getParent());
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }
}
