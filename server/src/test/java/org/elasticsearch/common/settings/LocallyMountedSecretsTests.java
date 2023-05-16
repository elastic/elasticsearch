/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.settings;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
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

public class LocallyMountedSecretsTests extends ESTestCase {
    Environment env;

    private static String testJSON = """
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

    public void testProcessSettingsFile() throws IOException {
        writeTestFile(env.configFile().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
        assertThat(secrets.getVersion(), equalTo(1L));
        assertThat(secrets.getSettingNames(), containsInAnyOrder("aaa", "ccc"));
        assertEquals("bbb", secrets.getString("aaa").toString());
        assertEquals("ddd", secrets.getString("ccc").toString());
    }

    public void testSettingsGetFile() throws IOException, GeneralSecurityException {
        writeTestFile(env.configFile().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
        assertThat(secrets.getSettingNames(), containsInAnyOrder("aaa", "ccc"));
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
    }

    public void testSettingsSHADigest() throws IOException, GeneralSecurityException {
        writeTestFile(env.configFile().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);
        assertTrue(secrets.isLoaded());
        assertThat(secrets.getSettingNames(), containsInAnyOrder("aaa", "ccc"));

        final byte[] stringSettingHash = MessageDigest.getInstance("SHA-256").digest("bbb".getBytes(StandardCharsets.UTF_8));
        assertThat(secrets.getSHA256Digest("aaa"), equalTo(stringSettingHash));
    }

    public void testProcessBadSettingsFile() throws IOException {
        writeTestFile(env.configFile().resolve("secrets").resolve("secrets.json"), noMetadataJSON);
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> new LocallyMountedSecrets(env)).getMessage(),
            containsString("Required [metadata]")
        );
    }

    public void testSerializationWithSecrets() throws Exception {
        writeTestFile(env.configFile().resolve("secrets").resolve("secrets.json"), testJSON);
        LocallyMountedSecrets secrets = new LocallyMountedSecrets(env);

        final BytesStreamOutput out = new BytesStreamOutput();
        secrets.writeTo(out);
        final LocallyMountedSecrets fromStream = new LocallyMountedSecrets(out.bytes().streamInput());

        assertThat(fromStream.getSettingNames(), hasSize(2));
        assertThat(fromStream.getSettingNames(), containsInAnyOrder("aaa", "ccc"));

        assertEquals(secrets.getString("aaa"), fromStream.getString("aaa"));
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
        writeTestFile(env.configFile().resolve("secrets").resolve("secrets.json"), testJSON);
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

        Files.write(tempFilePath, contents.getBytes(StandardCharsets.UTF_8));
        Files.createDirectories(path.getParent());
        Files.move(tempFilePath, path, StandardCopyOption.ATOMIC_MOVE);
    }
}
