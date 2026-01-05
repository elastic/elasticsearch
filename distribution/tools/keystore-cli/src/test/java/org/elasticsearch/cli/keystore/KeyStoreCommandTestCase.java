/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cli.keystore;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;

import org.apache.lucene.tests.util.LuceneTestCase;
import org.elasticsearch.cli.CommandTestCase;
import org.elasticsearch.common.settings.KeyStoreWrapper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.PathUtilsForTesting;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystem;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

/**
 * Base test case for manipulating the ES keystore.
 */
@LuceneTestCase.SuppressFileSystems("*") // we do our own mocking
public abstract class KeyStoreCommandTestCase extends CommandTestCase {

    Environment env;

    List<FileSystem> fileSystems = new ArrayList<>();

    @After
    public void closeMockFileSystems() throws IOException {
        IOUtils.close(fileSystems);
    }

    @Before
    public void setupEnv() throws IOException {
        env = setupEnv(true, fileSystems); // default to posix, but tests may call setupEnv(false) to overwrite
    }

    public static Environment setupEnv(boolean posix, List<FileSystem> fileSystems) throws IOException {
        final Configuration configuration;
        if (posix) {
            configuration = Configuration.unix().toBuilder().setAttributeViews("basic", "owner", "posix", "unix").build();
        } else {
            configuration = Configuration.unix();
        }
        FileSystem fs = Jimfs.newFileSystem(configuration);
        fileSystems.add(fs);
        PathUtilsForTesting.installMock(fs); // restored by restoreFileSystem in ESTestCase
        Path home = fs.getPath("/", "test-home");
        Files.createDirectories(home.resolve("config"));
        return TestEnvironment.newEnvironment(Settings.builder().put("path.home", home).build());
    }

    KeyStoreWrapper createKeystore(String password, String... settings) throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.create();
        assertEquals(0, settings.length % 2);
        for (int i = 0; i < settings.length; i += 2) {
            keystore.setString(settings[i], settings[i + 1].toCharArray());
        }
        saveKeystore(keystore, password);
        return keystore;
    }

    void saveKeystore(KeyStoreWrapper keystore, String password) throws Exception {
        keystore.save(env.configDir(), password.toCharArray());
    }

    KeyStoreWrapper loadKeystore(String password) throws Exception {
        KeyStoreWrapper keystore = KeyStoreWrapper.load(env.configDir());
        keystore.decrypt(password.toCharArray());
        return keystore;
    }

    void assertSecureString(String setting, String value, String password) throws Exception {
        assertSecureString(loadKeystore(password), setting, value);
    }

    void assertSecureString(KeyStoreWrapper keystore, String setting, String value) throws Exception {
        assertEquals(value, keystore.getString(setting).toString());
    }

    void assertSecureFile(String setting, Path file, String password) throws Exception {
        assertSecureFile(loadKeystore(password), setting, file);
    }

    void assertSecureFile(KeyStoreWrapper keystore, String setting, Path file) throws Exception {
        byte[] expectedBytes = Files.readAllBytes(file);
        try (InputStream input = keystore.getFile(setting)) {
            for (int i = 0; i < expectedBytes.length; ++i) {
                int got = input.read();
                int expected = Byte.toUnsignedInt(expectedBytes[i]);
                if (got < 0) {
                    fail("Got EOF from keystore stream at position " + i + " but expected 0x" + Integer.toHexString(expected));
                }
                assertEquals("Byte " + i, expected, got);
            }
            int eof = input.read();
            if (eof != -1) {
                fail(
                    "Found extra bytes in file stream from keystore, expected "
                        + expectedBytes.length
                        + " bytes but found 0x"
                        + Integer.toHexString(eof)
                );
            }
        }

    }

    String getPossibleKeystorePassword() {
        if (inFipsJvm()) {
            // FIPS Mode JVMs require a password for the ES keystore
            return "keystorepassword";
        }
        return randomFrom("", "keystorepassword");
    }
}
