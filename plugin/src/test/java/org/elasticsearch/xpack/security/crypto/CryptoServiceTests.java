/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.crypto;

import java.io.FileNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.XPackPlugin;
import org.junit.Before;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CryptoServiceTests extends ESTestCase {
    private Settings settings;
    private Environment env;
    private Path keyFile;

    @Before
    public void init() throws Exception {
        Path home = createTempDir();
        Path xpackConf = home.resolve("config").resolve(XPackPlugin.NAME);
        Files.createDirectories(xpackConf);
        keyFile = xpackConf.resolve("system_key");
        Files.write(keyFile, CryptoService.generateKey());
        settings = Settings.builder()
                .put("resource.reload.interval.high", "2s")
                .put("xpack.security.system_key.required", randomBoolean())
                .put("path.home", home)
                .build();
        env = new Environment(settings);
    }

    public void testEncryptionAndDecryptionChars() throws Exception {
        CryptoService service = new CryptoService(settings, env);
                assertThat(service.isEncryptionEnabled(), is(true));
        final char[] chars = randomAlphaOfLengthBetween(0, 1000).toCharArray();
        final char[] encrypted = service.encrypt(chars);
        assertThat(encrypted, notNullValue());
        assertThat(Arrays.equals(encrypted, chars), is(false));

        final char[] decrypted = service.decrypt(encrypted);
        assertThat(Arrays.equals(chars, decrypted), is(true));
    }

    public void testEncryptionAndDecryptionCharsWithoutKey() throws Exception {
        Files.delete(keyFile);
        CryptoService service = new CryptoService(Settings.EMPTY, env);
        assertThat(service.isEncryptionEnabled(), is(false));
        final char[] chars = randomAlphaOfLengthBetween(0, 1000).toCharArray();
        final char[] encryptedChars = service.encrypt(chars);
        final char[] decryptedChars = service.decrypt(encryptedChars);
        assertThat(chars, equalTo(encryptedChars));
        assertThat(chars, equalTo(decryptedChars));
    }

    public void testEncryptionEnabledWithKey() throws Exception {
        CryptoService service = new CryptoService(settings, env);
        assertThat(service.isEncryptionEnabled(), is(true));
    }

    public void testEncryptionEnabledWithoutKey() throws Exception {
        Files.delete(keyFile);
        CryptoService service = new CryptoService(Settings.EMPTY, env);
        assertThat(service.isEncryptionEnabled(), is(false));
    }

    public void testEncryptedChar() throws Exception {
        CryptoService service = new CryptoService(settings, env);
        assertThat(service.isEncryptionEnabled(), is(true));

        assertThat(service.isEncrypted((char[]) null), is(false));
        assertThat(service.isEncrypted(new char[0]), is(false));
        assertThat(service.isEncrypted(new char[CryptoService.ENCRYPTED_TEXT_PREFIX.length()]), is(false));
        assertThat(service.isEncrypted(CryptoService.ENCRYPTED_TEXT_PREFIX.toCharArray()), is(true));
        assertThat(service.isEncrypted(randomAlphaOfLengthBetween(0, 100).toCharArray()), is(false));
        assertThat(service.isEncrypted(service.encrypt(randomAlphaOfLength(10).toCharArray())), is(true));
    }

    public void testSystemKeyFileRequired() throws Exception {
        Files.delete(keyFile);
        Settings customSettings = Settings.builder().put(settings).put("xpack.security.system_key.required", true).build();
        FileNotFoundException fnfe = expectThrows(FileNotFoundException.class, () -> new CryptoService(customSettings, env));
        assertThat(fnfe.getMessage(), containsString("must be present with a valid key"));
    }

    public void testEmptySystemKeyFile() throws Exception {
        // delete and create empty file
        Files.delete(keyFile);
        Files.createFile(keyFile);
        assertTrue(Files.exists(keyFile));
        IllegalArgumentException iae = expectThrows(IllegalArgumentException.class, () -> new CryptoService(settings, env));
        assertThat(iae.getMessage(), containsString("Empty key"));
    }
}
