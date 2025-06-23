/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.crypto;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.junit.Before;

import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

import javax.crypto.KeyGenerator;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CryptoServiceTests extends ESTestCase {
    private Settings settings;

    @Before
    public void init() throws Exception {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), generateKey());
        settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
    }

    public void testEncryptionAndDecryptionChars() throws Exception {
        CryptoService service = new CryptoService(settings);
        final char[] chars = randomAlphaOfLengthBetween(0, 1000).toCharArray();
        final char[] encrypted = service.encrypt(chars);
        assertThat(encrypted, notNullValue());
        assertThat(Arrays.equals(encrypted, chars), is(false));

        final char[] decrypted = service.decrypt(encrypted);
        assertThat(Arrays.equals(chars, decrypted), is(true));
    }

    public void testEncryptedChar() throws Exception {
        CryptoService service = new CryptoService(settings);

        assertThat(CryptoService.isEncrypted((char[]) null), is(false));
        assertThat(CryptoService.isEncrypted(new char[0]), is(false));
        assertThat(CryptoService.isEncrypted(new char[CryptoService.ENCRYPTED_TEXT_PREFIX.length()]), is(false));
        assertThat(CryptoService.isEncrypted(CryptoService.ENCRYPTED_TEXT_PREFIX.toCharArray()), is(true));
        assertThat(CryptoService.isEncrypted(randomAlphaOfLengthBetween(0, 100).toCharArray()), is(false));
        assertThat(CryptoService.isEncrypted(service.encrypt(randomAlphaOfLength(10).toCharArray())), is(true));
    }

    public void testErrorMessageWhenSecureEncryptionKeySettingDoesNotExist() throws Exception {
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> new CryptoService(Settings.EMPTY));
        assertThat(e.getMessage(), is("setting [" + WatcherField.ENCRYPTION_KEY_SETTING.getKey() + "] must be set in keystore"));
    }

    public static byte[] generateKey() {
        try {
            KeyGenerator generator = KeyGenerator.getInstance(CryptoService.KEY_ALGO);
            generator.init(CryptoService.KEY_SIZE);
            return generator.generateKey().getEncoded();
        } catch (NoSuchAlgorithmException e) {
            throw new ElasticsearchException("failed to generate key", e);
        }
    }
}
