/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.crypto;

import java.util.Arrays;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.Watcher;
import org.junit.Before;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CryptoServiceTests extends ESTestCase {
    private Settings settings;

    @Before
    public void init() throws Exception {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile(Watcher.ENCRYPTION_KEY_SETTING.getKey(), CryptoService.generateKey());
        settings = Settings.builder()
                .setSecureSettings(mockSecureSettings)
                .build();
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

        assertThat(service.isEncrypted((char[]) null), is(false));
        assertThat(service.isEncrypted(new char[0]), is(false));
        assertThat(service.isEncrypted(new char[CryptoService.ENCRYPTED_TEXT_PREFIX.length()]), is(false));
        assertThat(service.isEncrypted(CryptoService.ENCRYPTED_TEXT_PREFIX.toCharArray()), is(true));
        assertThat(service.isEncrypted(randomAlphaOfLengthBetween(0, 100).toCharArray()), is(false));
        assertThat(service.isEncrypted(service.encrypt(randomAlphaOfLength(10).toCharArray())), is(true));
    }
}
