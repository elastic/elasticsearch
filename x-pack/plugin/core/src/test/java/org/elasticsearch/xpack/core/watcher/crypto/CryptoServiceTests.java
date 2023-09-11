/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.crypto;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureSetting;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.watcher.WatcherField;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import javax.crypto.KeyGenerator;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class CryptoServiceTests extends ESTestCase {
    private static final String TEST_ENCRYPTION_KEY_SETTING_NAME = "xpack.key";
    private Settings settings;
    private Setting<SecureString> encryptionKeySetting;

    @Before
    public void init() throws Exception {
        MockSecureSettings mockSecureSettings = new MockSecureSettings();
        mockSecureSettings.setFile(WatcherField.ENCRYPTION_KEY_SETTING.getKey(), generateKey());
        mockSecureSettings.setString(TEST_ENCRYPTION_KEY_SETTING_NAME, generateKeyAsString());
        settings = Settings.builder().setSecureSettings(mockSecureSettings).build();
        encryptionKeySetting = SecureSetting.secureString(TEST_ENCRYPTION_KEY_SETTING_NAME, null);
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

    public void testEncryptionAndDecryptionChars_UsingCryptoServiceWithKeySetting() throws Exception {
        CryptoService service = CryptoService.createFromEncryptionKeySetting(settings, encryptionKeySetting);

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

    public void testErrorMessageWhenSecureEncryptionKeySettingDoesNotExist() throws Exception {
        final ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> new CryptoService(Settings.EMPTY));
        assertThat(e.getMessage(), is("setting [" + WatcherField.ENCRYPTION_KEY_SETTING.getKey() + "] must be set in keystore"));
    }

    public void testErrorMessageWhenPassedSettingDoesNotExist() {
        final ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> CryptoService.createFromEncryptionKeySetting(Settings.EMPTY, encryptionKeySetting)
        );

        assertThat(e.getMessage(), is(format("setting [%s] must be set in keystore", TEST_ENCRYPTION_KEY_SETTING_NAME)));
    }

    public void testErrorMessageWhenPassedSettingIsEmpty() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(TEST_ENCRYPTION_KEY_SETTING_NAME, "");
        Settings mockedSettings = Settings.builder().setSecureSettings(secureSettings).build();

        final ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> CryptoService.createFromEncryptionKeySetting(mockedSettings, encryptionKeySetting)
        );

        assertThat(
            e.getMessage(),
            is(format("setting [%s] contents was empty, it must be set in keystore", TEST_ENCRYPTION_KEY_SETTING_NAME))
        );
    }

    public void testErrorMessageWhenPassedSettingIsLessThanRequiredSize() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(TEST_ENCRYPTION_KEY_SETTING_NAME, "a");
        Settings mockedSettings = Settings.builder().setSecureSettings(secureSettings).build();

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> CryptoService.createFromEncryptionKeySetting(mockedSettings, encryptionKeySetting)
        );

        assertThat(e.getMessage(), is("key size was less than the expected value; was the key generated with elasticsearch-syskeygen?"));
    }

    public void testDoesNotThrowExceptionWhenEncryptionKeyIsLargerThanRequiredSize() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        var keySize = CryptoService.KEY_SIZE / 8;
        secureSettings.setString(TEST_ENCRYPTION_KEY_SETTING_NAME, "a".repeat(keySize + 1));
        Settings mockedSettings = Settings.builder().setSecureSettings(secureSettings).build();

        CryptoService service = CryptoService.createFromEncryptionKeySetting(mockedSettings, encryptionKeySetting);

        final char[] chars = randomAlphaOfLengthBetween(0, 1000).toCharArray();
        final char[] encrypted = service.encrypt(chars);
        assertThat(encrypted, notNullValue());
        assertThat(Arrays.equals(encrypted, chars), is(false));

        final char[] decrypted = service.decrypt(encrypted);
        assertThat(Arrays.equals(chars, decrypted), is(true));
    }

    private static String generateKeyAsString() {
        return new String(generateKey(), StandardCharsets.ISO_8859_1);
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
