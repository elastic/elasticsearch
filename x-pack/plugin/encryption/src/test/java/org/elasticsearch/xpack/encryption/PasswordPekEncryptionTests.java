/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.ProjectEncryptionKeyMetadata.PekEncryption.WrappedKey;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class PasswordPekEncryptionTests extends ESTestCase {

    private static Settings settingsWithPassword(String passwordId, String password) {
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, passwordId);
        secure.setString(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + passwordId, password);
        return Settings.builder().setSecureSettings(secure).build();
    }

    public void testWrapUnwrapRoundTrip() {
        String passwordId = "v1";
        Settings settings = settingsWithPassword(passwordId, "correct-horse-battery-staple");
        PasswordPekEncryption enc = new PasswordPekEncryption(() -> settings);

        byte[] plaintext = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(plaintext);

        WrappedKey wrapped = enc.wrap(plaintext);
        assertEquals(passwordId, wrapped.passwordId());
        byte[] unwrapped = enc.unwrap(wrapped.wrapped(), wrapped.passwordId());

        assertThat(unwrapped, equalTo(plaintext));
    }

    public void testWrappedBytesAreDifferentEachCall() {
        String passwordId = "v1";
        Settings settings = settingsWithPassword(passwordId, "correct-horse-battery-staple");
        PasswordPekEncryption enc = new PasswordPekEncryption(() -> settings);

        byte[] plaintext = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(plaintext);

        WrappedKey wrapped1 = enc.wrap(plaintext);
        WrappedKey wrapped2 = enc.wrap(plaintext);

        assertFalse("each wrap must produce a fresh salt", Arrays.equals(wrapped1.wrapped(), wrapped2.wrapped()));
    }

    public void testWrapThrowsWhenPasswordMissing() {
        // Active id is set but no matching password material → wrap must throw loudly.
        MockSecureSettings secure = new MockSecureSettings();
        secure.setString(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY, "v1");
        Settings settings = Settings.builder().setSecureSettings(secure).build();
        PasswordPekEncryption enc = new PasswordPekEncryption(() -> settings);
        byte[] plaintext = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];

        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> enc.wrap(plaintext));
        assertTrue(ex.getMessage().contains("cannot wrap PEK for disk"));
        assertTrue(ex.getMessage().contains(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + "v1"));
    }

    public void testWrapThrowsWhenActivePasswordIdMissing() {
        PasswordPekEncryption enc = new PasswordPekEncryption(() -> Settings.EMPTY);
        byte[] plaintext = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];

        ElasticsearchException ex = expectThrows(ElasticsearchException.class, () -> enc.wrap(plaintext));
        assertTrue(ex.getMessage().contains("cannot wrap PEK for disk"));
        assertTrue(ex.getMessage().contains(ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY));
    }

    public void testUnwrapThrowsWhenPasswordMissing() {
        String passwordId = "v1";
        Settings settings = settingsWithPassword(passwordId, "correct-horse-battery-staple");
        PasswordPekEncryption enc = new PasswordPekEncryption(() -> settings);

        byte[] plaintext = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(plaintext);
        WrappedKey wrapped = enc.wrap(plaintext);

        PasswordPekEncryption encNoPassword = new PasswordPekEncryption(() -> Settings.EMPTY);
        ElasticsearchException ex = expectThrows(
            ElasticsearchException.class,
            () -> encNoPassword.unwrap(wrapped.wrapped(), wrapped.passwordId())
        );
        assertTrue(ex.getMessage().contains("cannot unwrap PEK from disk"));
        assertTrue(ex.getMessage().contains(ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX + passwordId));
    }

    public void testObservesSettingsReloadViaSupplier() {
        String passwordId = "v1";
        AtomicReference<Settings> settingsRef = new AtomicReference<>(settingsWithPassword(passwordId, "old-password-fips-ok-v1"));
        PasswordPekEncryption enc = new PasswordPekEncryption(settingsRef::get);

        byte[] plaintext = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        random().nextBytes(plaintext);

        // Wrap under old password, then rotate to new password and re-wrap.
        WrappedKey wrappedOld = enc.wrap(plaintext);

        settingsRef.set(settingsWithPassword(passwordId, "new-password-fips-ok-v1"));
        WrappedKey wrappedNew = enc.wrap(plaintext);

        // Old wrapped bytes can no longer be unwrapped (wrong password) — GCM tag mismatch.
        assertThrows(Exception.class, () -> enc.unwrap(wrappedOld.wrapped(), wrappedOld.passwordId()));
        // New wrapped bytes unwrap correctly.
        assertThat(enc.unwrap(wrappedNew.wrapped(), wrappedNew.passwordId()), equalTo(plaintext));
    }
}
