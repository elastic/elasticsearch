/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.util.function.Supplier;

/**
 * Password-based {@link ProjectEncryptionKeyMetadata.PekEncryption} implementation. Wraps and unwraps PEK bytes for disk (GATEWAY)
 * serialization using PBKDF2-derived AES-256-GCM (see {@link PasswordBasedEncryption}).
 *
 * <p>The active password id and password material are sourced from a {@link Supplier} so that secure-settings reloads are reflected
 * automatically: the supplier is re-evaluated on every call.
 */
final class PasswordPekEncryption implements ProjectEncryptionKeyMetadata.PekEncryption {

    private final Supplier<Settings> settingsSupplier;

    PasswordPekEncryption(Supplier<Settings> settingsSupplier) {
        this.settingsSupplier = settingsSupplier;
    }

    @Override
    public String activePasswordId() {
        String id = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(settingsSupplier.get());
        if (id == null) {
            throw new ElasticsearchException(
                "cannot wrap PEK for disk: [" + ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY + "] is not configured"
            );
        }
        return id;
    }

    @Override
    public WrappedKey wrap(byte[] plaintextPek) {
        Settings s = settingsSupplier.get();
        String passwordId = ProjectEncryptionKeyPasswordSettings.getActivePasswordId(s);
        if (passwordId == null) {
            throw new ElasticsearchException(
                "cannot wrap PEK for disk: [" + ProjectEncryptionKeyPasswordSettings.ACTIVE_PASSWORD_ID_KEY + "] is not configured"
            );
        }
        try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(s, passwordId)) {
            if (password == null) {
                throw new ElasticsearchException(
                    "cannot wrap PEK for disk: secure setting ["
                        + ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                        + passwordId
                        + "] is not configured"
                );
            }
            return new WrappedKey(passwordId, PasswordBasedEncryption.wrap(plaintextPek, passwordId, password.getChars()).payload());
        }
    }

    @Override
    public byte[] unwrap(byte[] wrappedPek, String passwordId) {
        try (SecureString password = ProjectEncryptionKeyPasswordSettings.getPassword(settingsSupplier.get(), passwordId)) {
            if (password == null) {
                throw new ElasticsearchException(
                    "cannot unwrap PEK from disk: secure setting ["
                        + ProjectEncryptionKeyPasswordSettings.PASSWORD_PREFIX
                        + passwordId
                        + "] is not configured"
                );
            }
            return PasswordBasedEncryption.unwrap(new EncryptedData(passwordId, wrappedPek), password.getChars());
        }
    }
}
