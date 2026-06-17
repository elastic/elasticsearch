/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionService;

import java.util.Objects;

import javax.crypto.SecretKey;

/**
 * AES-256-GCM implementation of {@link EncryptionService}. The cipher operation is delegated to
 * {@link AesGcm}; this class only resolves the active {@link SecretKey} from the
 * {@link KeyProvider}.
 *
 * <p>The payload stored in {@link EncryptedData#payload()} is exactly the byte string produced by
 * {@link AesGcm#encrypt}: {@code [version | iv | ciphertext + tag]}. The key ID is carried
 * externally in {@link EncryptedData#keyId()}.
 */
class AesGcmEncryptionService implements EncryptionService {

    /**
     * The currently active encryption key paired with its identifier. Bundled so callers
     * can read both atomically and avoid races against key rotation.
     */
    record ActiveKey(String keyId, SecretKey key) {
        ActiveKey {
            Objects.requireNonNull(keyId);
            Objects.requireNonNull(key);
        }
    }

    /**
     * Provides encryption keys to {@link AesGcmEncryptionService}.
     */
    interface KeyProvider {
        @Nullable
        ActiveKey getActiveKey();

        @Nullable
        SecretKey getKey(String keyId);
    }

    private final KeyProvider keyProvider;

    AesGcmEncryptionService(KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
    }

    @Override
    public EncryptedData encrypt(byte[] bytes) {
        ActiveKey activeKey = keyProvider.getActiveKey();
        if (activeKey == null) {
            throw new EncryptionKeyNotYetAvailableException("project encryption key is not yet available");
        }
        return new EncryptedData(activeKey.keyId(), AesGcm.encrypt(activeKey.key(), bytes));
    }

    @Override
    public byte[] decrypt(EncryptedData encryptedData) {
        String keyId = encryptedData.keyId();
        SecretKey key = keyProvider.getKey(keyId);
        if (key == null) {
            throw new EncryptionKeyNotYetAvailableException("decryption key with id [{}] is not yet available", keyId);
        }
        byte[] payload = encryptedData.payload();
        return AesGcm.decrypt(key, payload, 0, payload.length);
    }

}
