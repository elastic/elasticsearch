/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Objects;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/**
 * AES-256-GCM implementation of {@link EncryptionService}.
 *
 * <p>The binary payload format stored in {@link EncryptedData#payload()}:
 * <pre>
 * [version: 1 byte] [iv: 12 bytes] [ciphertext + GCM tag]
 * </pre>
 *
 * <p>The key ID is carried externally in {@link EncryptedData#keyId()}.
 */
public class AesGcmEncryptionService implements EncryptionService {

    /**
     * The currently active encryption key paired with its identifier. Bundled so callers
     * can read both atomically and avoid races against key rotation.
     */
    public record ActiveKey(String keyId, SecretKey key) {
        public ActiveKey {
            Objects.requireNonNull(keyId);
            Objects.requireNonNull(key);
        }
    }

    /**
     * Provides encryption keys to {@link AesGcmEncryptionService}.
     */
    public interface KeyProvider {
        @Nullable
        ActiveKey getActiveKey();

        @Nullable
        SecretKey getKey(String keyId);
    }

    private static final byte SERIALIZATION_FORMAT_VERSION = 1;
    // NIST recommends 96 bits, 12 bytes
    private static final int IV_LENGTH_BYTES = 12;
    // NIST recommends 128 bits
    private static final int GCM_TAG_LENGTH_BITS = 128;
    private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";
    // version (1) + IV (12) + GCM tag (16)
    private static final int MIN_PAYLOAD_LENGTH = 1 + IV_LENGTH_BYTES + GCM_TAG_LENGTH_BITS / 8;

    private final KeyProvider keyProvider;
    private final SecureRandom secureRandom = new SecureRandom();

    public AesGcmEncryptionService(KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
    }

    @Override
    public EncryptedData encrypt(byte[] bytes) {
        ActiveKey activeKey = keyProvider.getActiveKey();
        if (activeKey == null) {
            throw new EncryptionKeyNotYetAvailableException("primary encryption key is not yet available");
        }

        byte[] iv = new byte[IV_LENGTH_BYTES];
        secureRandom.nextBytes(iv);

        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, activeKey.key(), new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            byte[] encrypted = cipher.doFinal(bytes);
            assert encrypted.length >= GCM_TAG_LENGTH_BITS / 8 : "GCM output too short, expected at least tag length";

            ByteBuffer output = ByteBuffer.allocate(1 + IV_LENGTH_BYTES + encrypted.length);
            output.put(SERIALIZATION_FORMAT_VERSION);
            output.put(iv);
            output.put(encrypted);
            assert output.remaining() == 0 : "buffer not fully written";
            return new EncryptedData(activeKey.keyId(), output.array());
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("encryption failed", e);
        }
    }

    @Override
    public byte[] decrypt(EncryptedData encryptedData) {
        byte[] payload = encryptedData.payload();

        if (payload.length < MIN_PAYLOAD_LENGTH) {
            throw new IllegalArgumentException("invalid length of encrypted payload");
        }

        int version = Byte.toUnsignedInt(payload[0]);
        if (version != SERIALIZATION_FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported serialization version for encrypted data [" + version + "]");
        }

        String keyId = encryptedData.keyId();
        SecretKey key = keyProvider.getKey(keyId);
        if (key == null) {
            throw new EncryptionKeyNotYetAvailableException("decryption key with id [{}] is not yet available", keyId);
        }

        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            int ciphertextOffset = 1 + IV_LENGTH_BYTES;
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, payload, 1, IV_LENGTH_BYTES));
            return cipher.doFinal(payload, ciphertextOffset, payload.length - ciphertextOffset);
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("decryption failed for key [" + keyId + "]", e);
        }
    }
}
