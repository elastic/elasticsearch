/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionService;

import java.nio.ByteBuffer;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;

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
     * Provides encryption keys to {@link AesGcmEncryptionService}.
     */
    public interface KeyProvider {
        @Nullable
        SecretKey getActiveKey();

        @Nullable
        String getActiveKeyId();

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

    private static final Logger logger = LogManager.getLogger(AesGcmEncryptionService.class);

    private final KeyProvider keyProvider;
    private final SecureRandom secureRandom = new SecureRandom();

    public AesGcmEncryptionService(KeyProvider keyProvider) {
        this.keyProvider = keyProvider;
    }

    @Override
    public EncryptedData encrypt(byte[] bytes) {
        String activeKeyId = keyProvider.getActiveKeyId();
        SecretKey activeKey = keyProvider.getActiveKey();
        if (activeKeyId == null || activeKey == null) {
            logger.warn("attempted to encrypt but primary encryption key is not available");
            throw new IllegalStateException("primary encryption key is not available");
        }

        byte[] iv = new byte[IV_LENGTH_BYTES];
        secureRandom.nextBytes(iv);

        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, activeKey, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            byte[] encrypted = cipher.doFinal(bytes);
            assert encrypted.length >= GCM_TAG_LENGTH_BITS / 8 : "GCM output too short, expected at least tag length";

            ByteBuffer output = ByteBuffer.allocate(1 + IV_LENGTH_BYTES + encrypted.length);
            output.put(SERIALIZATION_FORMAT_VERSION);
            output.put(iv);
            output.put(encrypted);
            assert output.remaining() == 0 : "buffer not fully written";
            return new EncryptedData(activeKeyId, output.array());
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("encryption failed", e);
        }
    }

    @Override
    public byte[] decrypt(EncryptedData encryptedData) {
        byte[] payload = encryptedData.payload();

        if (payload.length < MIN_PAYLOAD_LENGTH) {
            logger.warn(
                "received payload [{}] bytes for decryption, less than the [{}] minimum length",
                payload.length,
                MIN_PAYLOAD_LENGTH
            );
            throw new IllegalArgumentException("invalid length of encrypted payload");
        }

        ByteBuffer buf = ByteBuffer.wrap(payload);

        int version = Byte.toUnsignedInt(buf.get());
        if (version != SERIALIZATION_FORMAT_VERSION) {
            logger.warn(
                "received serialization version [{}] for decryption, but current supported version is [{}]",
                version,
                SERIALIZATION_FORMAT_VERSION
            );
            throw new IllegalArgumentException("unsupported serialization version for encrypted data [" + version + "]");
        }

        byte[] iv = new byte[IV_LENGTH_BYTES];
        buf.get(iv);

        byte[] encrypted = new byte[buf.remaining()];
        buf.get(encrypted);

        String keyId = encryptedData.keyId();
        SecretKey key = keyProvider.getKey(keyId);
        if (key == null) {
            throw new IllegalStateException("decryption key with id [" + keyId + "] is not available");
        }

        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            return cipher.doFinal(encrypted);
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("decryption failed for key [" + keyId + "]", e);
        }
    }
}
