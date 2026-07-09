/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

/**
 * AES-256-GCM primitive with a self-describing wire format. Centralises the cipher operation so
 * callers do not duplicate {@code Cipher.init} / {@code doFinal} / IV-generation / version-prefixing.
 *
 * <p>{@link #encrypt} produces and {@link #decrypt} consumes the byte layout:
 * <pre>
 * [version(1) | iv(12) | ciphertext + GCM tag]
 * </pre>
 *
 * <p>The leading version byte lets us evolve the framing (different IV size, etc.) while still
 * being able to read previously persisted payloads. A switch to a different AEAD primitive
 * (e.g. ChaCha20-Poly1305) would be a sibling class with its own format, not a version bump here.
 */
public final class AesGcm {

    static final int IV_LENGTH_BYTES = 12;
    static final int GCM_TAG_LENGTH_BITS = 128;
    static final int OVERHEAD_BYTES = 1 + IV_LENGTH_BYTES + GCM_TAG_LENGTH_BITS / 8;

    private static final byte SERIALIZATION_FORMAT_VERSION = 1;
    private static final String CIPHER_ALGORITHM = "AES/GCM/NoPadding";

    private static final int VERSION_OFFSET = 0;
    private static final int IV_OFFSET = VERSION_OFFSET + 1;
    private static final int CIPHERTEXT_OFFSET = IV_OFFSET + IV_LENGTH_BYTES;

    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private AesGcm() {}

    /**
     * Encrypts {@code plaintext} under {@code key} using a freshly generated 12-byte IV.
     * Returns {@code [version | iv | ciphertext + tag]}.
     */
    public static byte[] encrypt(SecretKey key, byte[] plaintext) {
        byte[] iv = new byte[IV_LENGTH_BYTES];
        SECURE_RANDOM.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(GCM_TAG_LENGTH_BITS, iv));
            byte[] encrypted = cipher.doFinal(plaintext);

            byte[] output = new byte[CIPHERTEXT_OFFSET + encrypted.length];
            output[VERSION_OFFSET] = SERIALIZATION_FORMAT_VERSION;
            System.arraycopy(iv, 0, output, IV_OFFSET, IV_LENGTH_BYTES);
            System.arraycopy(encrypted, 0, output, CIPHERTEXT_OFFSET, encrypted.length);
            return output;
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("encryption failed", e);
        }
    }

    /**
     * Decrypts a previously {@link #encrypt encrypted} payload at {@code [offset, offset+length)}
     * of {@code ciphertext} using {@code key}. Throws if the version byte is not recognised or the
     * AES-GCM authentication tag fails.
     */
    public static byte[] decrypt(SecretKey key, byte[] ciphertext, int offset, int length) {
        if (length < OVERHEAD_BYTES) {
            throw new IllegalArgumentException("ciphertext too short");
        }
        int version = Byte.toUnsignedInt(ciphertext[offset + VERSION_OFFSET]);
        if (version != SERIALIZATION_FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported serialization version [" + version + "]");
        }
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(
                Cipher.DECRYPT_MODE,
                key,
                new GCMParameterSpec(GCM_TAG_LENGTH_BITS, ciphertext, offset + IV_OFFSET, IV_LENGTH_BYTES)
            );
            return cipher.doFinal(ciphertext, offset + CIPHERTEXT_OFFSET, length - CIPHERTEXT_OFFSET);
        } catch (AEADBadTagException e) {
            throw new ElasticsearchException("decryption failed: AES-GCM authentication tag mismatch (wrong key or corrupted payload)", e);
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("decryption failed", e);
        }
    }
}
