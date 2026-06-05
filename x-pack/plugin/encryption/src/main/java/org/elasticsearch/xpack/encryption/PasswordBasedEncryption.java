/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Password-based wrap/unwrap of the 32-byte project encryption key (PEK).
 *
 * <p>The key-encryption-key (KEK) is derived from the password via PBKDF2-HMAC-SHA512 with a per-PEK random 16-byte salt.
 * The AES-256-GCM cipher operation is delegated to {@link AesGcm}.
 *
 * <p>The wrapped PEK is returned as an {@link EncryptedData} where:
 * <ul>
 *   <li>{@link EncryptedData#keyId()} carries the {@code passwordId} (the secure-setting suffix)
 *   <li>{@link EncryptedData#payload()} is {@code [kdf_version(1) | salt(16) | AesGcm.encrypt output]}
 * </ul>
 *
 * <p>The leading {@code kdf_version} byte lets future versions change KDF parameters (algorithm, iterations, salt length)
 * while still being able to unwrap previously persisted payloads.
 *
 * <p>PBKDF2 stretching is applied universally since stateful clusters allow operators to set the keystore value directly.
 */
final class PasswordBasedEncryption {
    static final int PBKDF2_ITERATIONS = 210_000;
    static final int SALT_LENGTH_BYTES = 16;
    static final int KEK_LENGTH_BITS = 256;
    /** Plaintext length of a project encryption key (AES-256 → 32 bytes). Part of the {@link #wrap}/{@link #unwrap} contract. */
    static final int PEK_LENGTH_BYTES = 32;
    private static final SecureRandom SECURE_RANDOM = new SecureRandom();

    private static final String PBKDF2_ALGORITHM = "PBKDF2WithHmacSHA512";
    private static final byte KDF_FORMAT_VERSION = 1;
    private static final int VERSION_OFFSET = 0;
    static final int SALT_OFFSET = VERSION_OFFSET + 1;

    private PasswordBasedEncryption() {}

    /**
     * Wraps the given 32-byte plaintext PEK under a fresh key derived from the password. The returned {@link EncryptedData} carries the
     * salt prefix followed by the {@link AesGcm#encrypt} output so {@link #unwrap} can recover the original PEK given the same password.
     */
    static EncryptedData wrap(byte[] plaintextPek, String passwordId, char[] password) {
        byte[] salt = new byte[SALT_LENGTH_BYTES];
        SECURE_RANDOM.nextBytes(salt);

        SecretKey kek = deriveKek(password, salt);
        byte[] inner = AesGcm.encrypt(kek, plaintextPek);

        byte[] payload = new byte[1 + SALT_LENGTH_BYTES + inner.length];
        payload[VERSION_OFFSET] = KDF_FORMAT_VERSION;
        System.arraycopy(salt, 0, payload, SALT_OFFSET, SALT_LENGTH_BYTES);
        System.arraycopy(inner, 0, payload, SALT_OFFSET + SALT_LENGTH_BYTES, inner.length);
        return new EncryptedData(passwordId, payload);
    }

    /**
     * Unwraps a previously wrapped PEK using the same password the wrap was performed under. Throws if the password is wrong or the
     * ciphertext is corrupted (AES-GCM authentication failure).
     */
    static byte[] unwrap(EncryptedData wrapped, char[] password) {
        byte[] payload = wrapped.payload();
        if (payload.length < 1 + SALT_LENGTH_BYTES + AesGcm.OVERHEAD_BYTES) {
            throw new IllegalArgumentException("wrapped payload is too short");
        }
        int version = Byte.toUnsignedInt(payload[VERSION_OFFSET]);
        if (version != KDF_FORMAT_VERSION) {
            throw new IllegalArgumentException("unsupported KDF version [" + version + "]");
        }
        byte[] salt = Arrays.copyOfRange(payload, SALT_OFFSET, SALT_OFFSET + SALT_LENGTH_BYTES);
        SecretKey kek = deriveKek(password, salt);
        try {
            return AesGcm.decrypt(kek, payload, SALT_OFFSET + SALT_LENGTH_BYTES, payload.length - SALT_OFFSET - SALT_LENGTH_BYTES);
        } catch (ElasticsearchException e) {
            // Re-throw with a domain-specific message so callers can distinguish wrap/unwrap failures
            // from generic decryption errors.
            throw new ElasticsearchException("PEK unwrap failed", e);
        }
    }

    private static SecretKey deriveKek(char[] password, byte[] salt) {
        try {
            SecretKeyFactory factory = SecretKeyFactory.getInstance(PBKDF2_ALGORITHM);
            PBEKeySpec spec = new PBEKeySpec(password, salt, PBKDF2_ITERATIONS, KEK_LENGTH_BITS);
            try {
                byte[] kekBytes;
                try {
                    kekBytes = factory.generateSecret(spec).getEncoded();
                } catch (Error e) {
                    if (e instanceof VirtualMachineError) {
                        throw e;
                    }
                    // Under BC FIPS, prerequisite violations (notably a password shorter than 14 chars / 112 bits, salt or iv constraints)
                    // surface as a subclass of Error. Catch and rewrap so the JVM doesn't exit and the failure mirrors KeyStoreWrapper.
                    throw new ElasticsearchException("Wrapping key derivation failed: " + e.getMessage(), e);
                }
                try {
                    return new SecretKeySpec(kekBytes, "AES");
                } finally {
                    Arrays.fill(kekBytes, (byte) 0);
                }
            } finally {
                spec.clearPassword();
            }
        } catch (GeneralSecurityException e) {
            throw new ElasticsearchException("Wrapping key derivation failed", e);
        }
    }
}
