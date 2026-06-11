/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;

import java.security.SecureRandom;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public class AesGcmTests extends ESTestCase {

    private static SecretKey randomAesKey() {
        byte[] keyBytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        new SecureRandom().nextBytes(keyBytes);
        return new SecretKeySpec(keyBytes, "AES");
    }

    public void testRoundTrip() {
        SecretKey key = randomAesKey();
        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(0, 4096));

        byte[] ciphertext = AesGcm.encrypt(key, plaintext);
        assertEquals(AesGcm.OVERHEAD_BYTES + plaintext.length, ciphertext.length);

        byte[] decrypted = AesGcm.decrypt(key, ciphertext, 0, ciphertext.length);
        assertArrayEquals(plaintext, decrypted);
    }

    public void testDecryptWithDifferentKeyFails() {
        SecretKey k1 = randomAesKey();
        SecretKey k2 = randomAesKey();
        byte[] ciphertext = AesGcm.encrypt(k1, randomByteArrayOfLength(64));

        expectThrows(ElasticsearchException.class, () -> AesGcm.decrypt(k2, ciphertext, 0, ciphertext.length));
    }

    public void testTooShortCiphertextRejected() {
        SecretKey key = randomAesKey();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AesGcm.decrypt(key, new byte[0], 0, 0));
        assertTrue(e.getMessage().contains("too short"));
    }

    public void testUnsupportedVersionRejected() {
        SecretKey key = randomAesKey();
        byte[] ciphertext = AesGcm.encrypt(key, randomByteArrayOfLength(32));
        byte[] mutated = ciphertext.clone();
        mutated[0] = 0x7F;

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> AesGcm.decrypt(key, mutated, 0, mutated.length));
        assertTrue(e.getMessage().contains("unsupported serialization version"));
    }

    public void testDecryptWithOffset() {
        SecretKey key = randomAesKey();
        byte[] plaintext = randomByteArrayOfLength(64);
        byte[] ciphertext = AesGcm.encrypt(key, plaintext);

        // Place the ciphertext inside a larger buffer with leading bytes
        int prefixLength = randomIntBetween(1, 32);
        byte[] withPrefix = new byte[prefixLength + ciphertext.length];
        random().nextBytes(withPrefix);
        System.arraycopy(ciphertext, 0, withPrefix, prefixLength, ciphertext.length);

        byte[] decrypted = AesGcm.decrypt(key, withPrefix, prefixLength, ciphertext.length);
        assertArrayEquals(plaintext, decrypted);
    }
}
