/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.encryption.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.AesGcm;

import java.util.Arrays;

public class PasswordBasedEncryptionTests extends ESTestCase {

    private static final char[] PASSWORD = "p4ssw0rd-fips-ok".toCharArray();

    public void testRoundTripRecoversPlaintext() {
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);

        EncryptedData encrypted = PasswordBasedEncryption.wrap(plaintext, "v1", PASSWORD);
        assertEquals("v1", encrypted.keyId());
        assertTrue(encrypted.payload().length >= 1 + PasswordBasedEncryption.SALT_LENGTH_BYTES + AesGcm.OVERHEAD_BYTES);

        byte[] decrypted = PasswordBasedEncryption.unwrap(encrypted, PASSWORD);
        assertArrayEquals(plaintext, decrypted);
    }

    public void testWrongPasswordFailsToUnwrap() {
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);
        EncryptedData encrypted = PasswordBasedEncryption.wrap(plaintext, "v1", "right-password-fips".toCharArray());

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> PasswordBasedEncryption.unwrap(encrypted, "wrong-password-fips".toCharArray())
        );
        assertTrue(e.getMessage().contains("unwrap failed"));
    }

    public void testEachWrapProducesDistinctPayload() {
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);

        EncryptedData a = PasswordBasedEncryption.wrap(plaintext, "v1", PASSWORD);
        EncryptedData b = PasswordBasedEncryption.wrap(plaintext, "v1", PASSWORD);

        assertFalse(java.util.Arrays.equals(a.payload(), b.payload()));
    }

    public void testTamperedCiphertextFailsAuthenticatedDecryption() {
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);
        EncryptedData encrypted = PasswordBasedEncryption.wrap(plaintext, "v1", PASSWORD);

        byte[] tampered = encrypted.payload().clone();
        // flip a byte well inside the GCM ciphertext (after kdf_version + salt + aes_version + iv)
        tampered[PasswordBasedEncryption.SALT_OFFSET + PasswordBasedEncryption.SALT_LENGTH_BYTES + 1 + AesGcm.IV_LENGTH_BYTES] ^= 0x01;
        EncryptedData bad = new EncryptedData(encrypted.keyId(), tampered);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> PasswordBasedEncryption.unwrap(bad, PASSWORD));
        assertTrue(e.getMessage().contains("unwrap failed"));
    }

    public void testTooShortPayloadIsRejected() {
        EncryptedData empty = new EncryptedData("v1", new byte[0]);
        // The payload-length check fires before key derivation, so the password length doesn't matter here.
        expectThrows(IllegalArgumentException.class, () -> PasswordBasedEncryption.unwrap(empty, "p".toCharArray()));
    }

    public void testUnsupportedKdfVersionIsRejected() {
        EncryptedData encrypted = PasswordBasedEncryption.wrap(
            randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES),
            "v1",
            PASSWORD
        );
        byte[] mutated = encrypted.payload().clone();
        mutated[0] = 0x7F;
        EncryptedData bad = new EncryptedData(encrypted.keyId(), mutated);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PasswordBasedEncryption.unwrap(bad, PASSWORD));
        assertTrue(e.getMessage().contains("unsupported KDF version"));
    }

    public void testUnsupportedInnerVersionIsRejected() {
        EncryptedData encrypted = PasswordBasedEncryption.wrap(
            randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES),
            "v1",
            PASSWORD
        );

        byte[] mutated = encrypted.payload().clone();
        // The AesGcm version byte sits immediately after kdf_version + salt
        mutated[PasswordBasedEncryption.SALT_OFFSET + PasswordBasedEncryption.SALT_LENGTH_BYTES] = 0x7F;
        EncryptedData bad = new EncryptedData(encrypted.keyId(), mutated);

        // Version mismatch is a format error from AesGcm — surfaces as IAE, not as a cipher failure.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PasswordBasedEncryption.unwrap(bad, PASSWORD));
        assertTrue(e.getMessage().contains("unsupported serialization version"));
    }

    public void testArbitraryLengthPayloadsRoundTrip() {
        // no longer PEK-only: snapshot secrets are arbitrary length
        for (int length : new int[] { 0, 1, 15, 100, 5000 }) {
            byte[] plaintext = randomByteArrayOfLength(length);
            EncryptedData encrypted = PasswordBasedEncryption.wrap(plaintext, "v1", PASSWORD);
            assertArrayEquals("length " + length, plaintext, PasswordBasedEncryption.unwrap(encrypted, PASSWORD));
        }
    }

    public void testSamePasswordReusesCachedSalt() {
        EncryptedData first = PasswordBasedEncryption.wrap(randomByteArrayOfLength(16), "v1", PASSWORD);
        EncryptedData second = PasswordBasedEncryption.wrap(randomByteArrayOfLength(16), "v1", PASSWORD);
        assertArrayEquals("same password must reuse the cached KEK and its salt", saltOf(first), saltOf(second));
    }

    public void testDifferentPasswordGetsFreshSalt() {
        EncryptedData first = PasswordBasedEncryption.wrap(randomByteArrayOfLength(16), "v1", PASSWORD);
        EncryptedData other = PasswordBasedEncryption.wrap(randomByteArrayOfLength(16), "v1", "another-password-fips".toCharArray());
        assertFalse("a different password must derive a fresh KEK with a fresh salt", Arrays.equals(saltOf(first), saltOf(other)));
    }

    public void testAlternatingPasswordsRoundTripThroughTheSingleSlotCache() {
        char[] other = "another-password-fips".toCharArray();
        byte[] plaintextA = randomByteArrayOfLength(16);
        byte[] plaintextB = randomByteArrayOfLength(16);

        EncryptedData a1 = PasswordBasedEncryption.wrap(plaintextA, "v1", PASSWORD);
        EncryptedData b1 = PasswordBasedEncryption.wrap(plaintextB, "v1", other);
        EncryptedData a2 = PasswordBasedEncryption.wrap(plaintextA, "v1", PASSWORD);

        assertArrayEquals(plaintextA, PasswordBasedEncryption.unwrap(a1, PASSWORD));
        assertArrayEquals(plaintextB, PasswordBasedEncryption.unwrap(b1, other));
        assertArrayEquals(plaintextA, PasswordBasedEncryption.unwrap(a2, PASSWORD));
    }

    private static byte[] saltOf(EncryptedData encrypted) {
        byte[] payload = encrypted.payload();
        return Arrays.copyOfRange(
            payload,
            PasswordBasedEncryption.SALT_OFFSET,
            PasswordBasedEncryption.SALT_OFFSET + PasswordBasedEncryption.SALT_LENGTH_BYTES
        );
    }
}
