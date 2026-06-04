/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;

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
        assertTrue(e.getMessage().contains("PEK unwrap failed"));
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
        assertTrue(e.getMessage().contains("PEK unwrap failed"));
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
}
