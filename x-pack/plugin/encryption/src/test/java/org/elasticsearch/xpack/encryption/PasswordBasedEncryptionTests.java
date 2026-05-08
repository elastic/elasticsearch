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

    public void testRoundTripRecoversPlaintext() {
        char[] password = "p4ssw0rd".toCharArray();
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);

        EncryptedData encrypted = PasswordBasedEncryption.wrap(plaintext, "v1", password);
        assertEquals("v1", encrypted.keyId());
        assertTrue(encrypted.payload().length >= PasswordBasedEncryption.SALT_LENGTH_BYTES + AesGcm.OVERHEAD_BYTES);

        byte[] decrypted = PasswordBasedEncryption.unwrap(encrypted, password);
        assertArrayEquals(plaintext, decrypted);
    }

    public void testWrongPasswordFailsToUnwrap() {
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);
        EncryptedData encrypted = PasswordBasedEncryption.wrap(plaintext, "v1", "right".toCharArray());

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> PasswordBasedEncryption.unwrap(encrypted, "wrong".toCharArray())
        );
        assertTrue(e.getMessage().contains("PEK unwrap failed"));
    }

    public void testEachWrapProducesDistinctPayload() {
        char[] password = "p4ssw0rd".toCharArray();
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);

        EncryptedData a = PasswordBasedEncryption.wrap(plaintext, "v1", password);
        EncryptedData b = PasswordBasedEncryption.wrap(plaintext, "v1", password);

        assertFalse(java.util.Arrays.equals(a.payload(), b.payload()));
    }

    public void testTamperedCiphertextFailsAuthenticatedDecryption() {
        char[] password = "p4ssw0rd".toCharArray();
        byte[] plaintext = randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES);
        EncryptedData encrypted = PasswordBasedEncryption.wrap(plaintext, "v1", password);

        byte[] tampered = encrypted.payload().clone();
        // flip a byte well inside the GCM ciphertext (after salt + version + iv)
        tampered[PasswordBasedEncryption.SALT_LENGTH_BYTES + 1 + AesGcm.IV_LENGTH_BYTES] ^= 0x01;
        EncryptedData bad = new EncryptedData(encrypted.keyId(), tampered);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> PasswordBasedEncryption.unwrap(bad, password));
        assertTrue(e.getMessage().contains("PEK unwrap failed"));
    }

    public void testTooShortPayloadIsRejected() {
        EncryptedData empty = new EncryptedData("v1", new byte[0]);
        expectThrows(IllegalArgumentException.class, () -> PasswordBasedEncryption.unwrap(empty, "p".toCharArray()));
    }

    public void testUnsupportedInnerVersionIsRejected() {
        char[] password = "p4ssw0rd".toCharArray();
        EncryptedData encrypted = PasswordBasedEncryption.wrap(
            randomByteArrayOfLength(PasswordBasedEncryption.PEK_LENGTH_BYTES),
            "v1",
            password
        );

        byte[] mutated = encrypted.payload().clone();
        // The AesGcm version byte sits immediately after the salt
        mutated[PasswordBasedEncryption.SALT_LENGTH_BYTES] = 0x7F;
        EncryptedData bad = new EncryptedData(encrypted.keyId(), mutated);

        // Version mismatch is a format error from AesGcm — surfaces as IAE, not as a cipher failure.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> PasswordBasedEncryption.unwrap(bad, password));
        assertTrue(e.getMessage().contains("unsupported serialization version"));
    }
}
