/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AesGcmEncryptionServiceTests extends ESTestCase {

    private static SecretKey randomAesKey() {
        byte[] keyBytes = new byte[32];
        new SecureRandom().nextBytes(keyBytes);
        return new SecretKeySpec(keyBytes, "AES");
    }

    private static AesGcmEncryptionService.KeyProvider mockKeyProvider(String keyId, SecretKey key) {
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKeyId()).thenReturn(keyId);
        when(keyProvider.getActiveKey()).thenReturn(key);
        when(keyProvider.getKey(keyId)).thenReturn(key);
        return keyProvider;
    }

    public void testEncryptDecryptRoundTrip() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(0, 4096));
        byte[] ciphertext = service.encrypt(plaintext);
        byte[] decrypted = service.decrypt(ciphertext);

        assertArrayEquals(plaintext, decrypted);
    }

    public void testEncryptProducesCorrectFormat() {
        SecretKey key = randomAesKey();
        String keyId = "test-key-id";
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider(keyId, key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] ciphertext = service.encrypt(new byte[] { 1, 2, 3 });
        ByteBuffer buf = ByteBuffer.wrap(ciphertext);

        assertThat(buf.get(), equalTo((byte) 0x01));

        int keyIdLen = Byte.toUnsignedInt(buf.get());
        byte[] keyIdBytes = new byte[keyIdLen];
        buf.get(keyIdBytes);
        assertThat(new String(keyIdBytes, StandardCharsets.UTF_8), equalTo(keyId));

        // IV: 12 bytes
        assertThat(buf.remaining(), org.hamcrest.Matchers.greaterThanOrEqualTo(12 + 16 + 3));
    }

    public void testDecryptWithUnknownKeyIdFails() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] ciphertext = service.encrypt(randomByteArrayOfLength(10));

        // Replace the key ID in the ciphertext
        AesGcmEncryptionService.KeyProvider keyProvider2 = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider2.getKey("key-1")).thenReturn(null);
        AesGcmEncryptionService service2 = new AesGcmEncryptionService(keyProvider2);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> service2.decrypt(ciphertext));
        assertThat(e.getMessage(), containsString("not available"));
    }

    public void testDecryptWithWrongKeyFails() {
        SecretKey key1 = randomAesKey();
        SecretKey key2 = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider1 = mockKeyProvider("key-1", key1);
        AesGcmEncryptionService service1 = new AesGcmEncryptionService(keyProvider1);

        byte[] ciphertext = service1.encrypt(randomByteArrayOfLength(10));

        AesGcmEncryptionService.KeyProvider keyProvider2 = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider2.getKey("key-1")).thenReturn(key2);
        AesGcmEncryptionService service2 = new AesGcmEncryptionService(keyProvider2);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> service2.decrypt(ciphertext));
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptWithTamperedCiphertextFails() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] ciphertext = service.encrypt(randomByteArrayOfLength(32));
        // Flip a bit in the encrypted payload (after version + keyIdLen + keyId + IV)
        ciphertext[ciphertext.length - 1] ^= 0xFF;

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> service.decrypt(ciphertext));
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptWithUnsupportedVersionFails() {
        byte[] ciphertext = new byte[64];
        random().nextBytes(ciphertext);
        ciphertext[0] = 0x02;

        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.decrypt(ciphertext));
        assertThat(e.getMessage(), containsString("unsupported serialization version"));
    }

    public void testDecryptWithTruncatedInputFails() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.decrypt(new byte[] { 0x01, 0x01 }));
        assertThat(e.getMessage(), containsString("invalid length of encrypted data"));
    }

    public void testEncryptFailsWhenKeyNotAvailable() {
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKeyId()).thenReturn(null);
        when(keyProvider.getActiveKey()).thenReturn(null);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        IllegalStateException e = expectThrows(IllegalStateException.class, () -> service.encrypt(new byte[] { 1 }));
        assertThat(e.getMessage(), containsString("not available"));
    }

    public void testEncryptEmptyPlaintext() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] ciphertext = service.encrypt(new byte[0]);
        byte[] decrypted = service.decrypt(ciphertext);
        assertThat(decrypted.length, equalTo(0));
    }

    public void testDifferentEncryptionsProduceDifferentCiphertexts() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] plaintext = randomByteArrayOfLength(32);
        byte[] ciphertext1 = service.encrypt(plaintext);
        byte[] ciphertext2 = service.encrypt(plaintext);

        assertFalse("same plaintext should produce different ciphertexts due to random IV", Arrays.equals(ciphertext1, ciphertext2));

        assertArrayEquals(plaintext, service.decrypt(ciphertext1));
        assertArrayEquals(plaintext, service.decrypt(ciphertext2));
    }

    public void testDecryptWithTamperedIV() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] ciphertext = service.encrypt(randomByteArrayOfLength(32));
        int ivOffset = 1 + 1 + "key-1".getBytes(StandardCharsets.UTF_8).length;
        ciphertext[ivOffset] ^= 0xFF;

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> service.decrypt(ciphertext));
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptWithTamperedKeyId() {
        SecretKey key1 = randomAesKey();
        SecretKey key2 = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKeyId()).thenReturn("key-1");
        when(keyProvider.getActiveKey()).thenReturn(key1);
        when(keyProvider.getKey("key-1")).thenReturn(key1);
        when(keyProvider.getKey("key-2")).thenReturn(key2);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] ciphertext = service.encrypt(randomByteArrayOfLength(32));
        // Overwrite key ID from "key-1" to "key-2" (same length)
        byte[] newKeyId = "key-2".getBytes(StandardCharsets.UTF_8);
        System.arraycopy(newKeyId, 0, ciphertext, 2, newKeyId.length);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> service.decrypt(ciphertext));
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptEmptyByteArray() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> service.decrypt(new byte[0]));
        assertThat(e.getMessage(), containsString("invalid length of encrypted data"));
    }

    public void testLargePayloadRoundTrip() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] plaintext = randomByteArrayOfLength(1024 * 1024);
        byte[] ciphertext = service.encrypt(plaintext);
        byte[] decrypted = service.decrypt(ciphertext);
        assertArrayEquals(plaintext, decrypted);
    }
}
