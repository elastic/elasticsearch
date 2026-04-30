/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.crypto;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.crypto.EncryptedData;
import org.elasticsearch.xpack.core.crypto.EncryptionKeyNotYetAvailableException;

import java.nio.ByteBuffer;
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
        when(keyProvider.getActiveKey()).thenReturn(new AesGcmEncryptionService.ActiveKey(keyId, key));
        when(keyProvider.getKey(keyId)).thenReturn(key);
        return keyProvider;
    }

    public void testEncryptDecryptRoundTrip() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(0, 4096));
        EncryptedData encrypted = service.encrypt(plaintext);
        byte[] decrypted = service.decrypt(encrypted);

        assertArrayEquals(plaintext, decrypted);
    }

    public void testEncryptProducesCorrectFormat() {
        SecretKey key = randomAesKey();
        String keyId = "test-key-id";
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider(keyId, key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        EncryptedData encrypted = service.encrypt(new byte[] { 1, 2, 3 });

        assertThat(encrypted.keyId(), equalTo(keyId));

        ByteBuffer buf = ByteBuffer.wrap(encrypted.payload());
        assertThat(buf.get(), equalTo((byte) 0x01));
        // IV (12) + ciphertext (3) + GCM tag (16)
        assertThat(buf.remaining(), org.hamcrest.Matchers.greaterThanOrEqualTo(12 + 16 + 3));
    }

    public void testDecryptWithUnknownKeyIdFails() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        EncryptedData encrypted = service.encrypt(randomByteArrayOfLength(10));

        AesGcmEncryptionService.KeyProvider keyProvider2 = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider2.getKey("key-1")).thenReturn(null);
        AesGcmEncryptionService service2 = new AesGcmEncryptionService(keyProvider2);

        EncryptionKeyNotYetAvailableException e = expectThrows(
            EncryptionKeyNotYetAvailableException.class,
            () -> service2.decrypt(encrypted)
        );
        assertThat(e.getMessage(), containsString("not yet available"));
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testDecryptWithWrongKeyFails() {
        SecretKey key1 = randomAesKey();
        SecretKey key2 = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider1 = mockKeyProvider("key-1", key1);
        AesGcmEncryptionService service1 = new AesGcmEncryptionService(keyProvider1);

        EncryptedData encrypted = service1.encrypt(randomByteArrayOfLength(10));

        AesGcmEncryptionService.KeyProvider keyProvider2 = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider2.getKey("key-1")).thenReturn(key2);
        AesGcmEncryptionService service2 = new AesGcmEncryptionService(keyProvider2);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> service2.decrypt(encrypted));
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptWithTamperedPayloadFails() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        EncryptedData encrypted = service.encrypt(randomByteArrayOfLength(32));
        byte[] tampered = encrypted.payload().clone();
        tampered[tampered.length - 1] ^= 0xFF;

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> service.decrypt(new EncryptedData(encrypted.keyId(), tampered))
        );
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptWithUnsupportedVersionFails() {
        byte[] payload = new byte[64];
        random().nextBytes(payload);
        payload[0] = 0x02;

        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.decrypt(new EncryptedData("key-1", payload))
        );
        assertThat(e.getMessage(), containsString("unsupported serialization version"));
    }

    public void testDecryptWithTruncatedPayloadFails() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.decrypt(new EncryptedData("key-1", new byte[] { 0x01, 0x01 }))
        );
        assertThat(e.getMessage(), containsString("invalid length of encrypted payload"));
    }

    public void testEncryptFailsWhenKeyNotAvailable() {
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKey()).thenReturn(null);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        EncryptionKeyNotYetAvailableException e = expectThrows(
            EncryptionKeyNotYetAvailableException.class,
            () -> service.encrypt(new byte[] { 1 })
        );
        assertThat(e.getMessage(), containsString("not yet available"));
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testEncryptEmptyPlaintext() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        EncryptedData encrypted = service.encrypt(new byte[0]);
        byte[] decrypted = service.decrypt(encrypted);
        assertThat(decrypted.length, equalTo(0));
    }

    public void testDifferentEncryptionsProduceDifferentPayloads() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] plaintext = randomByteArrayOfLength(32);
        EncryptedData encrypted1 = service.encrypt(plaintext);
        EncryptedData encrypted2 = service.encrypt(plaintext);

        assertFalse(
            "same plaintext should produce different payloads due to random IV",
            Arrays.equals(encrypted1.payload(), encrypted2.payload())
        );

        assertArrayEquals(plaintext, service.decrypt(encrypted1));
        assertArrayEquals(plaintext, service.decrypt(encrypted2));
    }

    public void testDecryptWithTamperedIV() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        EncryptedData encrypted = service.encrypt(randomByteArrayOfLength(32));
        byte[] tampered = encrypted.payload().clone();
        // IV starts at offset 1 (after version byte)
        tampered[1] ^= 0xFF;

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> service.decrypt(new EncryptedData(encrypted.keyId(), tampered))
        );
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptWithWrongKeyIdFails() {
        SecretKey key1 = randomAesKey();
        SecretKey key2 = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKey()).thenReturn(new AesGcmEncryptionService.ActiveKey("key-1", key1));
        when(keyProvider.getKey("key-1")).thenReturn(key1);
        when(keyProvider.getKey("key-2")).thenReturn(key2);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        EncryptedData encrypted = service.encrypt(randomByteArrayOfLength(32));

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> service.decrypt(new EncryptedData("key-2", encrypted.payload()))
        );
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptEmptyPayload() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.decrypt(new EncryptedData("key-1", new byte[0]))
        );
        assertThat(e.getMessage(), containsString("invalid length of encrypted payload"));
    }

    public void testLargePayloadRoundTrip() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService.KeyProvider keyProvider = mockKeyProvider("key-1", key);
        AesGcmEncryptionService service = new AesGcmEncryptionService(keyProvider);

        byte[] plaintext = randomByteArrayOfLength(1024 * 1024);
        EncryptedData encrypted = service.encrypt(plaintext);
        byte[] decrypted = service.decrypt(encrypted);
        assertArrayEquals(plaintext, decrypted);
    }
}
