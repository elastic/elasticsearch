/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.encryption;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.encryption.spi.EncryptedData;
import org.elasticsearch.xpack.encryption.spi.EncryptionKeyNotYetAvailableException;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceState;
import org.elasticsearch.xpack.encryption.spi.EncryptionServiceUnavailableException;

import java.nio.ByteBuffer;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AesGcmEncryptionServiceTests extends ESTestCase {

    private static SecretKey randomAesKey() {
        byte[] keyBytes = new byte[PasswordBasedEncryption.PEK_LENGTH_BYTES];
        new SecureRandom().nextBytes(keyBytes);
        return new SecretKeySpec(keyBytes, "AES");
    }

    private static AesGcmEncryptionService.KeyProvider mockKeyProvider(String keyId, SecretKey key) {
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKey()).thenReturn(new AesGcmEncryptionService.ActiveKey(keyId, key));
        when(keyProvider.getKey(keyId)).thenReturn(key);
        return keyProvider;
    }

    private static AesGcmEncryptionService.KeyProvider nullKeyProvider() {
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKey()).thenReturn(null);
        when(keyProvider.getKey(anyString())).thenReturn(null);
        return keyProvider;
    }

    private static AesGcmEncryptionService readyService(AesGcmEncryptionService.KeyProvider kp) {
        return new AesGcmEncryptionService(kp, () -> EncryptionServiceState.READY, () -> true);
    }

    public void testEncryptDecryptRoundTrip() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", key));

        byte[] plaintext = randomByteArrayOfLength(randomIntBetween(0, 4096));
        EncryptedData encrypted = service.encrypt(plaintext);
        byte[] decrypted = service.decrypt(encrypted);

        assertArrayEquals(plaintext, decrypted);
    }

    public void testEncryptProducesCorrectFormat() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService service = readyService(mockKeyProvider("test-key-id", key));

        EncryptedData encrypted = service.encrypt(new byte[] { 1, 2, 3 });

        assertThat(encrypted.keyId(), equalTo("test-key-id"));

        ByteBuffer buf = ByteBuffer.wrap(encrypted.payload());
        assertThat(buf.get(), equalTo((byte) 0x01));
        // IV (12) + ciphertext (3) + GCM tag (16)
        assertThat(buf.remaining(), org.hamcrest.Matchers.greaterThanOrEqualTo(12 + 16 + 3));
    }

    public void testDecryptWithUnknownKeyIdFails() {
        SecretKey key = randomAesKey();
        EncryptedData encrypted = readyService(mockKeyProvider("key-1", key)).encrypt(randomByteArrayOfLength(10));

        AesGcmEncryptionService.KeyProvider keyProvider2 = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider2.getKey("key-1")).thenReturn(null);
        AesGcmEncryptionService service2 = readyService(keyProvider2);

        EncryptionKeyNotYetAvailableException e = expectThrows(
            EncryptionKeyNotYetAvailableException.class,
            () -> service2.decrypt(encrypted)
        );
        assertThat(e.getMessage(), containsString("key-1"));
        assertThat(e.getMessage(), containsString("not yet available"));
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testDecryptWithWrongKeyFails() {
        SecretKey key1 = randomAesKey();
        SecretKey key2 = randomAesKey();
        EncryptedData encrypted = readyService(mockKeyProvider("key-1", key1)).encrypt(randomByteArrayOfLength(10));

        AesGcmEncryptionService.KeyProvider keyProvider2 = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider2.getKey("key-1")).thenReturn(key2);
        AesGcmEncryptionService service2 = readyService(keyProvider2);

        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> service2.decrypt(encrypted));
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testDecryptWithTamperedPayloadFails() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", key));

        EncryptedData encrypted = service.encrypt(randomByteArrayOfLength(32));
        byte[] tampered = encrypted.payload().clone();
        tampered[tampered.length - 1] ^= (byte) 0xFF;

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

        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", randomAesKey()));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.decrypt(new EncryptedData("key-1", payload))
        );
        assertThat(e.getMessage(), containsString("unsupported serialization version"));
    }

    public void testDecryptWithTruncatedPayloadFails() {
        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", randomAesKey()));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> service.decrypt(new EncryptedData("key-1", new byte[] { 0x01, 0x01 }))
        );
        assertThat(e.getMessage(), containsString("ciphertext too short"));
    }

    public void testEncryptFailsWhenKeyNotAvailableAndStateReady() {
        AesGcmEncryptionService.KeyProvider keyProvider = mock(AesGcmEncryptionService.KeyProvider.class);
        when(keyProvider.getActiveKey()).thenReturn(null);
        AesGcmEncryptionService service = readyService(keyProvider);

        EncryptionKeyNotYetAvailableException e = expectThrows(
            EncryptionKeyNotYetAvailableException.class,
            () -> service.encrypt(new byte[] { 1 })
        );
        assertThat(e.getMessage(), containsString("not yet available"));
        assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
    }

    public void testEncryptThrowsUnavailableWhenDegraded() {
        for (EncryptionServiceState state : Arrays.stream(EncryptionServiceState.values())
            .filter(s -> s != EncryptionServiceState.READY)
            .toArray(EncryptionServiceState[]::new)) {

            AesGcmEncryptionService service = new AesGcmEncryptionService(nullKeyProvider(), () -> state, () -> true);
            EncryptionServiceUnavailableException e = expectThrows(
                EncryptionServiceUnavailableException.class,
                () -> service.encrypt(new byte[] { 1 })
            );
            assertThat(e.serviceState(), equalTo(state));
            assertThat(e.getMessage(), containsString(state.displayValue()));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
    }

    public void testDecryptThrowsUnavailableWhenDegraded() {
        SecretKey key = randomAesKey();
        EncryptedData encrypted = readyService(mockKeyProvider("key-1", key)).encrypt(randomByteArrayOfLength(16));

        for (EncryptionServiceState state : Arrays.stream(EncryptionServiceState.values())
            .filter(s -> s != EncryptionServiceState.READY)
            .toArray(EncryptionServiceState[]::new)) {

            AesGcmEncryptionService service = new AesGcmEncryptionService(nullKeyProvider(), () -> state, () -> true);
            EncryptionServiceUnavailableException e = expectThrows(
                EncryptionServiceUnavailableException.class,
                () -> service.decrypt(encrypted)
            );
            assertThat(e.serviceState(), equalTo(state));
            assertThat(e.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
    }

    public void testEncryptEmptyPlaintext() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", key));

        EncryptedData encrypted = service.encrypt(new byte[0]);
        byte[] decrypted = service.decrypt(encrypted);
        assertThat(decrypted.length, equalTo(0));
    }

    public void testDifferentEncryptionsProduceDifferentPayloads() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", key));

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
        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", key));

        EncryptedData encrypted = service.encrypt(randomByteArrayOfLength(32));
        byte[] tampered = encrypted.payload().clone();
        // IV starts at offset 1 (after version byte)
        tampered[1] ^= (byte) 0xFF;

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
        AesGcmEncryptionService service = readyService(keyProvider);

        EncryptedData encrypted = service.encrypt(randomByteArrayOfLength(32));

        ElasticsearchException e = expectThrows(
            ElasticsearchException.class,
            () -> service.decrypt(new EncryptedData("key-2", encrypted.payload()))
        );
        assertThat(e.getMessage(), containsString("decryption failed"));
    }

    public void testLargePayloadRoundTrip() {
        SecretKey key = randomAesKey();
        AesGcmEncryptionService service = readyService(mockKeyProvider("key-1", key));

        byte[] plaintext = randomByteArrayOfLength(1024 * 1024);
        EncryptedData encrypted = service.encrypt(plaintext);
        byte[] decrypted = service.decrypt(encrypted);
        assertArrayEquals(plaintext, decrypted);
    }
}
