/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.ByteArrayInputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.SecureRandom;
import java.util.Arrays;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

public class DecryptionPacketsInputStreamTests extends ESTestCase {

    public void testSuccessEncryptAndDecryptSmallPacketLength() throws Exception {
        int len = 8 + Randomness.get().nextInt(8);
        byte[] plainBytes = new byte[len];
        Randomness.get().nextBytes(plainBytes);
        SecretKey secretKey = generateSecretKey();
        int nonce = Randomness.get().nextInt();
        for (int packetLen : Arrays.asList(1, 2, 3, 4)) {
            testEncryptAndDecryptSuccess(plainBytes, secretKey, nonce, packetLen);
        }
    }

    public void testSuccessEncryptAndDecryptLargePacketLength() throws Exception {
        int len = 256 + Randomness.get().nextInt(256);
        byte[] plainBytes = new byte[len];
        Randomness.get().nextBytes(plainBytes);
        SecretKey secretKey = generateSecretKey();
        int nonce = Randomness.get().nextInt();
        for (int packetLen : Arrays.asList(len - 1, len - 2, len - 3, len - 4)) {
            testEncryptAndDecryptSuccess(plainBytes, secretKey, nonce, packetLen);
        }
    }

    public void testSuccessEncryptAndDecryptTypicalPacketLength() throws Exception {
        int len = 1024 + Randomness.get().nextInt(512);
        byte[] plainBytes = new byte[len];
        Randomness.get().nextBytes(plainBytes);
        SecretKey secretKey = generateSecretKey();
        int nonce = Randomness.get().nextInt();
        for (int packetLen : Arrays.asList(128, 256, 512)) {
            testEncryptAndDecryptSuccess(plainBytes, secretKey, nonce, packetLen);
        }
    }

    public void testFailureEncryptAndDecryptWrongKey() throws Exception {
        int len = 256 + Randomness.get().nextInt(256);
        // 2-3 packets
        int packetLen = 1 + Randomness.get().nextInt(len / 2);
        byte[] plainBytes = new byte[len];
        Randomness.get().nextBytes(plainBytes);
        SecretKey encryptSecretKey = generateSecretKey();
        SecretKey decryptSecretKey = generateSecretKey();
        int nonce = Randomness.get().nextInt();
        byte[] encryptedBytes;
        try (
            InputStream in = new EncryptionPacketsInputStream(
                new ByteArrayInputStream(plainBytes, 0, len),
                encryptSecretKey,
                nonce,
                packetLen
            )
        ) {
            encryptedBytes = in.readAllBytes();
        }
        try (InputStream in = new DecryptionPacketsInputStream(new ByteArrayInputStream(encryptedBytes), decryptSecretKey, packetLen)) {
            IOException e = expectThrows(IOException.class, () -> { in.readAllBytes(); });
            assertThat(e.getMessage(), Matchers.is("Exception during packet decryption"));
        }
    }

    public void testFailureEncryptAndDecryptAlteredCiphertext() throws Exception {
        int len = 8 + Randomness.get().nextInt(8);
        // one packet
        int packetLen = len + Randomness.get().nextInt(8);
        byte[] plainBytes = new byte[len];
        Randomness.get().nextBytes(plainBytes);
        SecretKey secretKey = generateSecretKey();
        int nonce = Randomness.get().nextInt();
        byte[] encryptedBytes;
        try (InputStream in = new EncryptionPacketsInputStream(new ByteArrayInputStream(plainBytes, 0, len), secretKey, nonce, packetLen)) {
            encryptedBytes = in.readAllBytes();
        }
        for (int i = EncryptedRepository.GCM_IV_LENGTH_IN_BYTES; i < EncryptedRepository.GCM_IV_LENGTH_IN_BYTES + len
            + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES; i++) {
            for (int j = 0; j < 8; j++) {
                // flip bit
                encryptedBytes[i] ^= (1 << j);
                // fail decryption
                try (InputStream in = new DecryptionPacketsInputStream(new ByteArrayInputStream(encryptedBytes), secretKey, packetLen)) {
                    IOException e = expectThrows(IOException.class, () -> { in.readAllBytes(); });
                    assertThat(e.getMessage(), Matchers.is("Exception during packet decryption"));
                }
                // flip bit back
                encryptedBytes[i] ^= (1 << j);
            }
        }
    }

    public void testFailureEncryptAndDecryptAlteredCiphertextIV() throws Exception {
        int len = 8 + Randomness.get().nextInt(8);
        int packetLen = 4 + Randomness.get().nextInt(4);
        byte[] plainBytes = new byte[len];
        Randomness.get().nextBytes(plainBytes);
        SecretKey secretKey = generateSecretKey();
        int nonce = Randomness.get().nextInt();
        byte[] encryptedBytes;
        try (InputStream in = new EncryptionPacketsInputStream(new ByteArrayInputStream(plainBytes, 0, len), secretKey, nonce, packetLen)) {
            encryptedBytes = in.readAllBytes();
        }
        assertThat(encryptedBytes.length, Matchers.is((int) EncryptionPacketsInputStream.getEncryptionLength(len, packetLen)));
        int encryptedPacketLen = EncryptedRepository.GCM_IV_LENGTH_IN_BYTES + packetLen + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES;
        for (int i = 0; i < encryptedBytes.length; i += encryptedPacketLen) {
            for (int j = 0; j < EncryptedRepository.GCM_IV_LENGTH_IN_BYTES; j++) {
                for (int k = 0; k < 8; k++) {
                    // flip bit
                    encryptedBytes[i + j] ^= (1 << k);
                    try (
                        InputStream in = new DecryptionPacketsInputStream(new ByteArrayInputStream(encryptedBytes), secretKey, packetLen)
                    ) {
                        IOException e = expectThrows(IOException.class, () -> { in.readAllBytes(); });
                        if (j < Integer.BYTES) {
                            assertThat(e.getMessage(), Matchers.startsWith("Exception during packet decryption"));
                        } else {
                            assertThat(e.getMessage(), Matchers.startsWith("Packet counter mismatch"));
                        }
                    }
                    // flip bit back
                    encryptedBytes[i + j] ^= (1 << k);
                }
            }
        }
    }

    private void testEncryptAndDecryptSuccess(byte[] plainBytes, SecretKey secretKey, int nonce, int packetLen) throws Exception {
        for (int len = 0; len <= plainBytes.length; len++) {
            byte[] encryptedBytes;
            try (
                InputStream in = new EncryptionPacketsInputStream(new ByteArrayInputStream(plainBytes, 0, len), secretKey, nonce, packetLen)
            ) {
                encryptedBytes = in.readAllBytes();
            }
            assertThat((long) encryptedBytes.length, Matchers.is(EncryptionPacketsInputStream.getEncryptionLength(len, packetLen)));
            byte[] decryptedBytes;
            try (
                InputStream in = new DecryptionPacketsInputStream(
                    new ReadLessFilterInputStream(new ByteArrayInputStream(encryptedBytes)),
                    secretKey,
                    packetLen
                )
            ) {
                decryptedBytes = in.readAllBytes();
            }
            assertThat(decryptedBytes.length, Matchers.is(len));
            assertThat(
                (long) decryptedBytes.length,
                Matchers.is(DecryptionPacketsInputStream.getDecryptionLength(encryptedBytes.length, packetLen))
            );
            for (int i = 0; i < len; i++) {
                assertThat(decryptedBytes[i], Matchers.is(plainBytes[i]));
            }
        }
    }

    // input stream that reads less bytes than asked to, testing that packet-wide reads don't rely on `read` calls for memory buffers which
    // always return the same number of bytes they are asked to
    private static class ReadLessFilterInputStream extends FilterInputStream {

        protected ReadLessFilterInputStream(InputStream in) {
            super(in);
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (len == 0) {
                return 0;
            }
            return super.read(b, off, randomIntBetween(1, len));
        }
    }

    private SecretKey generateSecretKey() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256, new SecureRandom());
        return keyGen.generateKey();
    }
}
