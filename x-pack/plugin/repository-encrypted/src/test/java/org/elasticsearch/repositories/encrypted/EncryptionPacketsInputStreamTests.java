/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

public class EncryptionPacketsInputStreamTests extends ESTestCase {

    // test odd packet lengths (multiple of AES_BLOCK_SIZE or not)
    // test non AES Key
    // test read strategies (mark/reset)

    private static int TEST_ARRAY_SIZE = 1 << 20;
    private static byte[] testPlaintextArray;
    private static SecretKey secretKey;

    @BeforeClass
    static void createSecretKeyAndTestArray() throws Exception {
        try {
            KeyGenerator keyGen = KeyGenerator.getInstance("AES");
            keyGen.init(256, new SecureRandom());
            secretKey = keyGen.generateKey();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        testPlaintextArray = new byte[TEST_ARRAY_SIZE];
        Randomness.get().nextBytes(testPlaintextArray);
    }

    public void testEmpty() throws Exception {
        int packetSize = 1 + Randomness.get().nextInt(2048);
        testEncryptPacketWise(0, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testSingleByteSize() throws Exception {
        testEncryptPacketWise(1, 1, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(1, 2, new DefaultBufferedReadAllStrategy());
        int packetSize = 3 + Randomness.get().nextInt(2046);
        testEncryptPacketWise(1, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testSizeSmallerThanPacket() throws Exception {
        int packetSize = 3 + Randomness.get().nextInt(2045);
        int size = 1 + Randomness.get().nextInt(packetSize - 1);
        testEncryptPacketWise(size, packetSize, new DefaultBufferedReadAllStrategy());
    }

    private void testEncryptPacketWise(int size, int packetSize, ReadStrategy readStrategy) throws Exception {
        int encryptedPacketSize = packetSize + EncryptedRepository.GCM_IV_SIZE_IN_BYTES + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES;
        int plaintextOffset = Randomness.get().nextInt(testPlaintextArray.length - size + 1);
        int nonce = Randomness.get().nextInt();
        long counter = EncryptedRepository.PACKET_START_COUNTER;
        try (InputStream encryptionInputStream = new EncryptionPacketsInputStream(new ByteArrayInputStream(testPlaintextArray,
                plaintextOffset, size), secretKey, nonce, packetSize)) {
            byte[] ciphertextArray = readStrategy.readAll(encryptionInputStream);
            assertThat((long)ciphertextArray.length, Matchers.is(EncryptionPacketsInputStream.getEncryptionSize(size, packetSize)));
            for (int ciphertextOffset = 0; ciphertextOffset < ciphertextArray.length; ciphertextOffset += encryptedPacketSize) {
                ByteBuffer ivBuffer = ByteBuffer.wrap(ciphertextArray, ciphertextOffset,
                        EncryptedRepository.GCM_IV_SIZE_IN_BYTES).order(ByteOrder.LITTLE_ENDIAN);
                assertThat(ivBuffer.getInt(), Matchers.is(nonce));
                assertThat(ivBuffer.getLong(), Matchers.is(counter++));
                GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(EncryptedRepository.GCM_TAG_SIZE_IN_BYTES * Byte.SIZE,
                        Arrays.copyOfRange(ciphertextArray, ciphertextOffset,
                                ciphertextOffset + EncryptedRepository.GCM_IV_SIZE_IN_BYTES));
                Cipher packetCipher = Cipher.getInstance(EncryptedRepository.GCM_ENCRYPTION_SCHEME);
                packetCipher.init(Cipher.DECRYPT_MODE, secretKey, gcmParameterSpec);
                try (InputStream packetDecryptionInputStream = new CipherInputStream(new ByteArrayInputStream(ciphertextArray,
                        ciphertextOffset + EncryptedRepository.GCM_IV_SIZE_IN_BYTES,
                        packetSize + EncryptedRepository.GCM_TAG_SIZE_IN_BYTES), packetCipher)) {
                    byte[] decryptedCiphertext = packetDecryptionInputStream.readAllBytes();
                    int decryptedPacketSize = size <= packetSize ? size : packetSize;
                    assertThat(decryptedCiphertext.length, Matchers.is(decryptedPacketSize));
                    assertSubArray(decryptedCiphertext, 0, testPlaintextArray, plaintextOffset, decryptedPacketSize);
                    size -= decryptedPacketSize;
                    plaintextOffset += decryptedPacketSize;
                }
            }
        }
    }

    private void assertSubArray(byte[] arr1, int offset1, byte[] arr2, int offset2, int length) {
        Objects.checkFromIndexSize(offset1, length, arr1.length);
        Objects.checkFromIndexSize(offset2, length, arr2.length);
        for (int i = 0; i < length; i++) {
            assertThat(arr1[offset1 + i], Matchers.is(arr2[offset2 + i]));
        }
    }

    interface ReadStrategy {
        byte[] readAll(InputStream inputStream) throws IOException;
    }

    static class DefaultBufferedReadAllStrategy implements ReadStrategy {
        @Override
        public byte[] readAll(InputStream inputStream) throws IOException {
            return inputStream.readAllBytes();
        }
    }

}
