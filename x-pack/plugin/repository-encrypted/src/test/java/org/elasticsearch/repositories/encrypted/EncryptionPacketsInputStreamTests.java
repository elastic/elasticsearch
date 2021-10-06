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
import org.junit.BeforeClass;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.crypto.Cipher;
import javax.crypto.CipherInputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class EncryptionPacketsInputStreamTests extends ESTestCase {

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
        testEncryptPacketWise(1, 3, new DefaultBufferedReadAllStrategy());
        int packetSize = 4 + Randomness.get().nextInt(2046);
        testEncryptPacketWise(1, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testSizeSmallerThanPacketSize() throws Exception {
        int packetSize = 3 + Randomness.get().nextInt(2045);
        testEncryptPacketWise(packetSize - 1, packetSize, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(packetSize - 2, packetSize, new DefaultBufferedReadAllStrategy());
        int size = 1 + Randomness.get().nextInt(packetSize - 1);
        testEncryptPacketWise(size, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testSizeEqualToPacketSize() throws Exception {
        int packetSize = 1 + Randomness.get().nextInt(2048);
        testEncryptPacketWise(packetSize, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testSizeLargerThanPacketSize() throws Exception {
        int packetSize = 1 + Randomness.get().nextInt(2048);
        testEncryptPacketWise(packetSize + 1, packetSize, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(packetSize + 2, packetSize, new DefaultBufferedReadAllStrategy());
        int size = packetSize + 3 + Randomness.get().nextInt(packetSize);
        testEncryptPacketWise(size, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testSizeMultipleOfPacketSize() throws Exception {
        int packetSize = 1 + Randomness.get().nextInt(512);
        testEncryptPacketWise(2 * packetSize, packetSize, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(3 * packetSize, packetSize, new DefaultBufferedReadAllStrategy());
        int packetCount = 4 + Randomness.get().nextInt(12);
        testEncryptPacketWise(packetCount * packetSize, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testSizeAlmostMultipleOfPacketSize() throws Exception {
        int packetSize = 3 + Randomness.get().nextInt(510);
        int packetCount = 2 + Randomness.get().nextInt(15);
        testEncryptPacketWise(packetCount * packetSize - 1, packetSize, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(packetCount * packetSize - 2, packetSize, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(packetCount * packetSize + 1, packetSize, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(packetCount * packetSize + 2, packetSize, new DefaultBufferedReadAllStrategy());
    }

    public void testShortPacketSizes() throws Exception {
        int packetCount = 2 + Randomness.get().nextInt(15);
        testEncryptPacketWise(2 + Randomness.get().nextInt(15), 1, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(4 + Randomness.get().nextInt(30), 2, new DefaultBufferedReadAllStrategy());
        testEncryptPacketWise(6 + Randomness.get().nextInt(45), 3, new DefaultBufferedReadAllStrategy());
    }

    public void testPacketSizeMultipleOfAESBlockSize() throws Exception {
        int packetSize = 1 + Randomness.get().nextInt(8);
        testEncryptPacketWise(
            1 + Randomness.get().nextInt(packetSize * EncryptedRepository.AES_BLOCK_LENGTH_IN_BYTES),
            packetSize * EncryptedRepository.AES_BLOCK_LENGTH_IN_BYTES,
            new DefaultBufferedReadAllStrategy()
        );
        testEncryptPacketWise(
            packetSize * EncryptedRepository.AES_BLOCK_LENGTH_IN_BYTES + Randomness.get().nextInt(8192),
            packetSize * EncryptedRepository.AES_BLOCK_LENGTH_IN_BYTES,
            new DefaultBufferedReadAllStrategy()
        );
    }

    public void testMarkAndResetPacketBoundaryNoMock() throws Exception {
        int packetSize = 3 + Randomness.get().nextInt(512);
        int size = 4 * packetSize + Randomness.get().nextInt(512);
        int plaintextOffset = Randomness.get().nextInt(testPlaintextArray.length - size + 1);
        int nonce = Randomness.get().nextInt();
        final byte[] referenceCiphertextArray;
        try (
            InputStream encryptionInputStream = new EncryptionPacketsInputStream(
                new ByteArrayInputStream(testPlaintextArray, plaintextOffset, size),
                secretKey,
                nonce,
                packetSize
            )
        ) {
            referenceCiphertextArray = encryptionInputStream.readAllBytes();
        }
        assertThat((long) referenceCiphertextArray.length, Matchers.is(EncryptionPacketsInputStream.getEncryptionLength(size, packetSize)));
        int encryptedPacketSize = packetSize + EncryptedRepository.GCM_IV_LENGTH_IN_BYTES + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES;
        try (
            InputStream encryptionInputStream = new EncryptionPacketsInputStream(
                new ByteArrayInputStream(testPlaintextArray, plaintextOffset, size),
                secretKey,
                nonce,
                packetSize
            )
        ) {
            // mark at the beginning
            encryptionInputStream.mark(encryptedPacketSize - 1);
            byte[] test = encryptionInputStream.readNBytes(1 + Randomness.get().nextInt(encryptedPacketSize));
            assertSubArray(referenceCiphertextArray, 0, test, 0, test.length);
            // reset at the beginning
            encryptionInputStream.reset();
            // read packet fragment
            test = encryptionInputStream.readNBytes(1 + Randomness.get().nextInt(encryptedPacketSize));
            assertSubArray(referenceCiphertextArray, 0, test, 0, test.length);
            // reset at the beginning
            encryptionInputStream.reset();
            // read complete packet
            test = encryptionInputStream.readNBytes(encryptedPacketSize);
            assertSubArray(referenceCiphertextArray, 0, test, 0, test.length);
            // mark at the second packet boundary
            encryptionInputStream.mark(Integer.MAX_VALUE);
            // read more than one packet
            test = encryptionInputStream.readNBytes(encryptedPacketSize + 1 + Randomness.get().nextInt(encryptedPacketSize));
            assertSubArray(referenceCiphertextArray, encryptedPacketSize, test, 0, test.length);
            // reset at the second packet boundary
            encryptionInputStream.reset();
            int middlePacketOffset = Randomness.get().nextInt(encryptedPacketSize);
            test = encryptionInputStream.readNBytes(middlePacketOffset);
            assertSubArray(referenceCiphertextArray, encryptedPacketSize, test, 0, test.length);
            // read up to the third packet boundary
            test = encryptionInputStream.readNBytes(encryptedPacketSize - middlePacketOffset);
            assertSubArray(referenceCiphertextArray, encryptedPacketSize + middlePacketOffset, test, 0, test.length);
            // mark at the third packet boundary
            encryptionInputStream.mark(Integer.MAX_VALUE);
            test = encryptionInputStream.readAllBytes();
            assertSubArray(referenceCiphertextArray, 2 * encryptedPacketSize, test, 0, test.length);
            encryptionInputStream.reset();
            test = encryptionInputStream.readNBytes(
                1 + Randomness.get().nextInt(referenceCiphertextArray.length - 2 * encryptedPacketSize)
            );
            assertSubArray(referenceCiphertextArray, 2 * encryptedPacketSize, test, 0, test.length);
        }
    }

    public void testMarkResetInsidePacketNoMock() throws Exception {
        int packetSize = 3 + Randomness.get().nextInt(64);
        int encryptedPacketSize = EncryptedRepository.GCM_IV_LENGTH_IN_BYTES + packetSize + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES;
        int size = 3 * packetSize + Randomness.get().nextInt(64);
        byte[] bytes = new byte[size];
        Randomness.get().nextBytes(bytes);
        int nonce = Randomness.get().nextInt();
        EncryptionPacketsInputStream test = new EncryptionPacketsInputStream(new TestInputStream(bytes), secretKey, nonce, packetSize);
        int offset1 = 1 + Randomness.get().nextInt(encryptedPacketSize - 1);
        // read past the first packet
        test.readNBytes(encryptedPacketSize + offset1);
        assertThat(test.counter, Matchers.is(Long.MIN_VALUE + 2));
        assertThat(((CountingInputStream) test.currentIn).count, Matchers.is((long) offset1));
        assertThat(test.markCounter, Matchers.nullValue());
        int readLimit = 1 + Randomness.get().nextInt(packetSize);
        // first mark
        test.mark(readLimit);
        assertThat(test.markCounter, Matchers.is(test.counter));
        assertThat(test.markSourceOnNextPacket, Matchers.is(readLimit));
        assertThat(test.markIn, Matchers.is(test.currentIn));
        assertThat(((CountingInputStream) test.markIn).mark, Matchers.is((long) offset1));
        assertThat(((TestInputStream) test.source).mark, Matchers.is(-1));
        // read before packet is complete
        test.readNBytes(1 + Randomness.get().nextInt(encryptedPacketSize - offset1));
        assertThat(((TestInputStream) test.source).mark, Matchers.is(-1));
        // reset
        test.reset();
        assertThat(test.markSourceOnNextPacket, Matchers.is(readLimit));
        assertThat(test.counter, Matchers.is(test.markCounter));
        assertThat(test.currentIn, Matchers.is(test.markIn));
        assertThat(((CountingInputStream) test.currentIn).count, Matchers.is((long) offset1));
        // read before the packet is complete
        int offset2 = 1 + Randomness.get().nextInt(encryptedPacketSize - offset1);
        test.readNBytes(offset2);
        assertThat(((TestInputStream) test.source).mark, Matchers.is(-1));
        assertThat(((CountingInputStream) test.currentIn).count, Matchers.is((long) offset1 + offset2));
        // second mark
        readLimit = 1 + Randomness.get().nextInt(packetSize);
        test.mark(readLimit);
        assertThat(((TestInputStream) test.source).mark, Matchers.is(-1));
        assertThat(test.markCounter, Matchers.is(test.counter));
        assertThat(test.markSourceOnNextPacket, Matchers.is(readLimit));
        assertThat(test.markIn, Matchers.is(test.currentIn));
        assertThat(((CountingInputStream) test.markIn).mark, Matchers.is((long) offset1 + offset2));
    }

    public void testMarkResetAcrossPacketsNoMock() throws Exception {
        int packetSize = 3 + Randomness.get().nextInt(64);
        int encryptedPacketSize = EncryptedRepository.GCM_IV_LENGTH_IN_BYTES + packetSize + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES;
        int size = 3 * packetSize + Randomness.get().nextInt(64);
        byte[] bytes = new byte[size];
        Randomness.get().nextBytes(bytes);
        int nonce = Randomness.get().nextInt();
        EncryptionPacketsInputStream test = new EncryptionPacketsInputStream(new TestInputStream(bytes), secretKey, nonce, packetSize);
        int readLimit = 2 * size + Randomness.get().nextInt(4096);
        // mark at the beginning
        test.mark(readLimit);
        assertThat(test.counter, Matchers.is(Long.MIN_VALUE));
        assertThat(test.markCounter, Matchers.is(Long.MIN_VALUE));
        assertThat(test.markSourceOnNextPacket, Matchers.is(readLimit));
        assertThat(test.markIn, Matchers.nullValue());
        // read past the first packet
        int offset1 = 1 + Randomness.get().nextInt(encryptedPacketSize);
        test.readNBytes(encryptedPacketSize + offset1);
        assertThat(test.markSourceOnNextPacket, Matchers.is(-1));
        assertThat(((TestInputStream) test.source).mark, Matchers.is(0));
        assertThat(test.counter, Matchers.is(Long.MIN_VALUE + 2));
        assertThat(((CountingInputStream) test.currentIn).count, Matchers.is((long) offset1));
        assertThat(test.markCounter, Matchers.is(Long.MIN_VALUE));
        assertThat(test.markIn, Matchers.nullValue());
        // reset at the beginning
        test.reset();
        assertThat(test.markSourceOnNextPacket, Matchers.is(-1));
        assertThat(test.counter, Matchers.is(Long.MIN_VALUE));
        assertThat(test.currentIn, Matchers.nullValue());
        assertThat(((TestInputStream) test.source).off, Matchers.is(0));
        // read past the first two packets
        int offset2 = 1 + Randomness.get().nextInt(encryptedPacketSize);
        test.readNBytes(2 * encryptedPacketSize + offset2);
        assertThat(test.markSourceOnNextPacket, Matchers.is(-1));
        assertThat(((TestInputStream) test.source).mark, Matchers.is(0));
        assertThat(test.counter, Matchers.is(Long.MIN_VALUE + 3));
        assertThat(((CountingInputStream) test.currentIn).count, Matchers.is((long) offset2));
        assertThat(test.markCounter, Matchers.is(Long.MIN_VALUE));
        assertThat(test.markIn, Matchers.nullValue());
        // mark inside the third packet
        test.mark(readLimit);
        assertThat(test.markCounter, Matchers.is(Long.MIN_VALUE + 3));
        assertThat(test.markSourceOnNextPacket, Matchers.is(readLimit));
        assertThat(((CountingInputStream) test.currentIn).count, Matchers.is((long) offset2));
        assertThat(test.markIn, Matchers.is(test.currentIn));
        assertThat(((CountingInputStream) test.markIn).mark, Matchers.is((long) offset2));
        // read until the end
        test.readAllBytes();
        assertThat(test.markCounter, Matchers.is(Long.MIN_VALUE + 3));
        assertThat(test.counter, Matchers.not(Long.MIN_VALUE + 3));
        assertThat(test.markSourceOnNextPacket, Matchers.is(-1));
        assertThat(test.markIn, Matchers.not(test.currentIn));
        assertThat(((CountingInputStream) test.markIn).mark, Matchers.is((long) offset2));
        // reset
        test.reset();
        assertThat(test.markSourceOnNextPacket, Matchers.is(-1));
        assertThat(test.counter, Matchers.is(Long.MIN_VALUE + 3));
        assertThat(((CountingInputStream) test.currentIn).count, Matchers.is((long) offset2));
        assertThat(test.markIn, Matchers.is(test.currentIn));
    }

    public void testMarkAfterResetNoMock() throws Exception {
        int packetSize = 4 + Randomness.get().nextInt(4);
        int plainLen = packetSize + 1 + Randomness.get().nextInt(packetSize - 1);
        int plaintextOffset = Randomness.get().nextInt(testPlaintextArray.length - plainLen + 1);
        int nonce = Randomness.get().nextInt();
        final byte[] referenceCiphertextArray;
        try (
            InputStream encryptionInputStream = new EncryptionPacketsInputStream(
                new ByteArrayInputStream(testPlaintextArray, plaintextOffset, plainLen),
                secretKey,
                nonce,
                packetSize
            )
        ) {
            referenceCiphertextArray = encryptionInputStream.readAllBytes();
        }
        int encryptedLen = referenceCiphertextArray.length;
        assertThat((long) encryptedLen, Matchers.is(EncryptionPacketsInputStream.getEncryptionLength(plainLen, packetSize)));
        for (int mark1 = 0; mark1 < encryptedLen; mark1++) {
            for (int offset1 = 0; offset1 < encryptedLen - mark1; offset1++) {
                int mark2 = Randomness.get().nextInt(encryptedLen - mark1);
                int offset2 = Randomness.get().nextInt(encryptedLen - mark1 - mark2);
                EncryptionPacketsInputStream test = new EncryptionPacketsInputStream(
                    new ByteArrayInputStream(testPlaintextArray, plaintextOffset, plainLen),
                    secretKey,
                    nonce,
                    packetSize
                );
                // read "mark1" bytes
                byte[] pre = test.readNBytes(mark1);
                for (int i = 0; i < pre.length; i++) {
                    assertThat(pre[i], Matchers.is(referenceCiphertextArray[i]));
                }
                // first mark
                test.mark(encryptedLen);
                // read "offset" bytes
                byte[] span1 = test.readNBytes(offset1);
                for (int i = 0; i < span1.length; i++) {
                    assertThat(span1[i], Matchers.is(referenceCiphertextArray[mark1 + i]));
                }
                // reset back to "mark1" offset
                test.reset();
                // read/replay "mark2" bytes
                byte[] span2 = test.readNBytes(mark2);
                for (int i = 0; i < span2.length; i++) {
                    assertThat(span2[i], Matchers.is(referenceCiphertextArray[mark1 + i]));
                }
                // second mark
                test.mark(encryptedLen);
                byte[] span3 = test.readNBytes(offset2);
                for (int i = 0; i < span3.length; i++) {
                    assertThat(span3[i], Matchers.is(referenceCiphertextArray[mark1 + mark2 + i]));
                }
                // reset to second mark
                test.reset();
                // read rest of bytes
                byte[] span4 = test.readAllBytes();
                for (int i = 0; i < span4.length; i++) {
                    assertThat(span4[i], Matchers.is(referenceCiphertextArray[mark1 + mark2 + i]));
                }
            }
        }
    }

    public void testMark() throws Exception {
        InputStream mockSource = mock(InputStream.class);
        when(mockSource.markSupported()).thenAnswer(invocationOnMock -> true);
        EncryptionPacketsInputStream test = new EncryptionPacketsInputStream(
            mockSource,
            mock(SecretKey.class),
            Randomness.get().nextInt(),
            1 + Randomness.get().nextInt(32)
        );
        int readLimit = 1 + Randomness.get().nextInt(4096);
        InputStream mockMarkIn = mock(InputStream.class);
        test.markIn = mockMarkIn;
        InputStream mockCurrentIn = mock(InputStream.class);
        test.currentIn = mockCurrentIn;
        test.counter = Randomness.get().nextLong();
        test.markCounter = Randomness.get().nextLong();
        test.markSourceOnNextPacket = Randomness.get().nextInt();
        // mark
        test.mark(readLimit);
        verify(mockMarkIn).close();
        assertThat(test.markIn, Matchers.is(mockCurrentIn));
        verify(test.markIn).mark(Mockito.anyInt());
        assertThat(test.currentIn, Matchers.is(mockCurrentIn));
        assertThat(test.markCounter, Matchers.is(test.counter));
        assertThat(test.markSourceOnNextPacket, Matchers.is(readLimit));
    }

    public void testReset() throws Exception {
        InputStream mockSource = mock(InputStream.class);
        when(mockSource.markSupported()).thenAnswer(invocationOnMock -> true);
        EncryptionPacketsInputStream test = new EncryptionPacketsInputStream(
            mockSource,
            mock(SecretKey.class),
            Randomness.get().nextInt(),
            1 + Randomness.get().nextInt(32)
        );
        InputStream mockMarkIn = mock(InputStream.class);
        test.markIn = mockMarkIn;
        InputStream mockCurrentIn = mock(InputStream.class);
        test.currentIn = mockCurrentIn;
        test.counter = Randomness.get().nextLong();
        test.markCounter = Randomness.get().nextLong();
        // source requires reset as well
        test.markSourceOnNextPacket = -1;
        // reset
        test.reset();
        verify(mockCurrentIn).close();
        assertThat(test.currentIn, Matchers.is(mockMarkIn));
        verify(test.currentIn).reset();
        assertThat(test.markIn, Matchers.is(mockMarkIn));
        assertThat(test.counter, Matchers.is(test.markCounter));
        assertThat(test.markSourceOnNextPacket, Matchers.is(-1));
        verify(mockSource).reset();
    }

    private void testEncryptPacketWise(int size, int packetSize, ReadStrategy readStrategy) throws Exception {
        int encryptedPacketSize = packetSize + EncryptedRepository.GCM_IV_LENGTH_IN_BYTES + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES;
        int plaintextOffset = Randomness.get().nextInt(testPlaintextArray.length - size + 1);
        int nonce = Randomness.get().nextInt();
        long counter = EncryptedRepository.PACKET_START_COUNTER;
        try (
            InputStream encryptionInputStream = new EncryptionPacketsInputStream(
                new ByteArrayInputStream(testPlaintextArray, plaintextOffset, size),
                secretKey,
                nonce,
                packetSize
            )
        ) {
            byte[] ciphertextArray = readStrategy.readAll(encryptionInputStream);
            assertThat((long) ciphertextArray.length, Matchers.is(EncryptionPacketsInputStream.getEncryptionLength(size, packetSize)));
            for (int ciphertextOffset = 0; ciphertextOffset < ciphertextArray.length; ciphertextOffset += encryptedPacketSize) {
                ByteBuffer ivBuffer = ByteBuffer.wrap(ciphertextArray, ciphertextOffset, EncryptedRepository.GCM_IV_LENGTH_IN_BYTES)
                    .order(ByteOrder.LITTLE_ENDIAN);
                assertThat(ivBuffer.getInt(), Matchers.is(nonce));
                assertThat(ivBuffer.getLong(), Matchers.is(counter++));
                GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(
                    EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES * Byte.SIZE,
                    Arrays.copyOfRange(ciphertextArray, ciphertextOffset, ciphertextOffset + EncryptedRepository.GCM_IV_LENGTH_IN_BYTES)
                );
                Cipher packetCipher = Cipher.getInstance(EncryptedRepository.DATA_ENCRYPTION_SCHEME);
                packetCipher.init(Cipher.DECRYPT_MODE, secretKey, gcmParameterSpec);
                try (
                    InputStream packetDecryptionInputStream = new CipherInputStream(
                        new ByteArrayInputStream(
                            ciphertextArray,
                            ciphertextOffset + EncryptedRepository.GCM_IV_LENGTH_IN_BYTES,
                            packetSize + EncryptedRepository.GCM_TAG_LENGTH_IN_BYTES
                        ),
                        packetCipher
                    )
                ) {
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
            assertThat("Mismatch at index [" + i + "]", arr1[offset1 + i], Matchers.is(arr2[offset2 + i]));
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

    static class TestInputStream extends InputStream {

        final byte[] b;
        final int label;
        final int len;
        int off = 0;
        int mark = -1;
        final AtomicBoolean closed = new AtomicBoolean(false);

        TestInputStream(byte[] b) {
            this(b, 0, b.length, 0);
        }

        TestInputStream(byte[] b, int label) {
            this(b, 0, b.length, label);
        }

        TestInputStream(byte[] b, int offset, int len, int label) {
            this.b = b;
            this.off = offset;
            this.len = len;
            this.label = label;
        }

        @Override
        public int read() throws IOException {
            if (b == null || off >= len) {
                return -1;
            }
            return b[off++] & 0xFF;
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
        }

        @Override
        public void mark(int readlimit) {
            this.mark = off;
        }

        @Override
        public void reset() {
            this.off = this.mark;
        }

        @Override
        public boolean markSupported() {
            return true;
        }

    }

}
