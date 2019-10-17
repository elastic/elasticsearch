/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;

import javax.crypto.AEADBadTagException;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;

import static org.elasticsearch.repositories.encrypted.GCMPacketsCipherInputStream.ENCRYPTED_PACKET_SIZE_IN_BYTES;
import static org.elasticsearch.repositories.encrypted.GCMPacketsCipherInputStream.READ_BUFFER_SIZE_IN_BYTES;

public class GCMPacketsCipherInputStreamTests extends ESTestCase {

    private static int TEST_ARRAY_SIZE = 8 * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES;
    private static byte[] testPlaintextArray;
    private static BouncyCastleFipsProvider bcFipsProvider;
    private SecretKey secretKey;

    @BeforeClass
    static void setupProvider() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            GCMPacketsCipherInputStreamTests.bcFipsProvider = new BouncyCastleFipsProvider();
            return null;
        });
        testPlaintextArray = new byte[TEST_ARRAY_SIZE];
        Randomness.get().nextBytes(testPlaintextArray);
    }

    @Before
    void createSecretKey() throws Exception {
        secretKey = generateSecretKey();
    }

    public void testEncryptDecryptEmpty() throws Exception {
        testEncryptDecryptRandomOfLength(0, secretKey);
    }

    public void testEncryptDecryptSmallerThanBufferSize() throws Exception {
        for (int i = 1; i < GCMPacketsCipherInputStream.READ_BUFFER_SIZE_IN_BYTES; i++) {
            testEncryptDecryptRandomOfLength(i, secretKey);
        }
    }

    public void testEncryptDecryptMultipleOfBufferSize() throws Exception {
        for (int i = 1; i < 10; i++) {
            testEncryptDecryptRandomOfLength(i * GCMPacketsCipherInputStream.READ_BUFFER_SIZE_IN_BYTES, secretKey);
        }
    }

    public void testEncryptDecryptSmallerThanPacketSize() throws Exception {
        for (int i = GCMPacketsCipherInputStream.READ_BUFFER_SIZE_IN_BYTES + 1; i < GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES; i++) {
            testEncryptDecryptRandomOfLength(i, secretKey);
        }
    }

    public void testEncryptDecryptLargerThanPacketSize() throws Exception {
        for (int i = GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES + 1; i <= GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES * 3; i++) {
            testEncryptDecryptRandomOfLength(i, secretKey);
        }
    }

    public void testEncryptDecryptMultipleOfPacketSize() throws Exception {
        for (int i = 1; i <= 6; i++) {
            testEncryptDecryptRandomOfLength(i * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES, secretKey);
        }
    }

    public void testMarkAndResetAtBeginningForEncryption() throws Exception {
        testMarkAndResetToSameOffsetForEncryption(0);
        testMarkAndResetToSameOffsetForEncryption(Math.toIntExact(GCMPacketsCipherInputStream.
                getEncryptionSizeFromPlainSize(GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES)));
    }

    public void testMarkAndResetFirstPacketForEncryption() throws Exception {
        for (int i = 1; i < ENCRYPTED_PACKET_SIZE_IN_BYTES; i++) {
            testMarkAndResetToSameOffsetForEncryption(i);
        }
    }

    public void testMarkAndResetRandomSecondPacketForEncryption() throws Exception {
        for (int i = ENCRYPTED_PACKET_SIZE_IN_BYTES + 1; i < 2 * ENCRYPTED_PACKET_SIZE_IN_BYTES; i++) {
            testMarkAndResetToSameOffsetForEncryption(i);
        }
    }

    public void testMarkAndResetCrawlForEncryption() throws Exception {
        int length = GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES;
        int startIndex = randomIntBetween(0, testPlaintextArray.length - length);
        int nonce = Randomness.get().nextInt();
        byte[] ciphertextBytes;
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, nonce, bcFipsProvider)) {
            ciphertextBytes = cipherInputStream.readAllBytes();
        }
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, nonce, bcFipsProvider)) {
            cipherInputStream.mark(Integer.MAX_VALUE);
            for (int i = 0; i < GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length); i++) {
                int skipSize = randomIntBetween(1, Math.toIntExact(GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length)) - i);
                // skip bytes
                cipherInputStream.readNBytes(skipSize);
                cipherInputStream.reset();
                // re-read one byte of the skipped bytes
                int byteRead = cipherInputStream.read();
                // mark the one byte progress
                cipherInputStream.mark(Integer.MAX_VALUE);
                assertThat("Mismatch at position: " + i, (byte) byteRead, Matchers.is(ciphertextBytes[i]));
            }
        }
    }

    public void testMarkAndResetStepInRewindBuffer() throws Exception {
        int length = 2 * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES;
        int startIndex = randomIntBetween(0, testPlaintextArray.length - length);
        int nonce = Randomness.get().nextInt();
        byte[] ciphertextBytes;
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, nonce, bcFipsProvider)) {
            ciphertextBytes = cipherInputStream.readAllBytes();
        }
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, nonce, bcFipsProvider)) {
            int position1 = randomIntBetween(1, ENCRYPTED_PACKET_SIZE_IN_BYTES - 2);
            int position2 = randomIntBetween(position1 + 1, ENCRYPTED_PACKET_SIZE_IN_BYTES - 1);
            int position3 = ENCRYPTED_PACKET_SIZE_IN_BYTES;
            int position4 = randomIntBetween(position3 + 1, 2 * ENCRYPTED_PACKET_SIZE_IN_BYTES - 2);
            int position5 = randomIntBetween(position4 + 1, 2 * ENCRYPTED_PACKET_SIZE_IN_BYTES - 1);
            int position6 = 2 * ENCRYPTED_PACKET_SIZE_IN_BYTES;
            int position7 = Math.toIntExact(GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length));
            // skip position1 bytes
            cipherInputStream.readNBytes(position1);
            // mark position1
            cipherInputStream.mark(Integer.MAX_VALUE);
            byte[] bytesPos17;
            if (randomBoolean()) {
                bytesPos17 = cipherInputStream.readAllBytes();
            } else {
                bytesPos17 = cipherInputStream.readNBytes(position7 - position1);
            }
            // reset back to position 1
            cipherInputStream.reset();
            byte[] bytesPos12 = cipherInputStream.readNBytes(position2 - position1);
            assertTrue(Arrays.equals(bytesPos12, 0, bytesPos12.length, bytesPos17, 0, bytesPos12.length));
            // mark position2
            cipherInputStream.mark(Integer.MAX_VALUE);
            byte[] bytesPos26 = cipherInputStream.readNBytes(position6 - position2);
            assertTrue(Arrays.equals(bytesPos26, 0, bytesPos26.length, bytesPos17, (position2 - position1),
                    (position2 - position1) + bytesPos26.length));
            // reset to position 2
            cipherInputStream.reset();
            byte[] bytesPos23 = cipherInputStream.readNBytes(position3 - position2);
            assertTrue(Arrays.equals(bytesPos23, 0, bytesPos23.length, bytesPos17, (position2 - position1),
                    (position2 - position1) + bytesPos23.length));
            // mark position3
            cipherInputStream.mark(Integer.MAX_VALUE);
            byte[] bytesPos36 = cipherInputStream.readNBytes(position6 - position3);
            assertTrue(Arrays.equals(bytesPos36, 0, bytesPos36.length, bytesPos17, (position3 - position1),
                    (position3 - position1) + bytesPos36.length));
            // reset to position 3
            cipherInputStream.reset();
            byte[] bytesPos34 = cipherInputStream.readNBytes(position4 - position3);
            assertTrue(Arrays.equals(bytesPos34, 0, bytesPos34.length, bytesPos17, (position3 - position1),
                    (position3 - position1) + bytesPos34.length));
            // mark position4
            cipherInputStream.mark(Integer.MAX_VALUE);
            byte[] bytesPos46 = cipherInputStream.readNBytes(position6 - position4);
            assertTrue(Arrays.equals(bytesPos46, 0, bytesPos46.length, bytesPos17, (position4 - position1),
                    (position4 - position1) + bytesPos46.length));
            // reset to position 4
            cipherInputStream.reset();
            byte[] bytesPos45 = cipherInputStream.readNBytes(position5 - position4);
            assertTrue(Arrays.equals(bytesPos45, 0, bytesPos45.length, bytesPos17, (position4 - position1),
                    (position4 - position1) + bytesPos45.length));
            // mark position 5
            cipherInputStream.mark(Integer.MAX_VALUE);
            byte[] bytesPos56 = cipherInputStream.readNBytes(position6 - position5);
            assertTrue(Arrays.equals(bytesPos56, 0, bytesPos56.length, bytesPos17, (position5 - position1),
                    (position5 - position1) + bytesPos56.length));
            // mark position 6
            cipherInputStream.mark(Integer.MAX_VALUE);
            byte[] bytesPos67;
            if (randomBoolean()) {
                bytesPos67 = cipherInputStream.readAllBytes();
            } else {
                bytesPos67 = cipherInputStream.readNBytes(position7 - position6);
            }
            assertTrue(Arrays.equals(bytesPos67, 0, bytesPos67.length, bytesPos17, (position6 - position1),
                    (position6 - position1) + bytesPos67.length));
            // mark position 7 (end of stream)
            cipherInputStream.mark(Integer.MAX_VALUE);
            // end of stream
            assertThat(cipherInputStream.read(), Matchers.is(-1));
            // reset at the end
            cipherInputStream.reset();
            assertThat(cipherInputStream.read(), Matchers.is(-1));
        }
    }

    public void testDecryptionFails() throws Exception {
        Random random = Randomness.get();
        int length = randomIntBetween(0, READ_BUFFER_SIZE_IN_BYTES);
        int startIndex = randomIntBetween(0, testPlaintextArray.length - length);
        int nonce = random.nextInt();
        byte[] ciphertextBytes;
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, nonce, bcFipsProvider)) {
            ciphertextBytes = cipherInputStream.readAllBytes();
        }
        // decryption fails for one byte modifications
        for (int i = 0; i < ciphertextBytes.length; i++) {
            byte bytei = ciphertextBytes[i];
            while (bytei == ciphertextBytes[i]) {
                ciphertextBytes[i] = (byte) random.nextInt();
            }
            try (InputStream plainInputStream =
                         GCMPacketsCipherInputStream.getDecryptor(new ByteArrayInputStream(ciphertextBytes),
                                 secretKey, nonce, bcFipsProvider)) {
                IOException e = expectThrows(IOException.class, () -> {
                    readAllInputStream(plainInputStream,
                            GCMPacketsCipherInputStream.getDecryptionSizeFromCipherSize(ciphertextBytes.length));
                });
                assertThat(e.getCause(), Matchers.isA(AEADBadTagException.class));
            }
            ciphertextBytes[i] = bytei;
        }
        // decryption fails for one byte omissions
        byte[] missingByteCiphertext = new byte[ciphertextBytes.length - 1];
        for (int i = 0; i < ciphertextBytes.length; i++) {
            System.arraycopy(ciphertextBytes, 0, missingByteCiphertext, 0, i);
            System.arraycopy(ciphertextBytes, i + 1, missingByteCiphertext, i, (ciphertextBytes.length - i - 1));
            try (InputStream plainInputStream =
                         GCMPacketsCipherInputStream.getDecryptor(new ByteArrayInputStream(missingByteCiphertext),
                                 secretKey, nonce, bcFipsProvider)) {
                IOException e = expectThrows(IOException.class, () -> {
                    readAllInputStream(plainInputStream,
                            GCMPacketsCipherInputStream.getDecryptionSizeFromCipherSize(missingByteCiphertext.length));
                });
                assertThat(e.getCause(), Matchers.isA(AEADBadTagException.class));
            }
        }
        // decryption fails for one extra byte
        byte[] extraByteCiphertext = new byte[ciphertextBytes.length + 1];
        for (int i = 0; i < ciphertextBytes.length; i++) {
            System.arraycopy(ciphertextBytes, 0, extraByteCiphertext, 0, i);
            extraByteCiphertext[i] = (byte) random.nextInt();
            System.arraycopy(ciphertextBytes, i, extraByteCiphertext, i + 1, (ciphertextBytes.length - i));
            try (InputStream plainInputStream =
                         GCMPacketsCipherInputStream.getDecryptor(new ByteArrayInputStream(extraByteCiphertext),
                                 secretKey, nonce, bcFipsProvider)) {
                IOException e = expectThrows(IOException.class, () -> {
                    readAllInputStream(plainInputStream,
                            GCMPacketsCipherInputStream.getDecryptionSizeFromCipherSize(extraByteCiphertext.length));
                });
                assertThat(e.getCause(), Matchers.isA(AEADBadTagException.class));
            }
        }
    }

    private void testMarkAndResetToSameOffsetForEncryption(int offset) throws Exception {
        int length = 4 * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES + randomIntBetween(0,
                GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES);
        int startIndex = randomIntBetween(0, testPlaintextArray.length - length);
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, Randomness.get().nextInt(), bcFipsProvider)) {
            // skip offset bytes
            cipherInputStream.readNBytes(offset);
            // mark after offset
            cipherInputStream.mark(Integer.MAX_VALUE);
            // read/skip less than (encrypted) packet size
            int skipSize = randomIntBetween(1, ENCRYPTED_PACKET_SIZE_IN_BYTES - 1);
            byte[] firstPassEncryption = cipherInputStream.readNBytes(skipSize);
            // back to start
            cipherInputStream.reset();
            // read/skip more than (encrypted) packet size, but less than the full stream
            skipSize = randomIntBetween(ENCRYPTED_PACKET_SIZE_IN_BYTES,
                    Math.toIntExact(GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize((long)length)) - 1 - offset);
            byte[] secondPassEncryption = cipherInputStream.readNBytes(skipSize);
            assertTrue(Arrays.equals(firstPassEncryption, 0, firstPassEncryption.length, secondPassEncryption, 0,
                    firstPassEncryption.length));
            // back to start
            cipherInputStream.reset();
            byte[] thirdPassEncryption;
            // read/skip to end of ciphertext
            if (randomBoolean()) {
                thirdPassEncryption = cipherInputStream.readAllBytes();
            } else {
                thirdPassEncryption = cipherInputStream.readNBytes(
                                Math.toIntExact(GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize((long) length)) - offset);
            }
            assertTrue(Arrays.equals(secondPassEncryption, 0, secondPassEncryption.length, thirdPassEncryption, 0,
                    secondPassEncryption.length));
            // back to start
            cipherInputStream.reset();
            // read/skip more than (encrypted) packet size, but less than the full stream
            skipSize = randomIntBetween(ENCRYPTED_PACKET_SIZE_IN_BYTES,
                    Math.toIntExact(GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize((long)length)) - 1 - offset);
            byte[] fourthPassEncryption = cipherInputStream.readNBytes(skipSize);
            assertTrue(Arrays.equals(fourthPassEncryption, 0, fourthPassEncryption.length, thirdPassEncryption, 0,
                    fourthPassEncryption.length));
            // back to start
            cipherInputStream.reset();
            // read/skip less than (encrypted) packet size
            skipSize = randomIntBetween(1, ENCRYPTED_PACKET_SIZE_IN_BYTES - 1);
            byte[] fifthsPassEncryption = cipherInputStream.readNBytes(skipSize);
            assertTrue(Arrays.equals(fifthsPassEncryption, 0, fifthsPassEncryption.length, fourthPassEncryption, 0,
                    fifthsPassEncryption.length));
        }
    }

    private void testEncryptDecryptRandomOfLength(int length, SecretKey secretKey) throws Exception {
        int nonce = Randomness.get().nextInt();
        ByteArrayOutputStream cipherTextOutput;
        ByteArrayOutputStream plainTextOutput;
        int startIndex = randomIntBetween(0, testPlaintextArray.length - length);
        // encrypt
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, nonce, bcFipsProvider)) {
            cipherTextOutput = readAllInputStream(cipherInputStream, GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length));
        }
        //decrypt
        try (InputStream plainInputStream =
                     GCMPacketsCipherInputStream.getDecryptor(new ByteArrayInputStream(cipherTextOutput.toByteArray()),
                             secretKey, nonce, bcFipsProvider)) {
            plainTextOutput = readAllInputStream(plainInputStream,
                    GCMPacketsCipherInputStream.getDecryptionSizeFromCipherSize(cipherTextOutput.size()));
        }
        assertTrue(Arrays.equals(plainTextOutput.toByteArray(), 0, length, testPlaintextArray, startIndex, startIndex + length));
    }

    private SecretKey generateSecretKey() throws Exception {
        return AccessController.doPrivileged((PrivilegedAction<SecretKey>) () -> {
            try {
                KeyGenerator keyGen = KeyGenerator.getInstance("AES", bcFipsProvider);
                keyGen.init(256, SecureRandom.getInstance("DEFAULT", bcFipsProvider));
                return keyGen.generateKey();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    // read "adversarily" in small random pieces
    private ByteArrayOutputStream readAllInputStream(InputStream inputStream, long size) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(Math.toIntExact(size));
        byte[] temp = new byte[randomIntBetween(1, size != 0 ? Math.toIntExact(size) : 1)];
        do {
            int bytesRead = inputStream.read(temp, 0, randomIntBetween(1, temp.length));
            if (bytesRead == -1) {
                break;
            }
            baos.write(temp, 0, bytesRead);
            if (randomBoolean()) {
                int singleByte = inputStream.read();
                if (singleByte == -1) {
                    break;
                }
                baos.write(singleByte);
            }
        } while (true);
        assertThat(baos.size(), Matchers.is(Math.toIntExact(size)));
        return baos;
    }
}
