package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;

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

public class GCMPacketsCipherInputStreamTests extends ESTestCase {

    private static int TEST_ARRAY_SIZE = 8 * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES;
    private static int ENCRYPTED_PACKET_SIZE = GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES);
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
        new Random().nextBytes(testPlaintextArray);
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
        testMarkAndResetToSameOffsetForEncryption(GCMPacketsCipherInputStream.
                getEncryptionSizeFromPlainSize(GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES));
    }

    public void testMarkAndResetFirstPacketForEncryption() throws Exception {
        for (int i = 1; i < ENCRYPTED_PACKET_SIZE; i++) {
            testMarkAndResetToSameOffsetForEncryption(i);
        }
    }

    public void testMarkAndResetRandomSecondPacketForEncryption() throws Exception {
        for (int i = ENCRYPTED_PACKET_SIZE + 1; i < 2 * ENCRYPTED_PACKET_SIZE; i++) {
            testMarkAndResetToSameOffsetForEncryption(i);
        }
    }

    public void testMarkAndResetCrawlForEncryption() throws Exception {
        int length = GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES;
        int startIndex = randomIntBetween(0, testPlaintextArray.length - length);
        int nonce = new Random().nextInt();
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
                int skipSize = randomIntBetween(1, GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length) - i);
                cipherInputStream.readNBytes(skipSize);
                cipherInputStream.reset();
                int byteRead = cipherInputStream.read();
                cipherInputStream.mark(Integer.MAX_VALUE);
                assertThat("Mismatch at position: " + i, (byte) byteRead, Matchers.is(ciphertextBytes[i]));
            }
        }
    }

    private void testMarkAndResetToSameOffsetForEncryption(int offset) throws Exception {
        int length = 4 * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES + randomIntBetween(0,
                GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES);
        int startIndex = randomIntBetween(0, testPlaintextArray.length - length);
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, new Random().nextInt(), bcFipsProvider)) {
            // skip offset bytes
            cipherInputStream.readNBytes(offset);
            // mark after offset
            cipherInputStream.mark(Integer.MAX_VALUE);
            // read/skip less than (encrypted) packet size
            int skipSize = randomIntBetween(1, ENCRYPTED_PACKET_SIZE - 1);
            byte[] firstPassEncryption = cipherInputStream.readNBytes(skipSize);
            // back to start
            cipherInputStream.reset();
            // read/skip more than (encrypted) packet size, but less than the full stream
            skipSize = randomIntBetween(ENCRYPTED_PACKET_SIZE,
                    GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length) - 1 - offset);
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
                thirdPassEncryption =
                        cipherInputStream.readNBytes(GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length) - offset);
            }
            assertTrue(Arrays.equals(secondPassEncryption, 0, secondPassEncryption.length, thirdPassEncryption, 0,
                    secondPassEncryption.length));
            // back to start
            cipherInputStream.reset();
            // read/skip more than (encrypted) packet size, but less than the full stream
            skipSize = randomIntBetween(ENCRYPTED_PACKET_SIZE,
                    GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length) - 1 - offset);
            byte[] fourthPassEncryption = cipherInputStream.readNBytes(skipSize);
            assertTrue(Arrays.equals(fourthPassEncryption, 0, fourthPassEncryption.length, thirdPassEncryption, 0,
                    fourthPassEncryption.length));
            // back to start
            cipherInputStream.reset();
            // read/skip less than (encrypted) packet size
            skipSize = randomIntBetween(1, ENCRYPTED_PACKET_SIZE - 1);
            byte[] fifthsPassEncryption = cipherInputStream.readNBytes(skipSize);
            assertTrue(Arrays.equals(fifthsPassEncryption, 0, fifthsPassEncryption.length, fourthPassEncryption, 0,
                    fifthsPassEncryption.length));
        }
    }

    private void testEncryptDecryptRandomOfLength(int length, SecretKey secretKey) throws Exception {
        Random random = new Random();
        int nonce = random.nextInt();
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
    private ByteArrayOutputStream readAllInputStream(InputStream inputStream, int size) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(size);
        byte[] temp = new byte[randomIntBetween(1, size != 0 ? size : 1)];
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
        assertThat(baos.size(), Matchers.is(size));
        return baos;
    }
}
