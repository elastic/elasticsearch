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

    private static int TEST_ARRAY_SIZE = 5 * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES;
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
        for (int i = GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES + 1; i < GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES * 3; i++) {
            testEncryptDecryptRandomOfLength(i, secretKey);
        }
    }

    public void testEncryptDecryptMultipleOfPacketSize() throws Exception {
        for (int i = 1; i <= 5; i++) {
            testEncryptDecryptRandomOfLength(i * GCMPacketsCipherInputStream.PACKET_SIZE_IN_BYTES, secretKey);
        }
    }

    private void testEncryptDecryptRandomOfLength(int length, SecretKey secretKey) throws Exception {
        Random random = new Random();
        int nonce = random.nextInt();
        ByteArrayOutputStream cipherTextOutput;
        ByteArrayOutputStream plainTextOutput;
        int startIndex =  randomIntBetween(0, testPlaintextArray.length - length);
        // encrypt
        try (InputStream cipherInputStream =
                     GCMPacketsCipherInputStream.getGCMPacketsEncryptor(new ByteArrayInputStream(testPlaintextArray, startIndex, length),
                             secretKey, nonce, bcFipsProvider)) {
            cipherTextOutput = readAllInputStream(cipherInputStream, GCMPacketsCipherInputStream.getEncryptionSizeFromPlainSize(length));
        }
        //decrypt
        try (InputStream plainInputStream =
                     GCMPacketsCipherInputStream.getGCMPacketsDecryptor(new ByteArrayInputStream(cipherTextOutput.toByteArray()),
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
