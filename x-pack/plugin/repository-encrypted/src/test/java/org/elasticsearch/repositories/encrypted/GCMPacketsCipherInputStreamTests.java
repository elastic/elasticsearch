package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.SecureRandom;
import java.util.Random;

public class GCMPacketsCipherInputStreamTests extends ESTestCase {

    private BouncyCastleFipsProvider bcFipsProvider;

    @Before
    public void setup() {
        AccessController.doPrivileged((PrivilegedAction<Void>) () -> {
            this.bcFipsProvider = new BouncyCastleFipsProvider();
//            for (Object o : this.bcFipsProvider.keySet()) {
//                System.out.println(o);
//            }
            return null;
        });
    }

    public void testHardcoreBasicEncryptDecrypt() throws Exception {
        SecretKey secretKey = AccessController.doPrivileged((PrivilegedAction<SecretKey>) () -> {
            try {
                KeyGenerator keyGen = KeyGenerator.getInstance("AES", bcFipsProvider);
                keyGen.init(256, SecureRandom.getInstance("DEFAULT", bcFipsProvider));
                return keyGen.generateKey();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

        for (int i = 1; i < 4096 * 8; i++) {
            testEncryptDecryptRandomOfLength(i, secretKey);
        }
    }

    private void testEncryptDecryptRandomOfLength(int length, SecretKey secretKey) throws Exception {
        Random random = new Random();
        byte[] temp = new byte[length];
        byte[] plaintextArray = new byte[length];
        random.nextBytes(plaintextArray);
        int nonce = random.nextInt();
        ByteArrayOutputStream cipherTextOutput = new ByteArrayOutputStream();
        ByteArrayOutputStream plainTextOutput = new ByteArrayOutputStream();
        // encrypt
        try (InputStream cipherInputStream = GCMPacketsCipherInputStream.getGCMPacketsEncryptor(new ByteArrayInputStream(plaintextArray),
                secretKey, nonce, bcFipsProvider)) {
            do {
                int bytesEncrypted = cipherInputStream.read(temp, 0, randomIntBetween(1, temp.length));
                if (bytesEncrypted == -1) {
                    break;
                }
                cipherTextOutput.write(temp, 0, bytesEncrypted);
            } while (true);
        }
        //decrypt
        try (InputStream plainInputStream =
                     GCMPacketsCipherInputStream.getGCMPacketsDecryptor(new ByteArrayInputStream(cipherTextOutput.toByteArray()),
                             secretKey, nonce, bcFipsProvider)) {
            do {
                int bytesDecrypted = plainInputStream.read(temp, 0, randomIntBetween(1, temp.length));
                if (bytesDecrypted == -1) {
                    break;
                }
                plainTextOutput.write(temp, 0, bytesDecrypted);
            } while (true);
        }
        Assert.assertThat(plainTextOutput.toByteArray(), Matchers.is(plaintextArray));
    }
}