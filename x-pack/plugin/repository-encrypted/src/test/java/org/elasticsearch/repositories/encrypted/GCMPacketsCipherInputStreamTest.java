package org.elasticsearch.repositories.encrypted;

import org.bouncycastle.jcajce.provider.BouncyCastleFipsProvider;
import org.hamcrest.Matchers;
import org.junit.Assert;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.util.Random;

public class GCMPacketsCipherInputStreamTest {

    private static final BouncyCastleFipsProvider BC_FIPS_PROV = new BouncyCastleFipsProvider();

    public void testBasicEncryptDecrypt() throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES", BC_FIPS_PROV);
        keyGen.init(256);
        SecretKey secretKey = keyGen.generateKey();

        for (int i = 0; i < 32; i++) {
            testEncryptDecryptRandomOfLength(i, secretKey);
        }
    }

    private void testEncryptDecryptRandomOfLength(int length, SecretKey secretKey) throws Exception {
        Random random = new Random();
        byte[] temp = new byte[length];
        byte[] plaintextArray = new byte[length];
        random.nextBytes(plaintextArray);
        ByteArrayOutputStream cipherTextOutput = new ByteArrayOutputStream();
        ByteArrayOutputStream plainTextOutput = new ByteArrayOutputStream();
        // encrypt
        try (InputStream cipherInputStream = GCMPacketsCipherInputStream.getGCMPacketsEncryptor(new ByteArrayInputStream(plaintextArray),
                BC_FIPS_PROV, secretKey)) {
            do {
                int bytesEncrypted = cipherInputStream.read(temp, 0, random.nextInt(temp.length));
                if (bytesEncrypted == -1) {
                    break;
                }
                cipherTextOutput.write(temp, 0, bytesEncrypted);
            } while (true);
        }
        //decrypt
        try (InputStream plainInputStream =
                     GCMPacketsCipherInputStream.getGCMPacketsDecryptor(new ByteArrayInputStream(cipherTextOutput.toByteArray()),
                             BC_FIPS_PROV, secretKey)) {
            do {
                int bytesDecrypted = plainInputStream.read(temp, 0, random.nextInt(temp.length));
                if (bytesDecrypted == -1) {
                    break;
                }
                plainTextOutput.write(temp, 0, bytesDecrypted);
            } while (true);
        }
        Assert.assertThat(plainTextOutput.toByteArray(), Matchers.is(plaintextArray));
    }
}