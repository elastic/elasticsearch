/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class Main {
    public static void main(String[] args) throws Exception {
        rsa2048();
    }

    public static void aes256() throws Exception {
        KeyGenerator keyGenerator = KeyGenerator.getInstance("AES");
        keyGenerator.init(256);
        SecretKey secretKey = keyGenerator.generateKey();
        byte[] data = new byte[1024*1024*50];

        Random r = new Random();
        r.nextBytes(data);

        Cipher c = Cipher.getInstance("AES/ECB/PKCS5Padding");
        c.init(Cipher.ENCRYPT_MODE, secretKey);
        byte[] encrypt = new byte[c.getOutputSize(data.length)];

        long ns = System.nanoTime();
        c.doFinal(ByteBuffer.wrap(data), ByteBuffer.wrap(encrypt));
        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ns) + "ms");

        c.init(Cipher.DECRYPT_MODE, secretKey);
        data = new byte[c.getOutputSize(encrypt.length)];

        ns = System.nanoTime();
        c.doFinal(ByteBuffer.wrap(encrypt), ByteBuffer.wrap(data));
        System.out.println(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ns) + "ms");
    }

    public static void rsa2048() throws Exception {
        long ns = System.nanoTime();
        KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA");
        keyGen.initialize(2048);
        KeyPair keypair = keyGen.genKeyPair();
        System.out.println("Generation: " + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - ns) + "ms");
        PrivateKey privateKey = keypair.getPrivate();
        PublicKey publicKey = keypair.getPublic();

        byte[] data = new byte[500];
        byte[] encrypt = new byte[500];

        Random r = new Random();
        r.nextBytes(data);

        ns = System.nanoTime();
        Cipher c = Cipher.getInstance("RSA");

        int w=0;
        //for (int b=0; b<1024*1024*5; b += 200) {
            c.init(Cipher.ENCRYPT_MODE, publicKey);
            w += c.doFinal(data, 0, 200, encrypt, w);
        //}
        System.out.println("Encryption: " + (System.nanoTime() - ns) + "ns");

    }
}
