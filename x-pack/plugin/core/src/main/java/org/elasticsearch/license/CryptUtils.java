/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.license;

import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;

public class CryptUtils {
    // SALT must be at least 128bits for FIPS 140-2 compliance
    private static final byte[] SALT = {
        (byte) 0x74,
        (byte) 0x68,
        (byte) 0x69,
        (byte) 0x73,
        (byte) 0x69,
        (byte) 0x73,
        (byte) 0x74,
        (byte) 0x68,
        (byte) 0x65,
        (byte) 0x73,
        (byte) 0x61,
        (byte) 0x6C,
        (byte) 0x74,
        (byte) 0x77,
        (byte) 0x65,
        (byte) 0x75 };
    private static final String KEY_ALGORITHM = "RSA";
    private static final char[] DEFAULT_PASS_PHRASE = "elasticsearch-license".toCharArray();
    private static final String KDF_ALGORITHM = "PBKDF2WithHmacSHA512";
    private static final int KDF_ITERATION_COUNT = 10000;
    private static final String CIPHER_ALGORITHM = "AES";
    // This can be changed to 256 once Java 9 is the minimum version
    // http://www.oracle.com/technetwork/java/javase/terms/readme/jdk9-readme-3852447.html#jce
    private static final int ENCRYPTION_KEY_LENGTH = 128;
    private static final SecureRandom RANDOM = new SecureRandom();

    /**
     * Read encrypted private key file content with default pass phrase
     */
    public static PrivateKey readEncryptedPrivateKey(byte[] fileContents) {
        return readEncryptedPrivateKey(fileContents, DEFAULT_PASS_PHRASE, false);
    }

    /**
     * Returns encrypted private key file content with default pass phrase
     */
    public static byte[] writeEncryptedPrivateKey(PrivateKey privateKey) {
        return writeEncryptedPrivateKey(privateKey, DEFAULT_PASS_PHRASE);
    }

    /**
     * Read encrypted private key file content with provided <code>passPhrase</code>
     */
    public static PrivateKey readEncryptedPrivateKey(byte[] fileContents, char[] passPhrase, boolean preV4) {
        byte[] keyBytes = preV4 ? decryptV3Format(fileContents) : decrypt(fileContents, passPhrase);
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(keyBytes);
        try {
            return KeyFactory.getInstance(KEY_ALGORITHM).generatePrivate(privateKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Read public key file content
     */
    public static PublicKey readPublicKey(byte[] fileContents) {
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(fileContents);
        try {
            return KeyFactory.getInstance(CryptUtils.KEY_ALGORITHM).generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns encrypted public key file content with provided <code>passPhrase</code>
     */
    public static byte[] writeEncryptedPublicKey(PublicKey publicKey) {
        X509EncodedKeySpec encodedKeySpec = new X509EncodedKeySpec(publicKey.getEncoded());
        return encrypt(encodedKeySpec.getEncoded(), DEFAULT_PASS_PHRASE);
    }

    /**
     * Returns encrypted private key file content with provided <code>passPhrase</code>
     */
    public static byte[] writeEncryptedPrivateKey(PrivateKey privateKey, char[] passPhrase) {
        PKCS8EncodedKeySpec encodedKeySpec = new PKCS8EncodedKeySpec(privateKey.getEncoded());
        return encrypt(encodedKeySpec.getEncoded(), passPhrase);
    }

    /**
     * Encrypts provided <code>data</code> with <code>DEFAULT_PASS_PHRASE</code>
     */
    static byte[] encrypt(byte[] data) {
        return encrypt(data, DEFAULT_PASS_PHRASE);
    }

    /**
     * Decrypts provided <code>encryptedData</code> with <code>DEFAULT_PASS_PHRASE</code>
     */
    static byte[] decrypt(byte[] encryptedData) {
        return decrypt(encryptedData, DEFAULT_PASS_PHRASE);
    }

    /**
     * Encrypts provided <code>data</code> with <code>passPhrase</code>
     */
    private static byte[] encrypt(byte[] data, char[] passPhrase) {
        try {
            final Cipher encryptionCipher = getEncryptionCipher(deriveSecretKey(passPhrase));
            return encryptionCipher.doFinal(data);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Decrypts provided <code>encryptedData</code> with <code>passPhrase</code>
     */
    private static byte[] decrypt(byte[] encryptedData, char[] passPhrase) {
        try {
            final Cipher cipher = getDecryptionCipher(deriveSecretKey(passPhrase));
            return cipher.doFinal(encryptedData);
        } catch (IllegalBlockSizeException | BadPaddingException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] encryptV3Format(byte[] data) {
        try {
            SecretKey encryptionKey = getV3Key();
            final Cipher encryptionCipher = getEncryptionCipher(encryptionKey);
            return encryptionCipher.doFinal(pad(data, 20));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    static byte[] decryptV3Format(byte[] data) {
        try {
            SecretKey decryptionKey = getV3Key();
            final Cipher decryptionCipher = getDecryptionCipher(decryptionKey);
            return unPad(decryptionCipher.doFinal(data));
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }

    private static SecretKey getV3Key() throws NoSuchAlgorithmException, InvalidKeySpecException {
        final byte[] salt = { (byte) 0xA9, (byte) 0xA2, (byte) 0xB5, (byte) 0xDE, (byte) 0x2A, (byte) 0x8A, (byte) 0x9A, (byte) 0xE6 };
        final byte[] passBytes = "elasticsearch-license".getBytes(StandardCharsets.UTF_8);
        final byte[] digest = MessageDigest.getInstance("SHA-512").digest(passBytes);
        final char[] hashedPassphrase = Base64.getEncoder().encodeToString(digest).toCharArray();
        PBEKeySpec keySpec = new PBEKeySpec(hashedPassphrase, salt, 1024, 128);
        byte[] shortKey = SecretKeyFactory.getInstance("PBEWithSHA1AndDESede").generateSecret(keySpec).getEncoded();
        byte[] intermediaryKey = new byte[16];
        for (int i = 0, j = 0; i < 16; i++) {
            intermediaryKey[i] = shortKey[j];
            if (++j == shortKey.length) j = 0;
        }
        return new SecretKeySpec(intermediaryKey, "AES");
    }

    private static SecretKey deriveSecretKey(char[] passPhrase) {
        try {
            PBEKeySpec keySpec = new PBEKeySpec(passPhrase, SALT, KDF_ITERATION_COUNT, ENCRYPTION_KEY_LENGTH);

            SecretKey secretKey = SecretKeyFactory.getInstance(KDF_ALGORITHM).generateSecret(keySpec);
            return new SecretKeySpec(secretKey.getEncoded(), CIPHER_ALGORITHM);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Cipher getEncryptionCipher(SecretKey secretKey) {
        return getCipher(Cipher.ENCRYPT_MODE, secretKey);
    }

    private static Cipher getDecryptionCipher(SecretKey secretKey) {
        return getCipher(Cipher.DECRYPT_MODE, secretKey);
    }

    private static Cipher getCipher(int mode, SecretKey secretKey) {
        try {
            Cipher cipher = Cipher.getInstance(CIPHER_ALGORITHM);
            cipher.init(mode, secretKey, RANDOM);
            return cipher;
        } catch (NoSuchAlgorithmException | InvalidKeyException | NoSuchPaddingException e) {
            throw new IllegalStateException(e);
        }
    }

    private static byte[] pad(byte[] bytes, int length) {
        if (bytes.length >= length) {
            byte[] out = new byte[bytes.length + 1];
            System.arraycopy(bytes, 0, out, 0, bytes.length);
            out[bytes.length] = (byte) 1;
            return out;
        }

        byte[] out = new byte[length + 1];

        int i = 0;
        for (; i < bytes.length; i++)
            out[i] = bytes[i];

        int padded = length - i;

        // fill the rest with random bytes
        byte[] fill = new byte[padded - 1];
        RANDOM.nextBytes(fill);
        System.arraycopy(fill, 0, out, i, padded - 1);

        out[length] = (byte) (padded + 1);

        return out;
    }

    private static byte[] unPad(byte[] bytes) {
        int padded = (int) bytes[bytes.length - 1];
        int targetLength = bytes.length - padded;

        byte[] out = new byte[targetLength];

        System.arraycopy(bytes, 0, out, 0, targetLength);

        return out;
    }
}
