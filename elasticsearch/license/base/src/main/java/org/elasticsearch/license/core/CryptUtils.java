/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.core;


import org.elasticsearch.common.Base64;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
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

public class CryptUtils {
    private static final int minimumPadding = 20;
    private static final byte[] salt = {
            (byte) 0xA9, (byte) 0xA2, (byte) 0xB5, (byte) 0xDE,
            (byte) 0x2A, (byte) 0x8A, (byte) 0x9A, (byte) 0xE6
    };
    private static final int iterationCount = 1024;
    private static final int aesKeyLength = 128;
    private static final String keyAlgorithm = "RSA";
    private static final String passHashAlgorithm = "SHA-512";
    private static final String DEFAULT_PASS_PHRASE = "elasticsearch-license";

    private static final SecureRandom random = new SecureRandom();

    /**
     * Read encrypted private key file content with default pass phrase
     */
    public static PrivateKey readEncryptedPrivateKey(byte[] fileContents) {
        try {
            return readEncryptedPrivateKey(fileContents, hashPassPhrase(DEFAULT_PASS_PHRASE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Read encrypted public key file content with default pass phrase
     */
    public static PublicKey readEncryptedPublicKey(byte[] fileContents) {
        try {
            return readEncryptedPublicKey(fileContents, hashPassPhrase(DEFAULT_PASS_PHRASE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns encrypted public key file content with default pass phrase
     */
    public static byte[] writeEncryptedPublicKey(PublicKey publicKey) {
        try {
            return writeEncryptedPublicKey(publicKey, hashPassPhrase(DEFAULT_PASS_PHRASE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns encrypted private key file content with default pass phrase
     */
    public static byte[] writeEncryptedPrivateKey(PrivateKey privateKey) {
        try {
            return writeEncryptedPrivateKey(privateKey, hashPassPhrase(DEFAULT_PASS_PHRASE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Read encrypted private key file content with provided <code>passPhrase</code>
     */
    public static PrivateKey readEncryptedPrivateKey(byte[] fileContents, char[] passPhrase) {
        PKCS8EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(decrypt(fileContents, passPhrase));
        try {
            return KeyFactory.getInstance(keyAlgorithm).generatePrivate(privateKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Read encrypted public key file content with provided <code>passPhrase</code>
     */
    public static PublicKey readEncryptedPublicKey(byte[] fileContents, char[] passPhrase) {
        X509EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(decrypt(fileContents, passPhrase));
        try {
            return KeyFactory.getInstance(CryptUtils.keyAlgorithm).generatePublic(publicKeySpec);
        } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Returns encrypted public key file content with provided <code>passPhrase</code>
     */
    public static byte[] writeEncryptedPublicKey(PublicKey publicKey, char[] passPhrase) {
        X509EncodedKeySpec encodedKeySpec = new X509EncodedKeySpec(publicKey.getEncoded());
        return encrypt(encodedKeySpec.getEncoded(), passPhrase);
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
    public static byte[] encrypt(byte[] data) {
        try {
            return encrypt(data, hashPassPhrase(DEFAULT_PASS_PHRASE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Decrypts provided <code>encryptedData</code> with <code>DEFAULT_PASS_PHRASE</code>
     */
    public static byte[] decrypt(byte[] encryptedData) {
        try {
            return decrypt(encryptedData, hashPassPhrase(DEFAULT_PASS_PHRASE));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Encrypts provided <code>data</code> with <code>passPhrase</code>
     */
    public static byte[] encrypt(byte[] data, char[] passPhrase) {
        try {
            final Cipher encryptionCipher = getEncryptionCipher(getSecretKey(passPhrase));
            return encryptionCipher.doFinal(pad(data, minimumPadding));
        } catch (InvalidKeySpecException | IllegalBlockSizeException | BadPaddingException e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * Decrypts provided <code>encryptedData</code> with <code>passPhrase</code>
     */
    private static byte[] decrypt(byte[] encryptedData, char[] passPhrase) {
        try {
            final Cipher cipher = getDecryptionCipher(getSecretKey(passPhrase));
            return unPad(cipher.doFinal(encryptedData));
        } catch (IllegalBlockSizeException | BadPaddingException | InvalidKeySpecException e) {
            throw new IllegalStateException(e);
        }

    }

    private static SecretKey getSecretKey(char[] passPhrase) throws InvalidKeySpecException {
        try {
            PBEKeySpec keySpec = new PBEKeySpec(passPhrase, salt, iterationCount, aesKeyLength);

            byte[] shortKey = SecretKeyFactory.getInstance("PBEWithSHA1AndDESede").
                    generateSecret(keySpec).getEncoded();

            byte[] intermediaryKey = new byte[aesKeyLength / 8];
            for (int i = 0, j = 0; i < aesKeyLength / 8; i++) {
                intermediaryKey[i] = shortKey[j];
                if (++j == shortKey.length)
                    j = 0;
            }

            return new SecretKeySpec(intermediaryKey, "AES");
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
            Cipher cipher = Cipher.getInstance(secretKey.getAlgorithm());
            cipher.init(mode, secretKey, random);
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
        random.nextBytes(fill);
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

    private static char[] hashPassPhrase(String passPhrase) throws NoSuchAlgorithmException {
        final byte[] passBytes = passPhrase.getBytes(StandardCharsets.UTF_8);
        final byte[] digest = MessageDigest.getInstance(passHashAlgorithm).digest(passBytes);
        return new String(Base64.encodeBytesToBytes(digest), StandardCharsets.UTF_8).toCharArray();
    }
}
