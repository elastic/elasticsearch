/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public final class PasswordBasedEncryptor {

    // the count of keys stored so as to avoid re-computation
    static final int ENCRYPTION_KEY_CACHE_SIZE = 512;
    // The cipher used to encrypt the data
    static final String CIPHER_ALGO = "AES";
    // The mode used with the cipher algorithm
    private static final String CIPHER_MODE = "GCM";
    // The padding used with the cipher algorithm
    private static final String CIPHER_PADDING = "NoPadding";
    // the KDF algorithm that generate the symmetric key given the password
    private static final String KDF_ALGO = "PBKDF2WithHmacSHA512";
    // parameter for the KDF function, it's a funny and unusual iter count larger than 60k
    private static final int KDF_ITER = 61616;
    // the salt, which is generated randomly, is another parameter for the KDF function
    private static final int SALT_LENGTH_IN_BYTES = 32;
    // the key encryption key is 256 bit wide (AES256)
    private static final int KEY_SIZE_IN_BITS = 256;
    // the GCM cipher mode uses a 12 byte wide IV
    private static final int IV_LENGTH_IN_BYTES = 12;
    // the GCM cipher mode generates a 16 byte wide authentication tag
    private static final int TAG_LENGTH_IN_BYTES = 16;
    // according to NIST SP 800-38D https://nvlpubs.nist.gov/nistpubs/Legacy/SP/nistspecialpublication800-38d.pdf
    // the number of encrypt operations using the same key, when the IV is generated randomly, should be limited
    // to less than 2^32 (so we use 2^31 to err on the safe side)
    private static final long ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY = 1L<<31;

    // the password which is used to generate all the encryption keys (the keys are different because they each
    // are generated using a different salt, which is generated randomly)
    private final char[] password;
    // this is used to generate the IVs for each encryption instance as well as the salt for every key generation
    private final SecureRandom secureRandom;
    private final Cache<String, Tuple<byte[], SecretKey>> keyBySaltCache;
    private final AtomicReference<LimitedSupplier<String>> currentEncryptionKeySalt;

    public PasswordBasedEncryptor(char[] password, SecureRandom secureRandom) {
        this.password = password;
        this.secureRandom = secureRandom;
        this.keyBySaltCache = CacheBuilder.<String, Tuple<byte[], SecretKey>>builder()
                .setMaximumWeight(ENCRYPTION_KEY_CACHE_SIZE)
                .build();
        byte[] randomEncryptionKeySaltBytes = new byte[SALT_LENGTH_IN_BYTES];
        secureRandom.nextBytes(randomEncryptionKeySaltBytes);
        this.currentEncryptionKeySalt = new AtomicReference<>(new LimitedSupplier<>(
                Base64.getEncoder().encodeToString(randomEncryptionKeySaltBytes),
                ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY));
    }

    public byte[] encrypt(byte[] data) throws NoSuchPaddingException, NoSuchAlgorithmException, BadPaddingException,
            IllegalBlockSizeException, ExecutionException, InvalidAlgorithmParameterException, InvalidKeyException {
        Objects.requireNonNull(data);
        // retrieve the encryption key
        Tuple<byte[], SecretKey> saltAndEncryptionKey = useEncryptionKey();
        // create the IV randomly
        byte[] iv = new byte[IV_LENGTH_IN_BYTES];
        secureRandom.nextBytes(iv);
        // create cipher for metadata encryption
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BYTES * Byte.SIZE, iv);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGO + "/" + CIPHER_MODE + "/" + CIPHER_PADDING);
        cipher.init(Cipher.ENCRYPT_MODE, saltAndEncryptionKey.v2(), gcmParameterSpec);
        // encrypt
        byte[] encryptedData = cipher.doFinal(data);
        // concatenate key salt, iv and metadata cipher text
        byte[] resultCiphertext = new byte[saltAndEncryptionKey.v1().length + iv.length + encryptedData.length];
        // prepend salt
        System.arraycopy(saltAndEncryptionKey.v1(), 0, resultCiphertext, 0, saltAndEncryptionKey.v1().length);
        System.arraycopy(iv, 0, resultCiphertext, saltAndEncryptionKey.v1().length, iv.length);
        System.arraycopy(encryptedData, 0, resultCiphertext, saltAndEncryptionKey.v1().length + iv.length, encryptedData.length);
        return resultCiphertext;
    }

    public byte[] decrypt(byte[] encryptedData) throws ExecutionException, NoSuchPaddingException, NoSuchAlgorithmException,
            InvalidAlgorithmParameterException, InvalidKeyException, BadPaddingException, IllegalBlockSizeException {
        if (Objects.requireNonNull(encryptedData).length < SALT_LENGTH_IN_BYTES + IV_LENGTH_IN_BYTES + TAG_LENGTH_IN_BYTES) {
            throw new IllegalArgumentException("Ciphertext too short");
        }
        // extract the salt prepended to the ciphertext
        byte[] salt = Arrays.copyOf(encryptedData, SALT_LENGTH_IN_BYTES);
        // get the key associated with the salt
        SecretKey decryptionKey = getKeyFromSalt(Base64.getEncoder().encodeToString(salt)).v2();
        // construct and initialize the decryption cipher
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BYTES * Byte.SIZE, encryptedData, SALT_LENGTH_IN_BYTES,
                IV_LENGTH_IN_BYTES);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGO + "/" + CIPHER_MODE + "/" + CIPHER_PADDING);
        cipher.init(Cipher.DECRYPT_MODE, decryptionKey, gcmParameterSpec);
        // decrypt metadata (use cipher)
        return cipher.doFinal(encryptedData, SALT_LENGTH_IN_BYTES + IV_LENGTH_IN_BYTES,
                encryptedData.length - SALT_LENGTH_IN_BYTES - IV_LENGTH_IN_BYTES);
    }

    private SecretKey generatePasswordBasedSecretKey(char[] password, byte[] salt) throws NoSuchAlgorithmException,
            InvalidKeySpecException {
        PBEKeySpec keySpec = new PBEKeySpec(password, salt, KDF_ITER, KEY_SIZE_IN_BITS);
        SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(KDF_ALGO);
        SecretKey secretKey = keyFactory.generateSecret(keySpec);
        SecretKeySpec secret = new SecretKeySpec(secretKey.getEncoded(), CIPHER_ALGO);
        return secret;
    }

    private Tuple<byte[], SecretKey> getKeyFromSalt(String salt) throws ExecutionException {
        return this.keyBySaltCache.computeIfAbsent(salt, (ignore) -> {
            byte[] saltBytes = Base64.getDecoder().decode(salt);
            SecretKey secretKey = generatePasswordBasedSecretKey(password, saltBytes);
            return new Tuple<>(saltBytes, secretKey);
        });
    }

    private void resetCurrentEncryptionKeySalt() {
        byte[] randomEncryptionKeySaltBytes = new byte[SALT_LENGTH_IN_BYTES];
        secureRandom.nextBytes(randomEncryptionKeySaltBytes);
        this.currentEncryptionKeySalt.set(new LimitedSupplier<>(
                Base64.getEncoder().encodeToString(randomEncryptionKeySaltBytes),
                ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY));
    }

    private Tuple<byte[], SecretKey> useEncryptionKey() throws ExecutionException {
        Optional<String> encryptionKeySalt = this.currentEncryptionKeySalt.get().get();
        if (encryptionKeySalt.isPresent()) {
            return getKeyFromSalt(encryptionKeySalt.get());
        }
        // change the salt and generate a new encryption key
        resetCurrentEncryptionKeySalt();
        // try again
        return useEncryptionKey();
    }

    static class LimitedSupplier<T> implements Supplier<Optional<T>> {

        private final AtomicLong count;

        private final Optional<T> value;

        private final long limit;
        LimitedSupplier(T value, long limit) {
            this.count = new AtomicLong(0L);
            this.value = Optional.of(Objects.requireNonNull(value));
            this.limit = limit;
        }

        @Override
        public Optional<T> get() {
            if (count.get() <= limit && count.incrementAndGet() <= limit) {
                return value;
            }
            return Optional.empty();
        }

    }

}
