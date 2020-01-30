/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.repositories.encrypted;

import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.PBEKeySpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
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

/**
 * Encrypts and decrypts using a password. Decryption authenticates the cyphertext so as to make sure that
 * the same password has been used during encryption (the cipher mode is AES/GCM/NoPadding). The caller must
 * ensure that the password and the ciphertext are not stored on the same "medium" (storage partition).
 * <p>
 * The {@code password} constructor argument is used to generate AES 256-bit wide keys using the PBKDF2 algorithm.
 * The "salt", which is the other required parameter to the PBKDF2 algo, is generated randomly (32 byte-wide) using a
 * {@code SecureRandom} instance. The "salt" is not a secret, like the password is, and it is used to generate different
 * keys starting from the same password.
 * <p>
 * A new encryption key is generated for every {@link PasswordBasedEncryption} instance (using a newly generated random
 * "salt"). The key is then reused for as many as {@link #ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY} encryption invocations;
 * when the limit is exceeded, a new key is computed from a newly generated "salt". In order to support the decryption
 * operation, the "salt" is prepended to the returned ciphertext. Decryption reads-in the "salt" and uses the secret
 * password to regenerate the same key and decrypt and authenticate the ciphertext. The key thus computed is locally
 * cached for possible reuses because generating the key from the password is an expensive operation (by design).
 * <p>
 * The reason why there is an encryption invocation limit for the same key is because the AES/GCM/NoPadding encryption mode
 * must not be used with the same key and the same Initialization Vector. During encryption, the {@link PasswordBasedEncryption}
 * randomly generates a new 12-byte wide IV, and so in order to limit the risk of a collision, the key must be changed
 * after at most {@link #ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY} IVs have been generated and used with that same key. For more
 * details, see Section 8.2 of https://csrc.nist.gov/publications/detail/sp/800-38d/final .
 */
public final class PasswordBasedEncryption {

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

    // the password which is used to generate all the encryption and decryption keys (the keys are different because they each
    // are generated using a different salt, which is generated randomly)
    private final char[] password;
    // this is used to generate the IVs for each encryption instance as well as the salt for every key generation
    private final SecureRandom secureRandom;
    // this is used to store the secret keys given the salt that was used in generating it
    private final Cache<String, Tuple<byte[], SecretKey>> keyBySaltCache;
    // the salt of the secret key which is used for encryption
    private final AtomicReference<LimitedSupplier<String>> currentEncryptionKeySalt;

    public PasswordBasedEncryption(char[] password, SecureRandom secureRandom) {
        this.password = password;
        this.secureRandom = secureRandom;
        this.keyBySaltCache = CacheBuilder.<String, Tuple<byte[], SecretKey>>builder()
                .setMaximumWeight(ENCRYPTION_KEY_CACHE_SIZE)
                .build();
        // set the random salt which is used to generate the encryption key
        byte[] randomEncryptionKeySaltBytes = new byte[SALT_LENGTH_IN_BYTES];
        secureRandom.nextBytes(randomEncryptionKeySaltBytes);
        this.currentEncryptionKeySalt = new AtomicReference<>(new LimitedSupplier<>(
                new String(Base64.getEncoder().encode(randomEncryptionKeySaltBytes), StandardCharsets.UTF_8),
                ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY));
    }

    public byte[] encrypt(byte[] data, @Nullable byte[] associatedData) throws ExecutionException, GeneralSecurityException {
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
        // update the cipher with the associated data
        if (associatedData != null && associatedData.length > 0) {
            cipher.updateAAD(associatedData);
        }
        // encrypt the data
        byte[] encryptedData = cipher.doFinal(data);
        // concatenate key salt, iv and metadata cipher text
        byte[] resultCiphertext = new byte[saltAndEncryptionKey.v1().length + iv.length + encryptedData.length];
        // prepend salt
        System.arraycopy(saltAndEncryptionKey.v1(), 0, resultCiphertext, 0, saltAndEncryptionKey.v1().length);
        System.arraycopy(iv, 0, resultCiphertext, saltAndEncryptionKey.v1().length, iv.length);
        System.arraycopy(encryptedData, 0, resultCiphertext, saltAndEncryptionKey.v1().length + iv.length, encryptedData.length);
        return resultCiphertext;
    }

    public byte[] decrypt(byte[] encryptedData, @Nullable byte[] associatedData) throws ExecutionException, GeneralSecurityException {
        if (Objects.requireNonNull(encryptedData).length < SALT_LENGTH_IN_BYTES + IV_LENGTH_IN_BYTES + TAG_LENGTH_IN_BYTES) {
            throw new IllegalArgumentException("Ciphertext too short");
        }
        // extract the salt prepended to the ciphertext
        byte[] salt = Arrays.copyOf(encryptedData, SALT_LENGTH_IN_BYTES);
        // get the key associated with the salt
        SecretKey decryptionKey = getKeyFromSalt(new String(Base64.getEncoder().encode(salt), StandardCharsets.UTF_8)).v2();
        // construct and initialize the decryption cipher
        GCMParameterSpec gcmParameterSpec = new GCMParameterSpec(TAG_LENGTH_IN_BYTES * Byte.SIZE, encryptedData, SALT_LENGTH_IN_BYTES,
                IV_LENGTH_IN_BYTES);
        Cipher cipher = Cipher.getInstance(CIPHER_ALGO + "/" + CIPHER_MODE + "/" + CIPHER_PADDING);
        cipher.init(Cipher.DECRYPT_MODE, decryptionKey, gcmParameterSpec);
        // update the cipher with the associated data
        if (associatedData != null && associatedData.length > 0) {
            cipher.updateAAD(associatedData);
        }
        // decrypt data
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

    /**
     * Return a secret key given the salt; computes the key if the key for the salt argument is not already cached.
     */
    private Tuple<byte[], SecretKey> getKeyFromSalt(String salt) throws ExecutionException {
        return this.keyBySaltCache.computeIfAbsent(salt, (ignore) -> {
            byte[] saltBytes = Base64.getDecoder().decode(salt);
            SecretKey secretKey = generatePasswordBasedSecretKey(password, saltBytes);
            return new Tuple<>(saltBytes, secretKey);
        });
    }

    /**
     * Replaces the currently exhausted salt supplier with a new one. The new salt is generated randomly.
     */
    private void resetCurrentEncryptionKeySalt(LimitedSupplier<String> currentExhaustedSupplier) {
        // generate a new random salt
        byte[] randomEncryptionKeySaltBytes = new byte[SALT_LENGTH_IN_BYTES];
        secureRandom.nextBytes(randomEncryptionKeySaltBytes);
        LimitedSupplier<String> newSaltSupplier = new LimitedSupplier<>(
                new String(Base64.getEncoder().encode(randomEncryptionKeySaltBytes), StandardCharsets.UTF_8),
                ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY);
        // replace the old salt supplier with the new one
        this.currentEncryptionKeySalt.compareAndExchange(currentExhaustedSupplier, newSaltSupplier);
    }

    private Tuple<byte[], SecretKey> useEncryptionKey() throws ExecutionException {
        // get the salt of the encryption key
        LimitedSupplier<String> currentEncryptionKeySaltSupplier = currentEncryptionKeySalt.get();
        Optional<String> encryptionKeySalt = currentEncryptionKeySaltSupplier.get();
        if (encryptionKeySalt.isPresent()) {
            // the salt has NOT been used more than {@code #ENCRYPT_INVOKE_LIMIT_USING_SAME_KEY} times
            return getKeyFromSalt(encryptionKeySalt.get());
        }
        // change the salt used to generate a new encryption key
        resetCurrentEncryptionKeySalt(currentEncryptionKeySaltSupplier);
        // try to use the new supplier again
        return useEncryptionKey();
    }

    /**
     * A supplier accepting a limited number of retrieve (get) invocations. After the limit has been exceeded
     * the supplier returns {@code Optional#empty()}.
     */
    static class LimitedSupplier<T> implements Supplier<Optional<T>> {
        // the current {@code #get()) invocation count
        private final AtomicLong count;
        // the constant value to return when the invocation count has not been exceeded
        private final Optional<T> value;
        private final long limit;

        LimitedSupplier(T value, long limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit argument must be strictly positive");
            }
            this.count = new AtomicLong(0L);
            this.value = Optional.of(Objects.requireNonNull(value));
            this.limit = limit;
        }

        @Override
        public Optional<T> get() {
            long invocationCount = count.getAndUpdate(prev -> prev < limit ? prev + 1 : limit);
            if (invocationCount < limit) {
                return value;
            }
            return Optional.empty();
        }

    }

}
