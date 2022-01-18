/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.watcher.crypto;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CharArrays;
import org.elasticsearch.xpack.core.security.SecurityField;
import org.elasticsearch.xpack.core.watcher.WatcherField;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * Service that provides cryptographic methods based on a shared system key
 */
public class CryptoService {

    public static final String KEY_ALGO = "HmacSHA512";
    public static final int KEY_SIZE = 1024;

    public static final String ENCRYPTED_TEXT_PREFIX = "::es_encrypted::";

    // the encryption used in this class was picked when Java 7 was still the min. supported
    // version. The use of counter mode was chosen to simplify the need to deal with padding
    // and for its speed. 128 bit key length is chosen due to the JCE policy that ships by
    // default with the Oracle JDK.
    // TODO: with better support in Java 8, we should consider moving to use AES GCM as it
    // also provides authentication of the encrypted data, which is something that we are
    // missing here.
    private static final String DEFAULT_ENCRYPTION_ALGORITHM = "AES/CTR/NoPadding";
    private static final String DEFAULT_KEY_ALGORITH = "AES";
    private static final int DEFAULT_KEY_LENGTH = 128;

    private static final Setting<String> ENCRYPTION_ALGO_SETTING = new Setting<>(
        SecurityField.setting("encryption.algorithm"),
        s -> DEFAULT_ENCRYPTION_ALGORITHM,
        s -> s,
        Property.NodeScope
    );
    private static final Setting<Integer> ENCRYPTION_KEY_LENGTH_SETTING = Setting.intSetting(
        SecurityField.setting("encryption_key.length"),
        DEFAULT_KEY_LENGTH,
        Property.NodeScope
    );
    private static final Setting<String> ENCRYPTION_KEY_ALGO_SETTING = new Setting<>(
        SecurityField.setting("encryption_key.algorithm"),
        DEFAULT_KEY_ALGORITH,
        s -> s,
        Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(CryptoService.class);

    private final SecureRandom secureRandom = new SecureRandom();
    private final String encryptionAlgorithm;
    private final int ivLength;
    /*
     * The encryption key is derived from the system key.
     */
    private final SecretKey encryptionKey;

    public CryptoService(Settings settings) throws IOException {
        this.encryptionAlgorithm = ENCRYPTION_ALGO_SETTING.get(settings);
        final int keyLength = ENCRYPTION_KEY_LENGTH_SETTING.get(settings);
        this.ivLength = keyLength / 8;
        String keyAlgorithm = ENCRYPTION_KEY_ALGO_SETTING.get(settings);

        if (keyLength % 8 != 0) {
            throw new IllegalArgumentException("invalid key length [" + keyLength + "]. value must be a multiple of 8");
        }

        try (InputStream in = WatcherField.ENCRYPTION_KEY_SETTING.get(settings)) {
            if (in == null) {
                throw new ElasticsearchException("setting [" + WatcherField.ENCRYPTION_KEY_SETTING.getKey() + "] must be set in keystore");
            }
            SecretKey systemKey = readSystemKey(in);
            try {
                encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
            } catch (NoSuchAlgorithmException nsae) {
                throw new ElasticsearchException("failed to start crypto service. could not load encryption key", nsae);
            }
        }
        assert encryptionKey != null : "the encryption key should never be null";
    }

    private static SecretKey readSystemKey(InputStream in) throws IOException {
        final int keySizeBytes = KEY_SIZE / 8;
        final byte[] keyBytes = new byte[keySizeBytes];
        final int read = Streams.readFully(in, keyBytes);
        if (read != keySizeBytes) {
            throw new IllegalArgumentException(
                "key size did not match expected value; was the key generated with elasticsearch-syskeygen?"
            );
        }
        return new SecretKeySpec(keyBytes, KEY_ALGO);
    }

    /**
     * Encrypts the provided char array and returns the encrypted values in a char array
     * @param chars the characters to encrypt
     * @return character array representing the encrypted data
     */
    public char[] encrypt(char[] chars) {
        byte[] charBytes = CharArrays.toUtf8Bytes(chars);
        String base64 = Base64.getEncoder().encodeToString(encryptInternal(charBytes, encryptionKey));
        return ENCRYPTED_TEXT_PREFIX.concat(base64).toCharArray();
    }

    /**
     * Decrypts the provided char array and returns the plain-text chars
     * @param chars the data to decrypt
     * @return plaintext chars
     */
    public char[] decrypt(char[] chars) {
        if (isEncrypted(chars) == false) {
            // Not encrypted
            return chars;
        }

        String encrypted = new String(chars, ENCRYPTED_TEXT_PREFIX.length(), chars.length - ENCRYPTED_TEXT_PREFIX.length());
        byte[] bytes;
        try {
            bytes = Base64.getDecoder().decode(encrypted);
        } catch (IllegalArgumentException e) {
            throw new ElasticsearchException("unable to decode encrypted data", e);
        }

        byte[] decrypted = decryptInternal(bytes, encryptionKey);
        return CharArrays.utf8BytesToChars(decrypted);
    }

    /**
     * Checks whether the given chars are encrypted
     * @param chars the chars to check if they are encrypted
     * @return true is data is encrypted
     */
    protected boolean isEncrypted(char[] chars) {
        return CharArrays.charsBeginsWith(ENCRYPTED_TEXT_PREFIX, chars);
    }

    private byte[] encryptInternal(byte[] bytes, SecretKey key) {
        byte[] iv = new byte[ivLength];
        secureRandom.nextBytes(iv);
        Cipher cipher = cipher(Cipher.ENCRYPT_MODE, encryptionAlgorithm, key, iv);
        try {
            byte[] encrypted = cipher.doFinal(bytes);
            byte[] output = new byte[iv.length + encrypted.length];
            System.arraycopy(iv, 0, output, 0, iv.length);
            System.arraycopy(encrypted, 0, output, iv.length, encrypted.length);
            return output;
        } catch (BadPaddingException | IllegalBlockSizeException e) {
            throw new ElasticsearchException("error encrypting data", e);
        }
    }

    private byte[] decryptInternal(byte[] bytes, SecretKey key) {
        if (bytes.length < ivLength) {
            logger.error("received data for decryption with size [{}] that is less than IV length [{}]", bytes.length, ivLength);
            throw new IllegalArgumentException("invalid data to decrypt");
        }

        byte[] iv = new byte[ivLength];
        System.arraycopy(bytes, 0, iv, 0, ivLength);
        byte[] data = new byte[bytes.length - ivLength];
        System.arraycopy(bytes, ivLength, data, 0, bytes.length - ivLength);

        Cipher cipher = cipher(Cipher.DECRYPT_MODE, encryptionAlgorithm, key, iv);
        try {
            return cipher.doFinal(data);
        } catch (BadPaddingException | IllegalBlockSizeException e) {
            throw new IllegalStateException("error decrypting data", e);
        }
    }

    private static Cipher cipher(int mode, String encryptionAlgorithm, SecretKey key, byte[] initializationVector) {
        try {
            Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
            cipher.init(mode, key, new IvParameterSpec(initializationVector));
            return cipher;
        } catch (Exception e) {
            throw new ElasticsearchException("error creating cipher", e);
        }
    }

    private static SecretKey encryptionKey(SecretKey systemKey, int keyLength, String algorithm) throws NoSuchAlgorithmException {
        byte[] bytes = systemKey.getEncoded();
        if ((bytes.length * 8) < keyLength) {
            throw new IllegalArgumentException("at least " + keyLength + " bits should be provided as key data");
        }

        MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
        byte[] digest = messageDigest.digest(bytes);
        assert digest.length == (256 / 8);

        if ((digest.length * 8) < keyLength) {
            throw new IllegalArgumentException("requested key length is too large");
        }
        byte[] truncatedDigest = Arrays.copyOfRange(digest, 0, (keyLength / 8));

        return new SecretKeySpec(truncatedDigest, algorithm);
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ENCRYPTION_KEY_LENGTH_SETTING);
        settings.add(ENCRYPTION_KEY_ALGO_SETTING);
        settings.add(ENCRYPTION_ALGO_SETTING);
    }
}
