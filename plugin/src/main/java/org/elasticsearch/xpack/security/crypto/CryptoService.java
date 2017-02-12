/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.crypto;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.regex.Pattern;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.XPackPlugin;
import org.elasticsearch.xpack.security.authc.support.CharArrays;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.authc.support.SecuredString.constantTimeEquals;

/**
 * Service that provides cryptographic methods based on a shared system key
 */
public class CryptoService extends AbstractComponent {

    public static final String KEY_ALGO = "HmacSHA512";
    public static final int KEY_SIZE = 1024;

    static final String FILE_NAME = "system_key";
    static final String HMAC_ALGO = "HmacSHA1";
    static final String DEFAULT_ENCRYPTION_ALGORITHM = "AES/CTR/NoPadding";
    static final String DEFAULT_KEY_ALGORITH = "AES";
    static final String ENCRYPTED_TEXT_PREFIX = "::es_encrypted::";
    static final int DEFAULT_KEY_LENGTH = 128;

    private static final Pattern SIG_PATTERN = Pattern.compile("^\\$\\$[0-9]+\\$\\$[^\\$]*\\$\\$.+");

    private static final Setting<Boolean> SYSTEM_KEY_REQUIRED_SETTING =
            Setting.boolSetting(setting("system_key.required"), false, Property.NodeScope);
    private static final Setting<String> ENCRYPTION_ALGO_SETTING =
            new Setting<>(setting("encryption.algorithm"), s -> DEFAULT_ENCRYPTION_ALGORITHM, s -> s, Property.NodeScope);
    private static final Setting<Integer> ENCRYPTION_KEY_LENGTH_SETTING =
            Setting.intSetting(setting("encryption_key.length"), DEFAULT_KEY_LENGTH, Property.NodeScope);
    private static final Setting<String> ENCRYPTION_KEY_ALGO_SETTING =
            new Setting<>(setting("encryption_key.algorithm"), DEFAULT_KEY_ALGORITH, s -> s, Property.NodeScope);

    private final SecureRandom secureRandom = new SecureRandom();
    private final String encryptionAlgorithm;
    private final int ivLength;
    /*
     * Scroll ids are signed using the system key to authenticate them as we currently do not have a proper way to authorize these requests
     * and the key is also used for encrypting sensitive data stored in watches. The encryption key is derived from the system key.
     */
    private final SecretKey encryptionKey;
    private final SecretKey systemKey;

    public CryptoService(Settings settings, Environment env) throws IOException {
        super(settings);
        this.encryptionAlgorithm = ENCRYPTION_ALGO_SETTING.get(settings);
        final int keyLength = ENCRYPTION_KEY_LENGTH_SETTING.get(settings);
        this.ivLength = keyLength / 8;
        String keyAlgorithm = ENCRYPTION_KEY_ALGO_SETTING.get(settings);

        if (keyLength % 8 != 0) {
            throw new IllegalArgumentException("invalid key length [" + keyLength + "]. value must be a multiple of 8");
        }

        Path keyFile = resolveSystemKey(env);
        systemKey = readSystemKey(keyFile, SYSTEM_KEY_REQUIRED_SETTING.get(settings));

        try {
            encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
        } catch (NoSuchAlgorithmException nsae) {
            throw new ElasticsearchException("failed to start crypto service. could not load encryption key", nsae);
        }
        if (systemKey != null) {
            logger.info("system key [{}] has been loaded", keyFile.toAbsolutePath());
        }
    }

    public static byte[] generateKey() {
        return generateSecretKey(KEY_SIZE).getEncoded();
    }

    static SecretKey generateSecretKey(int keyLength) {
        try {
            KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGO);
            generator.init(keyLength);
            return generator.generateKey();
        } catch (NoSuchAlgorithmException e) {
            throw new ElasticsearchException("failed to generate key", e);
        }
    }

    public static Path resolveSystemKey(Environment env) {
        return XPackPlugin.resolveConfigFile(env, FILE_NAME);
    }

    private static SecretKey readSystemKey(Path file, boolean required) throws IOException {
        if (Files.exists(file)) {
            byte[] bytes = Files.readAllBytes(file);
            return new SecretKeySpec(bytes, KEY_ALGO);
        }

        if (required) {
            throw new FileNotFoundException("[" + file + "] must be present with a valid key");
        }

        return null;
    }

    /**
     * Signs the given text and returns the signed text (original text + signature)
     * @param text the string to sign
     */
    public String sign(String text) throws IOException {
        if (systemKey != null) {
            String sigStr = signInternal(text, systemKey);
            return "$$" + sigStr.length() + "$$$$" + sigStr + text;
        } else {
            return text;
        }
    }

    /**
     * Unsigns the given signed text, verifies the original text with the attached signature and if valid returns
     * the unsigned (original) text. If signature verification fails a {@link IllegalArgumentException} is thrown.
     * @param signedText the string to unsign and verify
     */
    public String unsignAndVerify(String signedText) {
        if (systemKey == null) {
            return signedText;
        }

        if (!signedText.startsWith("$$") || signedText.length() < 2) {
            throw new IllegalArgumentException("tampered signed text");
        }

        // $$34$$$$sigtext
        String[] pieces = signedText.split("\\$\\$");
        if (pieces.length != 4 || pieces[0].isEmpty() == false || pieces[2].isEmpty() == false) {
            logger.debug("received signed text [{}] with [{}] parts", signedText, pieces.length);
            throw new IllegalArgumentException("tampered signed text");
        }
        String text;
        String receivedSignature;
        try {
            int length = Integer.parseInt(pieces[1]);
            receivedSignature = pieces[3].substring(0, length);
            text = pieces[3].substring(length);
        } catch (Exception e) {
            logger.error("error occurred while parsing signed text", e);
            throw new IllegalArgumentException("tampered signed text");
        }

        try {
            String sig = signInternal(text, systemKey);
            if (constantTimeEquals(sig, receivedSignature)) {
                return text;
            }
        } catch (Exception e) {
            logger.error("error occurred while verifying signed text", e);
            throw new IllegalStateException("error while verifying the signed text");
        }

        throw new IllegalArgumentException("tampered signed text");
    }

    /**
     * Checks whether the given text is signed.
     */
    public boolean isSigned(String text) {
        return SIG_PATTERN.matcher(text).matches();
    }

    /**
     * Encrypts the provided char array and returns the encrypted values in a char array
     * @param chars the characters to encrypt
     * @return character array representing the encrypted data
     */
    public char[] encrypt(char[] chars) {
        SecretKey key = this.encryptionKey;
        if (key == null) {
            logger.warn("encrypt called without a key, returning plain text. run syskeygen and copy same key to all nodes to enable " +
                "encryption");
            return chars;
        }

        byte[] charBytes = CharArrays.toUtf8Bytes(chars);
        String base64 = Base64.getEncoder().encodeToString(encryptInternal(charBytes, key));
        return ENCRYPTED_TEXT_PREFIX.concat(base64).toCharArray();
    }

    /**
     * Decrypts the provided char array and returns the plain-text chars
     * @param chars the data to decrypt
     * @return plaintext chars
     */
    public char[] decrypt(char[] chars) {
        if (encryptionKey == null) {
            return chars;
        }

        if (!isEncrypted(chars)) {
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
    public boolean isEncrypted(char[] chars) {
        return CharArrays.charsBeginsWith(ENCRYPTED_TEXT_PREFIX, chars);
    }

    /**
     * Flag for callers to determine if values will actually be encrypted or returned plaintext
     * @return true if values will be encrypted
     */
    public boolean isEncryptionEnabled() {
        return this.encryptionKey != null;
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

    static Mac createMac(SecretKey key) {
        try {
            Mac mac = HmacSHA1Provider.hmacSHA1();
            mac.init(key);
            return mac;
        } catch (Exception e) {
            throw new ElasticsearchException("could not initialize mac", e);
        }
    }

    private static String signInternal(String text, SecretKey key) throws IOException {
        Mac mac = createMac(key);
        byte[] sig = mac.doFinal(text.getBytes(StandardCharsets.UTF_8));
        return Base64.getUrlEncoder().encodeToString(sig);
    }


    static Cipher cipher(int mode, String encryptionAlgorithm, SecretKey key, byte[] initializationVector) {
        try {
            Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
            cipher.init(mode, key, new IvParameterSpec(initializationVector));
            return cipher;
        } catch (Exception e) {
            throw new ElasticsearchException("error creating cipher", e);
        }
    }

    static SecretKey encryptionKey(SecretKey systemKey, int keyLength, String algorithm) throws NoSuchAlgorithmException {
        if (systemKey == null) {
            return null;
        }

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

    /**
     * Provider class for the HmacSHA1 {@link Mac} that provides an optimization by using a thread local instead of calling
     * Mac#getInstance and obtaining a lock (in the internals)
     */
    private static class HmacSHA1Provider {

        private static final ThreadLocal<Mac> MAC = ThreadLocal.withInitial(() -> {
            try {
                return Mac.getInstance(HMAC_ALGO);
            } catch (NoSuchAlgorithmException e) {
                throw new IllegalStateException("could not create Mac instance with algorithm [" + HMAC_ALGO + "]", e);
            }
        });

        private static Mac hmacSHA1() {
            Mac instance = MAC.get();
            instance.reset();
            return instance;
        }

    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ENCRYPTION_KEY_LENGTH_SETTING);
        settings.add(ENCRYPTION_KEY_ALGO_SETTING);
        settings.add(ENCRYPTION_ALGO_SETTING);
        settings.add(SYSTEM_KEY_REQUIRED_SETTING);
    }
}
