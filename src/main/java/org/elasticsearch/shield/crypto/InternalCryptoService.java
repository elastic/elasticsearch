/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.base.Charsets;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldException;
import org.elasticsearch.shield.ShieldPlugin;
import org.elasticsearch.shield.ShieldSettingsException;
import org.elasticsearch.shield.authc.support.CharArrays;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;

import javax.crypto.*;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.regex.Pattern;

import static org.elasticsearch.shield.authc.support.SecuredString.constantTimeEquals;

/**
 *
 */
public class InternalCryptoService extends AbstractLifecycleComponent<InternalCryptoService> implements CryptoService {

    public static final String FILE_SETTING = "shield.system_key.file";
    public static final String KEY_ALGO = "HmacSHA512";
    public static final int KEY_SIZE = 1024;

    static final String FILE_NAME = "system_key";
    static final String HMAC_ALGO = "HmacSHA1";
    static final String DEFAULT_ENCRYPTION_ALGORITHM = "AES/CTR/NoPadding";
    static final String DEFAULT_KEY_ALGORITH = "AES";
    static final int DEFAULT_KEY_LENGTH = 128;

    private static final Pattern SIG_PATTERN = Pattern.compile("^\\$\\$[0-9]+\\$\\$.+");

    private final Environment env;
    private final ResourceWatcherService watcherService;
    private final Listener listener;
    private final SecureRandom secureRandom = new SecureRandom();
    private final String encryptionAlgorithm;
    private final String keyAlgorithm;
    private final int keyLength;
    private final int ivLength;

    private Path keyFile;

    private volatile SecretKey encryptionKey;
    private volatile SecretKey systemKey;

    @Inject
    public InternalCryptoService(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Listener.NOOP);
    }

    InternalCryptoService(Settings settings, Environment env, ResourceWatcherService watcherService, Listener listener) {
        super(settings);
        this.env = env;
        this.watcherService = watcherService;
        this.listener = listener;
        this.encryptionAlgorithm = settings.get("shield.encryption.algorithm", DEFAULT_ENCRYPTION_ALGORITHM);
        this.keyLength = settings.getAsInt("shield.encryption_key.length", DEFAULT_KEY_LENGTH);
        if (keyLength % 8 != 0) {
            throw new ShieldSettingsException("invalid key length [" + keyLength + "]. value must be a multiple of 8");
        }
        this.ivLength = keyLength / 8;
        this.keyAlgorithm = settings.get("shield.encryption_key.algorithm", DEFAULT_KEY_ALGORITH);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        keyFile = resolveSystemKey(settings, env);
        systemKey = readSystemKey(keyFile);
        encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
        FileWatcher watcher = new FileWatcher(keyFile.getParent());
        watcher.addListener(new FileListener(listener));
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching system key file [" + keyFile.toAbsolutePath() + "]", e);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {}

    @Override
    protected void doClose() throws ElasticsearchException {}

    public static byte[] generateKey() throws Exception {
        KeyGenerator generator = KeyGenerator.getInstance(KEY_ALGO);
        generator.init(KEY_SIZE);
        return generator.generateKey().getEncoded();
    }

    public static Path resolveSystemKey(Settings settings, Environment env) {
        String location = settings.get(FILE_SETTING);
        if (location == null) {
            return ShieldPlugin.resolveConfigFile(env, FILE_NAME);
        }
        return env.homeFile().resolve(location);
    }

    static SecretKey readSystemKey(Path file) {
        if (!Files.exists(file)) {
            return null;
        }
        try {
            byte[] bytes = Files.readAllBytes(file);
            return new SecretKeySpec(bytes, KEY_ALGO);
        } catch (IOException e) {
            throw new ShieldException("could not read secret key", e);
        }
    }

    @Override
    public String sign(String text) {
        SecretKey key = this.systemKey;
        if (key == null) {
            return text;
        }
        String sigStr = signInternal(text);
        return "$$" + sigStr.length() + "$$" + sigStr + text;
    }

    @Override
    public String unsignAndVerify(String signedText) {
        SecretKey key = this.systemKey;
        if (key == null) {
            return signedText;
        }

        if (!signedText.startsWith("$$") || signedText.length() < 2) {
            throw new SignatureException("tampered signed text");
        }

        String text;
        String receivedSignature;
        try {
            // $$34$$sigtext
            int i = signedText.indexOf("$$", 2);
            int length = Integer.parseInt(signedText.substring(2, i));
            receivedSignature = signedText.substring(i + 2, i + 2 + length);
            text = signedText.substring(i + 2 + length);
        } catch (Throwable t) {
            logger.error("error occurred while parsing signed text", t);
            throw new SignatureException("tampered signed text");
        }

        try {
            String sig = signInternal(text);
            if (constantTimeEquals(sig, receivedSignature)) {
                return text;
            }
        } catch (Throwable t) {
            logger.error("error occurred while verifying signed text", t);
            throw new SignatureException("error while verifying the signed text");
        }

        throw new SignatureException("tampered signed text");
    }

    @Override
    public boolean signed(String text) {
        return SIG_PATTERN.matcher(text).matches();
    }

    @Override
    public char[] encrypt(char[] chars) {
        SecretKey key = this.encryptionKey;
        if (key == null) {
            throw new UnsupportedOperationException("encryption cannot be performed without a system key. please run bin/shield/syskeygen on one node and copy\n"
                + "the file [" + ShieldPlugin.resolveConfigFile(env, FILE_NAME) + "] to all nodes and the key will be loaded automatically.");
        }

        byte[] charBytes = CharArrays.toUtf8Bytes(chars);
        return Base64.encodeBase64String(encryptInternal(charBytes, key)).toCharArray();
    }

    @Override
    public byte[] encrypt(byte[] bytes) {
        SecretKey key = this.encryptionKey;
        if (key == null) {
            throw new UnsupportedOperationException("encryption cannot be performed without a system key. please run bin/shield/syskeygen on one node and copy\n"
                    + "the file [" + ShieldPlugin.resolveConfigFile(env, FILE_NAME) + "] to all nodes and the key will be loaded automatically.");
        }
        return encryptInternal(bytes, key);
    }

    @Override
    public char[] decrypt(char[] chars) {
        SecretKey key = this.encryptionKey;
        if (key == null) {
            throw new UnsupportedOperationException("decryption cannot be performed without a system key. please run bin/shield/syskeygen on one node and copy\n"
                    + "the file [" + ShieldPlugin.resolveConfigFile(env, FILE_NAME) + "] to all nodes and the key will be loaded automatically.");
        }

        byte[] bytes = Base64.decodeBase64(new String(chars));
        byte[] decrypted = decryptInternal(bytes, key);
        return CharArrays.utf8BytesToChars(decrypted);
    }

    @Override
    public byte[] decrypt(byte[] bytes) {
        SecretKey key = this.encryptionKey;
        if (key == null) {
            throw new UnsupportedOperationException("decryption cannot be performed without a system key. please run bin/shield/syskeygen on one node and copy\n"
                    + "the file [" + ShieldPlugin.resolveConfigFile(env, FILE_NAME) + "] to all nodes and the key will be loaded automatically.");
        }
        return decryptInternal(bytes, key);
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
        } catch (BadPaddingException |IllegalBlockSizeException e) {
            throw new ShieldException("error encrypting data", e);
        }
    }

    private byte[] decryptInternal(byte[] bytes, SecretKey key) {
        if (bytes.length < ivLength) {
            logger.error("received data for decryption with size [{}] that is less than IV length [{}]", bytes.length, ivLength);
            throw new ShieldException("invalid data to decrypt");
        }

        byte[] iv = new byte[ivLength];
        System.arraycopy(bytes, 0, iv, 0, ivLength);
        byte[] data = new byte[bytes.length - ivLength];
        System.arraycopy(bytes, ivLength, data, 0, bytes.length - ivLength);

        Cipher cipher = cipher(Cipher.DECRYPT_MODE, encryptionAlgorithm, key, iv);
        try {
            return cipher.doFinal(data);
        } catch (BadPaddingException|IllegalBlockSizeException e) {
            throw new ShieldException("error decrypting data", e);
        }
    }

    static Mac createMac(SecretKey key) {
        try {
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(key);
            return mac;
        } catch (Exception e) {
            throw new ElasticsearchException("could not initialize mac", e);
        }
    }

    private String signInternal(String text) {
        Mac mac = createMac(systemKey);
        byte[] sig = mac.doFinal(text.getBytes(Charsets.UTF_8));
        return Base64.encodeBase64URLSafeString(sig);
    }


    static Cipher cipher(int mode, String encryptionAlgorithm, SecretKey key, byte[] initializationVector) {
        try {
            Cipher cipher = Cipher.getInstance(encryptionAlgorithm);
            cipher.init(mode, key, new IvParameterSpec(initializationVector));
            return cipher;
        } catch (Exception e) {
            throw new ShieldException("error creating cipher", e);
        }
    }

    static SecretKey encryptionKey(SecretKey systemKey, int keyLength, String algorithm) {
        if (systemKey == null) {
            return null;
        }

        try {
            byte[] bytes = systemKey.getEncoded();
            if ((bytes.length * 8) < keyLength) {
                throw new ShieldException("at least " + keyLength +" bits should be provided as key data");
            }

            MessageDigest messageDigest = MessageDigest.getInstance("SHA-256");
            byte[] digest = messageDigest.digest(bytes);
            assert digest.length == (256 / 8);

            if ((digest.length * 8) < keyLength) {
                throw new ShieldException("requested key length is too large");
            }
            byte[] truncatedDigest = Arrays.copyOfRange(digest, 0, (keyLength / 8));

            return new SecretKeySpec(truncatedDigest, algorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new ShieldException("error getting encryption key", e);
        }
    }

    private class FileListener extends FileChangesListener {

        private final Listener listener;

        private FileListener(Listener listener) {
            this.listener = listener;
        }

        @Override
        public void onFileCreated(Path file) {
            if (file.equals(keyFile)) {
                systemKey = readSystemKey(file);
                encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
                logger.info("system key [{}] has been loaded", file.toAbsolutePath());
                listener.onKeyRefresh();
            }
        }

        @Override
        public void onFileDeleted(Path file) {
            if (file.equals(keyFile)) {
                logger.error("system key file was removed! as long as the system key file is missing, elasticsearch " +
                        "won't function as expected for some requests (e.g. scroll/scan) and won't be able to decrypt\n" +
                        "previously encrypted values without the original key");
                systemKey = null;
                encryptionKey = null;
            }
        }

        @Override
        public void onFileChanged(Path file) {
            if (file.equals(keyFile)) {
                logger.warn("system key file changed! previously encrypted values cannot be successfully decrypted with a different key");
                systemKey = readSystemKey(file);
                encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
                listener.onKeyRefresh();
            }
        }
    }

    static interface Listener {

        final Listener NOOP = new Listener() {
            @Override
            public void onKeyRefresh() {
            }
        };

        void onKeyRefresh();
    }
}
