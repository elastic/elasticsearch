/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto;

import com.google.common.base.Charsets;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.ShieldPlugin;
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
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
    static final String ENCRYPTED_TEXT_PREFIX = "::es_encrypted::";
    static final byte[] ENCRYPTED_BYTE_PREFIX = ENCRYPTED_TEXT_PREFIX.getBytes(Charsets.UTF_8);
    static final int DEFAULT_KEY_LENGTH = 128;

    private static final Pattern SIG_PATTERN = Pattern.compile("^\\$\\$[0-9]+\\$\\$.+");

    private final Environment env;
    private final ResourceWatcherService watcherService;
    private final List<Listener> listeners;
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
        this(settings, env, watcherService, Collections.<Listener>emptyList());
    }

    InternalCryptoService(Settings settings, Environment env, ResourceWatcherService watcherService, List<Listener> listeners) {
        super(settings);
        this.env = env;
        this.watcherService = watcherService;
        this.listeners = new CopyOnWriteArrayList<>(listeners);
        this.encryptionAlgorithm = settings.get("shield.encryption.algorithm", DEFAULT_ENCRYPTION_ALGORITHM);
        this.keyLength = settings.getAsInt("shield.encryption_key.length", DEFAULT_KEY_LENGTH);
        this.ivLength = keyLength / 8;
        this.keyAlgorithm = settings.get("shield.encryption_key.algorithm", DEFAULT_KEY_ALGORITH);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (keyLength % 8 != 0) {
            throw new IllegalArgumentException("invalid key length [" + keyLength + "]. value must be a multiple of 8");
        }

        keyFile = resolveSystemKey(settings, env);
        systemKey = readSystemKey(keyFile);
        try {
            encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
        } catch (NoSuchAlgorithmException nsae) {
            throw new ElasticsearchException("failed to start crypto service. could not load encryption key", nsae);
        }
        FileWatcher watcher = new FileWatcher(keyFile.getParent());
        watcher.addListener(new FileListener(listeners));
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
            throw new ElasticsearchException("could not read secret key", e);
        }
    }

    @Override
    public String sign(String text) throws IOException {
        return sign(text, this.systemKey);
    }

    @Override
    public String sign(String text, SecretKey key) throws IOException {
        if (key == null) {
            return text;
        }
        String sigStr = signInternal(text, key);
        return "$$" + sigStr.length() + "$$" + sigStr + text;
    }

    @Override
    public String unsignAndVerify(String signedText) {
        return unsignAndVerify(signedText, this.systemKey);
    }

    @Override
    public String unsignAndVerify(String signedText, SecretKey key) {
        if (key == null) {
            return signedText;
        }

        if (!signedText.startsWith("$$") || signedText.length() < 2) {
            throw new IllegalArgumentException("tampered signed text");
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
            throw new IllegalArgumentException("tampered signed text");
        }

        try {
            String sig = signInternal(text, key);
            if (constantTimeEquals(sig, receivedSignature)) {
                return text;
            }
        } catch (Throwable t) {
            logger.error("error occurred while verifying signed text", t);
            throw new IllegalStateException("error while verifying the signed text");
        }

        throw new IllegalArgumentException("tampered signed text");
    }

    @Override
    public boolean signed(String text) {
        return SIG_PATTERN.matcher(text).matches();
    }

    @Override
    public char[] encrypt(char[] chars) {
        SecretKey key = this.encryptionKey;
        if (key == null) {
            logger.warn("encrypt called without a key, returning plain text. run syskeygen and copy same key to all nodes to enable encryption");
            return chars;
        }

        byte[] charBytes = CharArrays.toUtf8Bytes(chars);
        String base64 = Base64.encodeBytes(encryptInternal(charBytes, key));
        return ENCRYPTED_TEXT_PREFIX.concat(base64).toCharArray();
    }

    @Override
    public byte[] encrypt(byte[] bytes) {
        SecretKey key = this.encryptionKey;
        if (key == null) {
            logger.warn("encrypt called without a key, returning plain text. run syskeygen and copy same key to all nodes to enable encryption");
            return bytes;
        }
        byte[] encrypted = encryptInternal(bytes, key);
        byte[] prefixed = new byte[ENCRYPTED_BYTE_PREFIX.length + encrypted.length];
        System.arraycopy(ENCRYPTED_BYTE_PREFIX, 0, prefixed, 0, ENCRYPTED_BYTE_PREFIX.length);
        System.arraycopy(encrypted, 0, prefixed, ENCRYPTED_BYTE_PREFIX.length, encrypted.length);
        return prefixed;
    }

    @Override
    public char[] decrypt(char[] chars) {
        return decrypt(chars, this.encryptionKey);
    }

    @Override
    public char[] decrypt(char[] chars, SecretKey key) {
        if (key == null) {
            return chars;
        }

        if (!encrypted(chars)) {
            // Not encrypted
            return chars;
        }

        String encrypted = new String(chars, ENCRYPTED_TEXT_PREFIX.length(), chars.length - ENCRYPTED_TEXT_PREFIX.length());
        byte[] bytes;
        try {
            bytes = Base64.decode(encrypted);
        } catch (IOException e) {
            throw new ElasticsearchException("unable to decode encrypted data", e);
        }

        byte[] decrypted = decryptInternal(bytes, key);
        return CharArrays.utf8BytesToChars(decrypted);
    }

    @Override
    public byte[] decrypt(byte[] bytes) {
        return decrypt(bytes, this.encryptionKey);
    }

    @Override
    public byte[] decrypt(byte[] bytes, SecretKey key) {
        if (key == null) {
            return bytes;
        }

        if (!encrypted(bytes)) {
            return bytes;
        }

        byte[] encrypted = Arrays.copyOfRange(bytes, ENCRYPTED_BYTE_PREFIX.length, bytes.length);
        return decryptInternal(encrypted, key);
    }

    @Override
    public boolean encrypted(char[] chars) {
        return CharArrays.charsBeginsWith(ENCRYPTED_TEXT_PREFIX, chars);
    }

    @Override
    public boolean encrypted(byte[] bytes) {
        return bytesBeginsWith(ENCRYPTED_BYTE_PREFIX, bytes);
    }

    @Override
    public void register(Listener listener) {
        this.listeners.add(listener);
    }

    @Override
    public boolean encryptionEnabled() {
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
        } catch (BadPaddingException|IllegalBlockSizeException e) {
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
            Mac mac = Mac.getInstance(HMAC_ALGO);
            mac.init(key);
            return mac;
        } catch (Exception e) {
            throw new ElasticsearchException("could not initialize mac", e);
        }
    }

    private static String signInternal(String text, SecretKey key) throws IOException {
        Mac mac = createMac(key);
        byte[] sig = mac.doFinal(text.getBytes(Charsets.UTF_8));
        return Base64.encodeBytes(sig, 0, sig.length, Base64.URL_SAFE);
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
            throw new IllegalArgumentException("at least " + keyLength +" bits should be provided as key data");
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

    private static boolean bytesBeginsWith(byte[] prefix, byte[] bytes) {
        if (bytes == null || prefix == null) {
            return false;
        }

        if (prefix.length > bytes.length) {
            return false;
        }

        for (int i = 0; i < prefix.length; i++) {
            if (bytes[i] != prefix[i]) {
                return false;
            }
        }

        return true;
    }

    private class FileListener extends FileChangesListener {

        private final List<Listener> listeners;

        private FileListener(List<Listener> listeners) {
            this.listeners = listeners;
        }

        @Override
        public void onFileCreated(Path file) {
            if (file.equals(keyFile)) {
                final SecretKey oldSystemKey = systemKey;
                final SecretKey oldEncryptionKey = encryptionKey;

                systemKey = readSystemKey(file);
                try {
                    encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
                } catch (NoSuchAlgorithmException nsae) {
                    logger.error("could not load encryption key", nsae);
                    encryptionKey = null;
                }
                logger.info("system key [{}] has been loaded", file.toAbsolutePath());
                callListeners(oldSystemKey, oldEncryptionKey);
            }
        }

        @Override
        public void onFileDeleted(Path file) {
            if (file.equals(keyFile)) {
                final SecretKey oldSystemKey = systemKey;
                final SecretKey oldEncryptionKey = encryptionKey;
                logger.error("system key file was removed! as long as the system key file is missing, elasticsearch " +
                        "won't function as expected for some requests (e.g. scroll/scan)");
                systemKey = null;
                encryptionKey = null;

                callListeners(oldSystemKey, oldEncryptionKey);
            }
        }

        @Override
        public void onFileChanged(Path file) {
            if (file.equals(keyFile)) {
                final SecretKey oldSystemKey = systemKey;
                final SecretKey oldEncryptionKey = encryptionKey;

                logger.warn("system key file changed!");
                systemKey = readSystemKey(file);
                try {
                    encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
                } catch (NoSuchAlgorithmException nsae) {
                    logger.error("could not load encryption key", nsae);
                    encryptionKey = null;
                }

                callListeners(oldSystemKey, oldEncryptionKey);
            }
        }

        private void callListeners(SecretKey oldSystemKey, SecretKey oldEncryptionKey) {
            Throwable th = null;
            for (Listener listener : listeners) {
                try {
                    listener.onKeyChange(oldSystemKey, oldEncryptionKey);
                } catch (Throwable t) {
                    if (th == null) {
                        th = t;
                    } else {
                        th.addSuppressed(t);
                    }
                }
            }

            // all listeners were notified now rethrow
            if (th != null) {
                logger.error("called all key change listeners but one or more exceptions was thrown", th);
                if (th instanceof RuntimeException) {
                    throw (RuntimeException) th;
                } else if (th instanceof Error) {
                    throw (Error) th;
                } else {
                    throw new RuntimeException(th);
                }
            }
        }
    }
}
