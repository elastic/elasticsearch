/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.crypto;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Base64;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.env.Environment;
import org.elasticsearch.shield.authc.support.CharArrays;
import org.elasticsearch.watcher.FileChangesListener;
import org.elasticsearch.watcher.FileWatcher;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.XPackPlugin;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.KeyGenerator;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;

import static org.elasticsearch.shield.authc.support.SecuredString.constantTimeEquals;
import static org.elasticsearch.shield.Security.setting;

/**
 *
 */
public class InternalCryptoService extends AbstractLifecycleComponent<InternalCryptoService> implements CryptoService {

    public static final String KEY_ALGO = "HmacSHA512";
    public static final int KEY_SIZE = 1024;

    static final String FILE_NAME = "system_key";
    static final String HMAC_ALGO = "HmacSHA1";
    static final String DEFAULT_ENCRYPTION_ALGORITHM = "AES/CTR/NoPadding";
    static final String DEFAULT_KEY_ALGORITH = "AES";
    static final String ENCRYPTED_TEXT_PREFIX = "::es_encrypted::";
    static final byte[] ENCRYPTED_BYTE_PREFIX = ENCRYPTED_TEXT_PREFIX.getBytes(StandardCharsets.UTF_8);
    static final int DEFAULT_KEY_LENGTH = 128;
    static final int RANDOM_KEY_SIZE = 128;

    private static final Pattern SIG_PATTERN = Pattern.compile("^\\$\\$[0-9]+\\$\\$[^\\$]*\\$\\$.+");
    private static final byte[] HKDF_APP_INFO = "es-shield-crypto-service".getBytes(StandardCharsets.UTF_8);

    public static final Setting<String> FILE_SETTING = Setting.simpleString(setting("system_key.file"), Property.NodeScope);
    public static final Setting<String> ENCRYPTION_ALGO_SETTING =
            new Setting<>(setting("encryption.algorithm"), s -> DEFAULT_ENCRYPTION_ALGORITHM, s -> s, Property.NodeScope);
    public static final Setting<Integer> ENCRYPTION_KEY_LENGTH_SETTING =
            Setting.intSetting(setting("encryption_key.length"), DEFAULT_KEY_LENGTH, Property.NodeScope);
    public static final Setting<String> ENCRYPTION_KEY_ALGO_SETTING =
            new Setting<>(setting("encryption_key.algorithm"), DEFAULT_KEY_ALGORITH, s -> s, Property.NodeScope);

    private final Environment env;
    private final ResourceWatcherService watcherService;
    private final List<Listener> listeners;
    private final SecureRandom secureRandom = new SecureRandom();
    private final String encryptionAlgorithm;
    private final String keyAlgorithm;
    private final int keyLength;
    private final int ivLength;

    private Path keyFile;

    private SecretKey randomKey;
    private String randomKeyBase64;

    private volatile SecretKey encryptionKey;
    private volatile SecretKey systemKey;
    private volatile SecretKey signingKey;

    @Inject
    public InternalCryptoService(Settings settings, Environment env, ResourceWatcherService watcherService) {
        this(settings, env, watcherService, Collections.<Listener>emptyList());
    }

    InternalCryptoService(Settings settings, Environment env, ResourceWatcherService watcherService, List<Listener> listeners) {
        super(settings);
        this.env = env;
        this.watcherService = watcherService;
        this.listeners = new CopyOnWriteArrayList<>(listeners);
        this.encryptionAlgorithm = ENCRYPTION_ALGO_SETTING.get(settings);
        this.keyLength = ENCRYPTION_KEY_LENGTH_SETTING.get(settings);
        this.ivLength = keyLength / 8;
        this.keyAlgorithm = ENCRYPTION_KEY_ALGO_SETTING.get(settings);
    }

    @Override
    protected void doStart() throws ElasticsearchException {
        if (keyLength % 8 != 0) {
            throw new IllegalArgumentException("invalid key length [" + keyLength + "]. value must be a multiple of 8");
        }

        loadKeys();
        FileWatcher watcher = new FileWatcher(keyFile.getParent());
        watcher.addListener(new FileListener(listeners));
        try {
            watcherService.add(watcher, ResourceWatcherService.Frequency.HIGH);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to start watching system key file [" + keyFile.toAbsolutePath() + "]", e);
        }
    }

    @Override
    protected void doStop() throws ElasticsearchException {
    }

    @Override
    protected void doClose() throws ElasticsearchException {
    }

    private void loadKeys() {
        keyFile = resolveSystemKey(settings, env);
        systemKey = readSystemKey(keyFile);
        randomKey = generateSecretKey(RANDOM_KEY_SIZE);
        try {
            randomKeyBase64 = Base64.encodeBytes(randomKey.getEncoded(), 0, randomKey.getEncoded().length, Base64.URL_SAFE);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to encode key data as base64", e);
        }

        signingKey = createSigningKey(systemKey, randomKey);

        try {
            encryptionKey = encryptionKey(systemKey, keyLength, keyAlgorithm);
        } catch (NoSuchAlgorithmException nsae) {
            throw new ElasticsearchException("failed to start crypto service. could not load encryption key", nsae);
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

    public static Path resolveSystemKey(Settings settings, Environment env) {
        String location = FILE_SETTING.get(settings);
        if (location.isEmpty()) {
            return XPackPlugin.resolveConfigFile(env, FILE_NAME);
        }
        return XPackPlugin.resolveConfigFile(env, location);
    }

    static SecretKey createSigningKey(@Nullable SecretKey systemKey, SecretKey randomKey) {
        assert randomKey != null;
        if (systemKey != null) {
            return systemKey;
        } else {
            // the random key is only 128 bits so we use HKDF to expand to 1024 bits with some application specific data mixed in
            byte[] keyBytes = HmacSHA1HKDF.extractAndExpand(null, randomKey.getEncoded(), HKDF_APP_INFO, (KEY_SIZE / 8));
            assert keyBytes.length * 8 == KEY_SIZE;
            return new SecretKeySpec(keyBytes, KEY_ALGO);
        }
    }

    private static SecretKey readSystemKey(Path file) {
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
        return sign(text, this.signingKey, this.systemKey);
    }

    @Override
    public String sign(String text, SecretKey signingKey, @Nullable SecretKey systemKey) throws IOException {
        assert signingKey != null;
        String sigStr = signInternal(text, signingKey);
        return "$$" + sigStr.length() + "$$" + (systemKey == signingKey ? "" : randomKeyBase64) + "$$" + sigStr + text;
    }

    @Override
    public String unsignAndVerify(String signedText) {
        return unsignAndVerify(signedText, this.systemKey);
    }

    @Override
    public String unsignAndVerify(String signedText, SecretKey systemKey) {
        if (!signedText.startsWith("$$") || signedText.length() < 2) {
            throw new IllegalArgumentException("tampered signed text");
        }

        // $$34$$randomKeyBase64$$sigtext
        String[] pieces = signedText.split("\\$\\$");
        if (pieces.length != 4 || !pieces[0].equals("")) {
            logger.debug("received signed text [{}] with [{}] parts", signedText, pieces.length);
            throw new IllegalArgumentException("tampered signed text");
        }
        String text;
        String base64RandomKey;
        String receivedSignature;
        try {
            int length = Integer.parseInt(pieces[1]);
            base64RandomKey = pieces[2];
            receivedSignature = pieces[3].substring(0, length);
            text = pieces[3].substring(length);
        } catch (Throwable t) {
            logger.error("error occurred while parsing signed text", t);
            throw new IllegalArgumentException("tampered signed text");
        }

        SecretKey signingKey;
        // no random key, so we must have a system key
        if (base64RandomKey.isEmpty()) {
            if (systemKey == null) {
                logger.debug("received signed text without random key information and no system key is present");
                throw new IllegalArgumentException("tampered signed text");
            }
            signingKey = systemKey;
        } else if (systemKey != null) {
            // we have a system key and there is some random key data, this is an error
            logger.debug("received signed text with random key information but a system key is present");
            throw new IllegalArgumentException("tampered signed text");
        } else {
            byte[] randomKeyBytes;
            try {
                randomKeyBytes = Base64.decode(base64RandomKey, Base64.URL_SAFE);
                if (randomKeyBytes.length * 8 != RANDOM_KEY_SIZE) {
                    logger.debug("incorrect random key data length. received [{}] bytes", randomKeyBytes.length);
                    throw new IllegalArgumentException("tampered signed text");
                }
                SecretKey randomKey = new SecretKeySpec(randomKeyBytes, KEY_ALGO);
                signingKey = createSigningKey(systemKey, randomKey);
            } catch (IOException e) {
                logger.error("error occurred while decoding key data", e);
                throw new IllegalStateException("error while verifying the signed text");
            }
        }

        try {
            String sig = signInternal(text, signingKey);
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
            logger.warn("encrypt called without a key, returning plain text. run syskeygen and copy same key to all nodes to enable " +
                    "encryption");
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
            logger.warn("encrypt called without a key, returning plain text. run syskeygen and copy same key to all nodes to enable " +
                    "encryption");
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
                signingKey = createSigningKey(systemKey, randomKey);
                try {
                    encryptionKey = encryptionKey(signingKey, keyLength, keyAlgorithm);
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
                signingKey = createSigningKey(systemKey, randomKey);

                callListeners(oldSystemKey, oldEncryptionKey);
            }
        }

        @Override
        public void onFileChanged(Path file) {
            if (file.equals(keyFile)) {
                final SecretKey oldSystemKey = systemKey;
                final SecretKey oldEncryptionKey = encryptionKey;

                logger.warn("system key file changed!");
                SecretKey systemKey = readSystemKey(file);
                signingKey = createSigningKey(systemKey, randomKey);
                try {
                    encryptionKey = encryptionKey(signingKey, keyLength, keyAlgorithm);
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

    /**
     * Simplified implementation of HKDF using the HmacSHA1 algortihm.
     *
     * @see <a href=https://tools.ietf.org/html/rfc5869>RFC 5869</a>
     */
    private static class HmacSHA1HKDF {
        private static final int HMAC_SHA1_BYTE_LENGTH = 20;
        private static final String HMAC_SHA1_ALGORITHM = "HmacSHA1";

        /**
         * This method performs the <code>extract</code> and <code>expand</code> steps of HKDF in one call with the given
         * data. The output of the extract step is used as the input to the expand step
         *
         * @param salt         optional salt value (a non-secret random value); if not provided, it is set to a string of HashLen zeros.
         * @param ikm          the input keying material
         * @param info         optional context and application specific information; if not provided a zero length byte[] is used
         * @param outputLength length of output keying material in octets (&lt;= 255*HashLen)
         * @return the output keying material
         */
        static byte[] extractAndExpand(@Nullable SecretKey salt, byte[] ikm, @Nullable byte[] info, int outputLength) {
            // arg checking
            Objects.requireNonNull(ikm, "the input keying material must not be null");
            if (outputLength < 1) {
                throw new IllegalArgumentException("output length must be positive int >= 1");
            }
            if (outputLength > 255 * HMAC_SHA1_BYTE_LENGTH) {
                throw new IllegalArgumentException("output length must be <= 255*" + HMAC_SHA1_BYTE_LENGTH);
            }
            if (salt == null) {
                salt = new SecretKeySpec(new byte[HMAC_SHA1_BYTE_LENGTH], HMAC_SHA1_ALGORITHM);
            }
            if (info == null) {
                info = new byte[0];
            }

            // extract
            Mac mac = createMac(salt);
            byte[] keyBytes = mac.doFinal(ikm);
            final SecretKey pseudoRandomKey = new SecretKeySpec(keyBytes, HMAC_SHA1_ALGORITHM);

            /*
             * The output OKM is calculated as follows:
             * N = ceil(L/HashLen)
             * T = T(1) | T(2) | T(3) | ... | T(N)
             * OKM = first L octets of T
             *
             * where:
             * T(0) = empty string (zero length)
             * T(1) = HMAC-Hash(PRK, T(0) | info | 0x01)
             * T(2) = HMAC-Hash(PRK, T(1) | info | 0x02)
             * T(3) = HMAC-Hash(PRK, T(2) | info | 0x03)
             * ...
             *
             * (where the constant concatenated to the end of each T(n) is a single octet.)
             */
            int n = (outputLength % HMAC_SHA1_BYTE_LENGTH == 0) ?
                    outputLength / HMAC_SHA1_BYTE_LENGTH :
                    (outputLength / HMAC_SHA1_BYTE_LENGTH) + 1;

            byte[] hashRound = new byte[0];

            ByteBuffer generatedBytes = ByteBuffer.allocate(Math.multiplyExact(n, HMAC_SHA1_BYTE_LENGTH));
            try {
                // initiliaze the mac with the new key
                mac.init(pseudoRandomKey);
            } catch (InvalidKeyException e) {
                throw new ElasticsearchException("failed to initialize the mac", e);
            }
            for (int roundNum = 1; roundNum <= n; roundNum++) {
                mac.reset();
                mac.update(hashRound);
                mac.update(info);
                mac.update((byte) roundNum);
                hashRound = mac.doFinal();
                generatedBytes.put(hashRound);
            }

            byte[] result = new byte[outputLength];
            generatedBytes.rewind();
            generatedBytes.get(result, 0, outputLength);
            return result;
        }
    }

    public static void registerSettings(SettingsModule settingsModule) {
        settingsModule.registerSetting(FILE_SETTING);
        settingsModule.registerSetting(ENCRYPTION_KEY_LENGTH_SETTING);
        settingsModule.registerSetting(ENCRYPTION_KEY_ALGO_SETTING);
        settingsModule.registerSetting(ENCRYPTION_ALGO_SETTING);
    }
}
