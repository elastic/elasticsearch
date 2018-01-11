/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.settings;

import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import javax.security.auth.DestroyFailedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStore.PasswordProtection;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.spec.InvalidKeySpecException;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;

/**
 * A wrapper around a Java KeyStore which provides supplements the keystore with extra metadata.
 *
 * Loading a keystore has 2 phases. First, call {@link #load(Path)}. Then call
 * {@link #decrypt(char[])} with the keystore password, or an empty char array if
 * {@link #hasPassword()} is {@code false}.  Loading and decrypting should happen
 * in a single thread. Once decrypted, keys may be read with the wrapper in
 * multiple threads.
 */
public class KeyStoreWrapper implements SecureSettings {

    /** Pattern for the valid characters of any setting name. */
    private static final Pattern ALLOWED_SETTING_NAME = Pattern.compile("[a-z0-9_\\-.]+");

    /** Setting accessor for the seed setting. */
    public static final Setting<SecureString> SEED_SETTING = SecureSetting.secureString("keystore.seed", null);

    /**
     * Character set for the value of the bootstrap seed setting.
     */
    private static final char[] SEED_CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789".toCharArray();

    /** Type of entries. */
    private enum KeyType {
        STRING,
        FILE
    }

    /** Hardcoded name of the keystore inside the config directory. */
    private static final String KEYSTORE_FILENAME = "elasticsearch.keystore";

    /** Newest metadata format version. */
    private static final int FORMAT_VERSION = 2;

    /** Oldest metadata format version. */
    private static final int MIN_FORMAT_VERSION = 1;

    /** The keystore type for a newly created keystore. */
    private static final String NEW_KEYSTORE_TYPE = "PKCS12";

    /** The algorithm used to store string setting contents. */
    private static final String NEW_KEYSTORE_STRING_KEY_ALGO = "PBE";

    /** The algorithm used to store file setting contents. */
    private static final String NEW_KEYSTORE_FILE_KEY_ALGO = "PBE";

    /** An encoder to check whether string values are ascii. */
    private static final CharsetEncoder ASCII_ENCODER = StandardCharsets.US_ASCII.newEncoder();

    /** The metadata format version of the current keystore wrapper. */
    private final int formatVersion;

    /** True iff the keystore requires non-empty password to unlock. */
    private final boolean hasPassword;

    /** The type of the keystore, as passed to {@link java.security.KeyStore#getInstance(String)} */
    private final String type;

    /**
     * The factory for constructing instances of string secrets in a
     * {@link KeyStore}.
     */
    private final SecretKeyFactory stringFactory;

    /**
     * The factory for constructing instances of file secrets in a {@link KeyStore}.
     */
    private final SecretKeyFactory fileFactory;

    /** The settings that exist in the keystore, mapped to their type of data. */
    private final Map<String, KeyType> settingTypes;

    /** The position in file of the encrypted keystore. */
    private final long keystoreBytesPos;

    /** The directory where the keystore is saved */
    private final Path configDir;

    /** Present iff secret settings are unlocked see {@link #unlock(char[])}. */
    private Optional<Tuple<KeyStore, KeyStore.PasswordProtection>> keystore;

    private KeyStoreWrapper(int formatVersion, boolean hasPassword, String type, String stringKeyAlgo, String fileKeyAlgo,
            Map<String, KeyType> settingTypes, long keystoreBytesPos, Path configDir,
            Optional<Tuple<KeyStore, KeyStore.PasswordProtection>> keystore) throws NoSuchAlgorithmException {
        this.formatVersion = formatVersion;
        this.hasPassword = hasPassword;
        this.type = type;
        this.stringFactory = SecretKeyFactory.getInstance(stringKeyAlgo);
        this.fileFactory = SecretKeyFactory.getInstance(fileKeyAlgo);
        this.settingTypes = settingTypes;
        this.keystoreBytesPos = keystoreBytesPos;
        this.configDir = configDir;
        this.keystore = keystore;
    }

    private KeyStoreWrapper(KeyStoreWrapper other) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException {
        this.formatVersion = other.formatVersion;
        this.hasPassword = other.hasPassword;
        this.type = other.type;
        this.stringFactory = other.stringFactory;
        this.fileFactory = other.fileFactory;
        this.settingTypes = new HashMap<>(other.settingTypes);
        this.keystoreBytesPos = other.keystoreBytesPos;
        this.configDir = other.configDir;
        this.keystore = Optional.empty();
        if (other.keystore.isPresent()) {
            // clone the keystore and password
            final ByteArrayOutputStream baos = new ByteArrayOutputStream();
            final char[] pass = other.keystore.get().v2().getPassword();
            // copy keystore to temporary buffer
            other.keystore.get().v1().store(baos, pass);
            final byte[] temp = baos.toByteArray();
            try (ByteArrayInputStream bais = new ByteArrayInputStream(temp)) {
                final KeyStore newKeyStore = KeyStore.getInstance(type);
                newKeyStore.load(bais, pass);
                this.keystore = Optional.of(new Tuple<>(newKeyStore, new PasswordProtection(pass)));
            } finally {
                Arrays.fill(temp, (byte) 0);
                baos.reset();
                baos.write(temp, 0, temp.length);
            }
        }
    }

    /**
     * Reads metadata information about the Elasticsearch keystore from the provided
     * config directory. The returned keystore is locked. To read the concrete
     * contents of the keystore use {@link #unlock(char[])}. The keystore entry set
     * cannot be changed. Use {@link Builder} to add and remove entries to a
     * keystore.
     *
     * {@link #unlock(char[])} MUST be called before reading any entries.
     * {@link #lock(char[])} SHOULD be called immediately after all required entries
     * have been read.
     */
    public static Optional<KeyStoreWrapper> load(Path configDir) throws IOException, NoSuchAlgorithmException {
        final Path keystoreFile = keystorePath(configDir);
        if (Files.exists(keystoreFile) == false) {
            return Optional.empty();
        }
        try (SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
                IndexInput indexInput = directory.openInput(KEYSTORE_FILENAME, IOContext.READONCE);
                ChecksumIndexInput input = new BufferedChecksumIndexInput(indexInput)) {
            final int formatVersion = CodecUtil.checkHeader(input, KEYSTORE_FILENAME, MIN_FORMAT_VERSION, FORMAT_VERSION);
            final byte hasPasswordByte = input.readByte();
            if (hasPasswordByte != 1 && hasPasswordByte != 0) {
                throw new IllegalStateException("hasPassword boolean is corrupt: " + String.format(Locale.ROOT, "%02x", hasPasswordByte));
            }
            final String type = input.readString();
            final String stringKeyAlgo = input.readString();
            final String fileKeyAlgo;
            if (formatVersion >= FORMAT_VERSION) {
                fileKeyAlgo = input.readString();
            } else {
                fileKeyAlgo = NEW_KEYSTORE_FILE_KEY_ALGO;
            }
            // version 1 does not include metadata about the entries
            // the keystore HAS to be upgraded and reloaded
            final Map<String, KeyType> settingTypes;
            if (formatVersion >= FORMAT_VERSION) {
                // entries name and type will not change
                settingTypes = Collections.unmodifiableMap(input.readMapOfStrings().entrySet().stream().collect(
                        Collectors.toMap(Map.Entry::getKey, e -> KeyType.valueOf(e.getValue()))));
            } else {
                settingTypes = new HashMap<>();
            }
            final long pos = input.getFilePointer();
            final int off = input.readInt();
            // skip reading the keystore content, but update checksum
            input.seek(input.getFilePointer() + off);
            CodecUtil.checkFooter(input);
            return Optional.of(new KeyStoreWrapper(formatVersion, hasPasswordByte == 1, type, stringKeyAlgo, fileKeyAlgo, settingTypes, pos,
                    configDir, Optional.empty() /* keystore locked */));
        }
    }

    /**
     * Return true iff calling {@link #unlock(char[])} requires a non-empty
     * password.
     */
    public boolean hasPassword() {
        return hasPassword;
    }

    /**
     * Names of all setting entries. These WILL not change in between
     * {@link #lock()} and {@link #unlock(char[])} calls.
     */
    @Override
    public Set<String> getSettingNames() {
        return settingTypes.keySet();
    }

    /**
     * True iff the last {@link #unlock(char[])} call has not been followed by any
     * {@link #lock()} call yet.
     */
    @Override
    public boolean isUnlocked() {
        return keystore.isPresent();
    }

    @Override
    public SecureString getString(String name) throws GeneralSecurityException {
        if (isUnlocked() == false) {
            throw new IllegalStateException("Secure setting is locked: getString called before unlock.");
        }
        final KeyStore.Entry entry = keystore.get().v1().getEntry(name, keystore.get().v2());
        if (settingTypes.get(name) != KeyType.STRING || entry instanceof KeyStore.SecretKeyEntry == false) {
            throw new IllegalArgumentException("Secret setting " + name + " is missing or is not a string");
        }
        final KeyStore.SecretKeyEntry secretKeyEntry = (KeyStore.SecretKeyEntry) entry;
        final PBEKeySpec keySpec = (PBEKeySpec) stringFactory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);
        final SecureString value = new SecureString(keySpec.getPassword());
        keySpec.clearPassword();
        return value;
    }

    @Override
    public InputStream getFile(String name) throws GeneralSecurityException {
        if (isUnlocked() == false) {
            throw new IllegalStateException("Secret setting is locked: getFile called before unlock.");
        }
        final KeyStore.Entry entry = keystore.get().v1().getEntry(name, keystore.get().v2());
        if (settingTypes.get(name) != KeyType.FILE || entry instanceof KeyStore.SecretKeyEntry == false) {
            throw new IllegalArgumentException("Secret setting " + name + " is missing or is not a file");
        }
        final KeyStore.SecretKeyEntry secretKeyEntry = (KeyStore.SecretKeyEntry) entry;
        final PBEKeySpec keySpec = (PBEKeySpec) fileFactory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);
        // The PBE keyspec gives us chars, we first convert to bytes, then decode base64 inline.
        final char[] chars = keySpec.getPassword();
        final byte[] bytes = new byte[chars.length];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte)chars[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
        }
        keySpec.clearPassword(); // wipe the original copy
        final InputStream bytesStream = new ByteArrayInputStream(bytes) {
            @Override
            public void close() throws IOException {
                Arrays.fill(bytes, (byte) 0); // wipe our second copy when the stream is exhausted
                super.close();
            }
        };
        return Base64.getDecoder().wrap(bytesStream);
    }

    /**
     * Reads and decrypts the underlying java keystore from disk.
     *
     * This has to be called before accessing settings with {@link #getFile(String)}
     * and {@link #getString(String)}. The provided password will be cleared, but a
     * clone will be retained internally for decrypting individual entries upon
     * request.
     */
    @Override
    public AutoCloseable unlock(char[] password) throws GeneralSecurityException, IOException {
        if (isUnlocked()) {
            // unlock is idempotent
            Arrays.fill(password, (char) 0);
            return this;
        }
        try (SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
                IndexInput indexInput = directory.openInput(KEYSTORE_FILENAME, IOContext.READONCE);
                ChecksumIndexInput input = new BufferedChecksumIndexInput(indexInput)) {
            // seek to the encrypted keystore position
            input.seek(keystoreBytesPos);
            final int keyStoreByteSize = input.readInt();
            final byte[] keyStoreBytes = new byte[keyStoreByteSize];
            // read encrypted keystore from disk
            input.readBytes(keyStoreBytes, 0, keyStoreByteSize);
            final KeyStore newKeystore = KeyStore.getInstance(type);
            // load buffer in a keystore
            try (InputStream in = new ByteArrayInputStream(keyStoreBytes)) {
                newKeystore.load(in, password);
            } finally {
                Arrays.fill(keyStoreBytes, (byte) 0);
            }
            CodecUtil.checkFooter(input);
            final Enumeration<String> aliases = keystore.get().v1().aliases();
            if (formatVersion == 1) {
                // populate entries metadata
                // version 1 does not store metadata about entries
                // the keystore SHOULD be updated
                while (aliases.hasMoreElements()) {
                    settingTypes.put(aliases.nextElement(), KeyType.STRING);
                }
            } else {
                // verify integrity: check if aliases in keystore mirror entries metadata
                final Set<String> expectedSettings = new HashSet<>(settingTypes.keySet());
                while (aliases.hasMoreElements()) {
                    final String settingName = aliases.nextElement();
                    if (expectedSettings.remove(settingName) == false) {
                        throw new SecurityException("Keystore has been corrupted or tampered with");
                    }
                }
                if (expectedSettings.isEmpty() == false) {
                    throw new SecurityException("Keystore has been corrupted or tampered with");
                }
            }
            // store password for entries access later
            keystore = Optional.of(new Tuple<>(newKeystore, new KeyStore.PasswordProtection(password)));
        } finally {
            Arrays.fill(password, (char) 0);
        }
        return this;
    }

    @Override
    public void lock() {
        if (isUnlocked() == false) {
            // lock is idempotent
            return;
        }
        try {
            keystore.get().v2().destroy();
        } catch (final DestroyFailedException e) {
            // we don't expect the user to handle lock failures, too cumbersome
            throw new RuntimeException(e);
        }
        keystore = Optional.empty();
    }

    @Override
    public void close() {
        lock();
    }

    /** Validates the given setting name. */
    public static void validateSettingName(String setting) {
        if (ALLOWED_SETTING_NAME.matcher(setting).matches() == false) {
            throw new IllegalArgumentException("Setting name [" + setting + "] does not match the allowed setting name pattern ["
                    + ALLOWED_SETTING_NAME.pattern() + "]");
        }
    }

    /** Returns a path representing the ES keystore in the given config dir. */
    public static Path keystorePath(Path configDir) {
        return configDir.resolve(KEYSTORE_FILENAME);
    }

    public boolean requiresUpdate() {
        return formatVersion < FORMAT_VERSION || getSettingNames().contains(SEED_SETTING.getKey()) == false;
    }

    public static Builder builder(KeyStoreWrapper unlockedKeyStore) throws Exception {
        return new Builder(unlockedKeyStore);
    }


    public static Builder builder(char[] password) throws Exception {
        return new Builder(password);
    }

    public static class Builder implements AutoCloseable {

        /**
         * Unlocked keystore not exposed to lock, unlock and get operations. Used
         * specifically for editing settings.
         */
        private final KeyStoreWrapper unlockedKeyStore;

        /**
         * The passed Keystore will be cloned. The passed Keystore may be used further.
         */
        Builder(KeyStoreWrapper unlockedKeyStore) throws IOException, GeneralSecurityException {
            if (unlockedKeyStore.isUnlocked() == false) {
                throw new IllegalArgumentException("KeyStore must be unlocked before adding new entries.");
            }
            // clone the the keystore
            this.unlockedKeyStore = new KeyStoreWrapper(unlockedKeyStore);
            // update keystore automatically
            ensureBootstrapSeed();
        }

        /**
         * Constructs a new keystore with the given password. Password will be cleared.
         */
        Builder(char[] password) throws IOException, GeneralSecurityException {
            try {
                final KeyStore emptyKeystore = KeyStore.getInstance(NEW_KEYSTORE_TYPE);
                emptyKeystore.load(null, null);
                final Map<String, KeyType> settingTypes = new HashMap<>();
                // store password for later (entry set and save)
                final Optional<Tuple<KeyStore, KeyStore.PasswordProtection>> keystore = Optional
                        .of(new Tuple<>(emptyKeystore, new KeyStore.PasswordProtection(password)));
                this.unlockedKeyStore = new KeyStoreWrapper(FORMAT_VERSION, password.length != 0, NEW_KEYSTORE_TYPE,
                        NEW_KEYSTORE_STRING_KEY_ALGO, NEW_KEYSTORE_FILE_KEY_ALGO, settingTypes, -1, null, keystore);
            } finally {
                Arrays.fill(password, (char) 0);
            }
            // update keystore automatically
            ensureBootstrapSeed();
        }

        /** Set a string setting. */
        Builder setString(String name, char[] value) throws InvalidKeySpecException, KeyStoreException {
            validateSettingName(name);
            if (ASCII_ENCODER.canEncode(CharBuffer.wrap(value)) == false) {
                throw new IllegalArgumentException("Setting value must be ascii");
            }
            final SecretKey secretKey = unlockedKeyStore.stringFactory.generateSecret(new PBEKeySpec(value));
            unlockedKeyStore.keystore.get().v1().setEntry(name, new KeyStore.SecretKeyEntry(secretKey),
                    unlockedKeyStore.keystore.get().v2());
            unlockedKeyStore.settingTypes.put(name, KeyType.STRING);
            return this;
        }

        /** Set a file setting. */
        Builder setFile(String name, byte[] bytes) throws InvalidKeySpecException, KeyStoreException {
            validateSettingName(name);
            final byte[] bytesB64 = Base64.getEncoder().encode(bytes);
            final char[] chars = new char[bytesB64.length];
            for (int i = 0; i < chars.length; ++i) {
                chars[i] = (char) bytesB64[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
            }
            final SecretKey secretKey = unlockedKeyStore.stringFactory.generateSecret(new PBEKeySpec(chars));
            unlockedKeyStore.keystore.get().v1().setEntry(name, new KeyStore.SecretKeyEntry(secretKey),
                    unlockedKeyStore.keystore.get().v2());
            unlockedKeyStore.settingTypes.put(name, KeyType.FILE);
            return this;
        }

        /** Remove the given setting from the keystore. */
        Builder remove(String name) throws KeyStoreException {
            unlockedKeyStore.settingTypes.remove(name);
            unlockedKeyStore.keystore.get().v1().deleteEntry(name);
            return this;
        }

        /**
         * Overwrite the keystore to the given config directory.
         */
        public void save(Path configDir)
                throws IOException, KeyStoreException, NoSuchAlgorithmException, CertificateException, UserException {
            final char[] password = unlockedKeyStore.keystore.get().v2().getPassword();
            // write to tmp file first, then overwrite
            final String tmpFile = KEYSTORE_FILENAME + ".tmp";
            try (SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
                    IndexOutput output = directory.createOutput(tmpFile, IOContext.DEFAULT)) {
                CodecUtil.writeHeader(output, KEYSTORE_FILENAME, FORMAT_VERSION);
                output.writeByte(password.length == 0 ? (byte) 0 : (byte) 1);
                output.writeString(unlockedKeyStore.type);
                output.writeString(unlockedKeyStore.stringFactory.getAlgorithm());
                output.writeString(unlockedKeyStore.fileFactory.getAlgorithm());
                output.writeMapOfStrings(unlockedKeyStore.settingTypes.entrySet().stream().collect(
                        Collectors.toMap(Map.Entry::getKey, e -> e.getValue().name())));
                // write encrypted keystore
                final ByteArrayOutputStream baos = new ByteArrayOutputStream();
                unlockedKeyStore.keystore.get().v1().store(baos, password);
                final byte[] keystoreBytes = baos.toByteArray();
                try {
                    output.writeInt(keystoreBytes.length);
                    output.writeBytes(keystoreBytes, keystoreBytes.length);
                } finally {
                    Arrays.fill(keystoreBytes, (byte) 0);
                    baos.reset();
                    baos.write(keystoreBytes, 0, keystoreBytes.length);
                }
                CodecUtil.writeFooter(output);
            } catch (final AccessDeniedException e) {
                final String message = String.format(Locale.ROOT,
                        "unable to create temporary keystore at [%s], please check filesystem permissions", configDir.resolve(tmpFile));
                throw new UserException(ExitCodes.CONFIG, message, e);
            }
            // move
            final Path keystoreFile = keystorePath(configDir);
            Files.move(configDir.resolve(tmpFile), keystoreFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            final PosixFileAttributeView attrs = Files.getFileAttributeView(keystoreFile, PosixFileAttributeView.class);
            if (attrs != null) {
                // don't rely on umask: ensure the keystore has minimal permissions
                attrs.setPermissions(PosixFilePermissions.fromString("rw-rw----"));
            }
        }

        private static char[] bootrapSeed() {
            final SecureRandom random = Randomness.createSecure();
            final int passwordLength = 22; // Generate 22 character passwords
            final char[] characters = new char[passwordLength];
            for (int i = 0; i < passwordLength; ++i) {
                characters[i] = SEED_CHARS[random.nextInt(SEED_CHARS.length)];
            }
            return characters;
        }

        /**
         * Add the bootstrap seed setting, which may be used as a unique, secure, random
         * value by the node
         */
        private void ensureBootstrapSeed() throws GeneralSecurityException {
            if (unlockedKeyStore.getSettingNames().contains(SEED_SETTING.getKey())) {
                return;
            }
            final char[] seed = bootrapSeed();
            try {
                setString(SEED_SETTING.getKey(), seed);
            } catch (InvalidKeySpecException | KeyStoreException e) {
                close();
                throw e;
            } finally {
                Arrays.fill(seed, (char) 0);
            }
        }

        @Override
        public void close() {
            this.unlockedKeyStore.close();
        }
    }

}
