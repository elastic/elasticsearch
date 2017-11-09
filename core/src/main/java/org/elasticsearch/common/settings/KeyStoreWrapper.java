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
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Base64;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.bootstrap.BootstrapSettings;
import org.elasticsearch.cli.ExitCodes;
import org.elasticsearch.cli.UserException;
import org.elasticsearch.common.Randomness;

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

    public static final Setting<SecureString> SEED_SETTING = SecureSetting.secureString("keystore.seed", null);

    /** Characters that may be used in the bootstrap seed setting added to all keystores. */
    private static final char[] SEED_CHARS = ("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789" +
        "~!@#$%^&*-_=+?").toCharArray();

    /** An identifier for the type of data that may be stored in a keystore entry. */
    private enum KeyType {
        STRING,
        FILE
    }

    /** The name of the keystore file to read and write. */
    private static final String KEYSTORE_FILENAME = "elasticsearch.keystore";

    /** The version of the metadata written before the keystore data. */
    private static final int FORMAT_VERSION = 2;

    /** The oldest metadata format version that can be read. */
    private static final int MIN_FORMAT_VERSION = 1;

    /** The keystore type for a newly created keystore. */
    private static final String NEW_KEYSTORE_TYPE = "PKCS12";

    /** The algorithm used to store string setting contents. */
    private static final String NEW_KEYSTORE_STRING_KEY_ALGO = "PBE";

    /** The algorithm used to store file setting contents. */
    private static final String NEW_KEYSTORE_FILE_KEY_ALGO = "PBE";

    /** An encoder to check whether string values are ascii. */
    private static final CharsetEncoder ASCII_ENCODER = StandardCharsets.US_ASCII.newEncoder();

    /** The metadata format version used to read the current keystore wrapper. */
    private final int formatVersion;

    /** True iff the keystore has a password needed to read. */
    private final boolean hasPassword;

    /** The type of the keystore, as passed to {@link java.security.KeyStore#getInstance(String)} */
    private final String type;

    /** A factory necessary for constructing instances of string secrets in a {@link KeyStore}. */
    private final SecretKeyFactory stringFactory;

    /** A factory necessary for constructing instances of file secrets in a {@link KeyStore}. */
    private final SecretKeyFactory fileFactory;

    /**
     * The settings that exist in the keystore, mapped to their type of data.
     */
    private final Map<String, KeyType> settingTypes;

    /** The raw bytes of the encrypted keystore. */
    private final byte[] keystoreBytes;

    /** The loaded keystore. See {@link #decrypt(char[])}. */
    private final SetOnce<KeyStore> keystore = new SetOnce<>();

    /** The password for the keystore. See {@link #decrypt(char[])}. */
    private final SetOnce<KeyStore.PasswordProtection> keystorePassword = new SetOnce<>();

    private KeyStoreWrapper(int formatVersion, boolean hasPassword, String type,
                            String stringKeyAlgo, String fileKeyAlgo,
                            Map<String, KeyType> settingTypes, byte[] keystoreBytes) {
        this.formatVersion = formatVersion;
        this.hasPassword = hasPassword;
        this.type = type;
        try {
            stringFactory = SecretKeyFactory.getInstance(stringKeyAlgo);
            fileFactory = SecretKeyFactory.getInstance(fileKeyAlgo);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        this.settingTypes = settingTypes;
        this.keystoreBytes = keystoreBytes;
    }

    /** Returns a path representing the ES keystore in the given config dir. */
    public static Path keystorePath(Path configDir) {
        return configDir.resolve(KEYSTORE_FILENAME);
    }

    /** Constructs a new keystore with the given password. */
    public static KeyStoreWrapper create(char[] password) throws Exception {
        KeyStoreWrapper wrapper = new KeyStoreWrapper(FORMAT_VERSION, password.length != 0, NEW_KEYSTORE_TYPE,
            NEW_KEYSTORE_STRING_KEY_ALGO, NEW_KEYSTORE_FILE_KEY_ALGO, new HashMap<>(), null);
        KeyStore keyStore = KeyStore.getInstance(NEW_KEYSTORE_TYPE);
        keyStore.load(null, null);
        wrapper.keystore.set(keyStore);
        wrapper.keystorePassword.set(new KeyStore.PasswordProtection(password));
        addBootstrapSeed(wrapper);
        return wrapper;
    }

    /** Add the bootstrap seed setting, which may be used as a unique, secure, random value by the node */
    public static void addBootstrapSeed(KeyStoreWrapper wrapper) throws GeneralSecurityException {
        assert wrapper.getSettingNames().contains(SEED_SETTING.getKey()) == false;
        SecureRandom random = Randomness.createSecure();
        int passwordLength = 20; // Generate 20 character passwords
        char[] characters = new char[passwordLength];
        for (int i = 0; i < passwordLength; ++i) {
            characters[i] = SEED_CHARS[random.nextInt(SEED_CHARS.length)];
        }
        wrapper.setString(SEED_SETTING.getKey(), characters);
        Arrays.fill(characters, (char)0);
    }

    /**
     * Loads information about the Elasticsearch keystore from the provided config directory.
     *
     * {@link #decrypt(char[])} must be called before reading or writing any entries.
     * Returns {@code null} if no keystore exists.
     */
    public static KeyStoreWrapper load(Path configDir) throws IOException {
        Path keystoreFile = keystorePath(configDir);
        if (Files.exists(keystoreFile) == false) {
            return null;
        }

        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        try (IndexInput indexInput = directory.openInput(KEYSTORE_FILENAME, IOContext.READONCE)) {
            ChecksumIndexInput input = new BufferedChecksumIndexInput(indexInput);
            int formatVersion = CodecUtil.checkHeader(input, KEYSTORE_FILENAME, MIN_FORMAT_VERSION, FORMAT_VERSION);
            byte hasPasswordByte = input.readByte();
            boolean hasPassword = hasPasswordByte == 1;
            if (hasPassword == false && hasPasswordByte != 0) {
                throw new IllegalStateException("hasPassword boolean is corrupt: "
                    + String.format(Locale.ROOT, "%02x", hasPasswordByte));
            }
            String type = input.readString();
            String stringKeyAlgo = input.readString();
            final String fileKeyAlgo;
            if (formatVersion >= 2) {
                fileKeyAlgo = input.readString();
            } else {
                fileKeyAlgo = NEW_KEYSTORE_FILE_KEY_ALGO;
            }
            final Map<String, KeyType> settingTypes;
            if (formatVersion >= 2) {
                settingTypes = input.readMapOfStrings().entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> KeyType.valueOf(e.getValue())));
            } else {
                settingTypes = new HashMap<>();
            }
            byte[] keystoreBytes = new byte[input.readInt()];
            input.readBytes(keystoreBytes, 0, keystoreBytes.length);
            CodecUtil.checkFooter(input);
            return new KeyStoreWrapper(formatVersion, hasPassword, type, stringKeyAlgo, fileKeyAlgo, settingTypes, keystoreBytes);
        }
    }

    /** Upgrades the format of the keystore, if necessary. */
    public static void upgrade(KeyStoreWrapper wrapper, Path configDir) throws Exception {
        // ensure keystore.seed exists
        if (wrapper.getSettingNames().contains(SEED_SETTING.getKey())) {
            return;
        }
        addBootstrapSeed(wrapper);
        wrapper.save(configDir);
    }

    @Override
    public boolean isLoaded() {
        return keystore.get() != null;
    }

    /** Return true iff calling {@link #decrypt(char[])} requires a non-empty password. */
    public boolean hasPassword() {
        return hasPassword;
    }

    /**
     * Decrypts the underlying java keystore.
     *
     * This may only be called once. The provided password will be zeroed out.
     */
    public void decrypt(char[] password) throws GeneralSecurityException, IOException {
        if (keystore.get() != null) {
            throw new IllegalStateException("Keystore has already been decrypted");
        }
        keystore.set(KeyStore.getInstance(type));
        try (InputStream in = new ByteArrayInputStream(keystoreBytes)) {
            keystore.get().load(in, password);
        } finally {
            Arrays.fill(keystoreBytes, (byte)0);
        }
        keystorePassword.set(new KeyStore.PasswordProtection(password));
        Arrays.fill(password, '\0');

        Enumeration<String> aliases = keystore.get().aliases();
        if (formatVersion == 1) {
            while (aliases.hasMoreElements()) {
                settingTypes.put(aliases.nextElement(), KeyType.STRING);
            }
        } else {
            // verify integrity: keys in keystore match what the metadata thinks exist
            Set<String> expectedSettings = new HashSet<>(settingTypes.keySet());
            while (aliases.hasMoreElements()) {
                String settingName = aliases.nextElement();
                if (expectedSettings.remove(settingName) == false) {
                    throw new SecurityException("Keystore has been corrupted or tampered with");
                }
            }
            if (expectedSettings.isEmpty() == false) {
                throw new SecurityException("Keystore has been corrupted or tampered with");
            }
        }
    }

    /** Write the keystore to the given config directory. */
    public void save(Path configDir) throws Exception {
        assert isLoaded();
        char[] password = this.keystorePassword.get().getPassword();

        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        // write to tmp file first, then overwrite
        String tmpFile = KEYSTORE_FILENAME + ".tmp";
        try (IndexOutput output = directory.createOutput(tmpFile, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, KEYSTORE_FILENAME, FORMAT_VERSION);
            output.writeByte(password.length == 0 ? (byte)0 : (byte)1);
            output.writeString(NEW_KEYSTORE_TYPE);
            output.writeString(NEW_KEYSTORE_STRING_KEY_ALGO);
            output.writeString(NEW_KEYSTORE_FILE_KEY_ALGO);
            output.writeMapOfStrings(settingTypes.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                e -> e.getValue().name())));

            // TODO: in the future if we ever change any algorithms used above, we need
            // to create a new KeyStore here instead of using the existing one, so that
            // the encoded material inside the keystore is updated
            assert type.equals(NEW_KEYSTORE_TYPE) : "keystore type changed";
            assert stringFactory.getAlgorithm().equals(NEW_KEYSTORE_STRING_KEY_ALGO) : "string pbe algo changed";
            assert fileFactory.getAlgorithm().equals(NEW_KEYSTORE_FILE_KEY_ALGO) : "file pbe algo changed";

            ByteArrayOutputStream keystoreBytesStream = new ByteArrayOutputStream();
            keystore.get().store(keystoreBytesStream, password);
            byte[] keystoreBytes = keystoreBytesStream.toByteArray();
            output.writeInt(keystoreBytes.length);
            output.writeBytes(keystoreBytes, keystoreBytes.length);
            CodecUtil.writeFooter(output);
        } catch (final AccessDeniedException e) {
            final String message = String.format(
                    Locale.ROOT,
                    "unable to create temporary keystore at [%s], please check filesystem permissions",
                    configDir.resolve(tmpFile));
            throw new UserException(ExitCodes.CONFIG, message, e);
        }

        Path keystoreFile = keystorePath(configDir);
        Files.move(configDir.resolve(tmpFile), keystoreFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        PosixFileAttributeView attrs = Files.getFileAttributeView(keystoreFile, PosixFileAttributeView.class);
        if (attrs != null) {
            // don't rely on umask: ensure the keystore has minimal permissions
            attrs.setPermissions(PosixFilePermissions.fromString("rw-rw----"));
        }
    }

    @Override
    public Set<String> getSettingNames() {
        return settingTypes.keySet();
    }

    // TODO: make settings accessible only to code that registered the setting
    @Override
    public SecureString getString(String setting) throws GeneralSecurityException {
        assert isLoaded();
        KeyStore.Entry entry = keystore.get().getEntry(setting, keystorePassword.get());
        if (settingTypes.get(setting) != KeyType.STRING ||
            entry instanceof KeyStore.SecretKeyEntry == false) {
            throw new IllegalStateException("Secret setting " + setting + " is not a string");
        }
        // TODO: only allow getting a setting once?
        KeyStore.SecretKeyEntry secretKeyEntry = (KeyStore.SecretKeyEntry) entry;
        PBEKeySpec keySpec = (PBEKeySpec) stringFactory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);
        SecureString value = new SecureString(keySpec.getPassword());
        keySpec.clearPassword();
        return value;
    }

    @Override
    public InputStream getFile(String setting) throws GeneralSecurityException {
        assert isLoaded();
        KeyStore.Entry entry = keystore.get().getEntry(setting, keystorePassword.get());
        if (settingTypes.get(setting) != KeyType.FILE ||
            entry instanceof KeyStore.SecretKeyEntry == false) {
            throw new IllegalStateException("Secret setting " + setting + " is not a file");
        }
        KeyStore.SecretKeyEntry secretKeyEntry = (KeyStore.SecretKeyEntry) entry;
        PBEKeySpec keySpec = (PBEKeySpec) fileFactory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);
        // The PBE keyspec gives us chars, we first convert to bytes, then decode base64 inline.
        char[] chars = keySpec.getPassword();
        byte[] bytes = new byte[chars.length];
        for (int i = 0; i < bytes.length; ++i) {
            bytes[i] = (byte)chars[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
        }
        keySpec.clearPassword(); // wipe the original copy
        InputStream bytesStream = new ByteArrayInputStream(bytes) {
            @Override
            public void close() throws IOException {
                super.close();
                Arrays.fill(bytes, (byte)0); // wipe our second copy when the stream is exhausted
            }
        };
        return Base64.getDecoder().wrap(bytesStream);
    }

    /**
     * Set a string setting.
     *
     * @throws IllegalArgumentException if the value is not ASCII
     */
    void setString(String setting, char[] value) throws GeneralSecurityException {
        assert isLoaded();
        if (ASCII_ENCODER.canEncode(CharBuffer.wrap(value)) == false) {
            throw new IllegalArgumentException("Value must be ascii");
        }
        SecretKey secretKey = stringFactory.generateSecret(new PBEKeySpec(value));
        keystore.get().setEntry(setting, new KeyStore.SecretKeyEntry(secretKey), keystorePassword.get());
        settingTypes.put(setting, KeyType.STRING);
    }

    /** Set a file setting. */
    void setFile(String setting, byte[] bytes) throws GeneralSecurityException {
        assert isLoaded();
        bytes = Base64.getEncoder().encode(bytes);
        char[] chars = new char[bytes.length];
        for (int i = 0; i < chars.length; ++i) {
            chars[i] = (char)bytes[i]; // PBE only stores the lower 8 bits, so this narrowing is ok
        }
        SecretKey secretKey = stringFactory.generateSecret(new PBEKeySpec(chars));
        keystore.get().setEntry(setting, new KeyStore.SecretKeyEntry(secretKey), keystorePassword.get());
        settingTypes.put(setting, KeyType.FILE);
    }

    /** Remove the given setting from the keystore. */
    void remove(String setting) throws KeyStoreException {
        assert isLoaded();
        keystore.get().deleteEntry(setting);
        settingTypes.remove(setting);
    }

    @Override
    public void close() throws IOException {
        try {
            if (keystorePassword.get() != null) {
                keystorePassword.get().destroy();
            }
        } catch (DestroyFailedException e) {
            throw new IOException(e);
        }
    }
}
