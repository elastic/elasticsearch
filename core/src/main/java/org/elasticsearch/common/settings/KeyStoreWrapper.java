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
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.BufferedChecksumIndexInput;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.SimpleFSDirectory;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;

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

    /** The name of the keystore file to read and write. */
    private static final String KEYSTORE_FILENAME = "elasticsearch.keystore";

    /** The version of the metadata written before the keystore data. */
    private static final int FORMAT_VERSION = 1;

    /** The keystore type for a newly created keystore. */
    private static final String NEW_KEYSTORE_TYPE = "PKCS12";

    /** The algorithm used to store password for a newly created keystore. */
    private static final String NEW_KEYSTORE_SECRET_KEY_ALGO = "PBE";//"PBEWithHmacSHA256AndAES_128";

    /** An encoder to check whether string values are ascii. */
    private static final CharsetEncoder ASCII_ENCODER = StandardCharsets.US_ASCII.newEncoder();

    /** True iff the keystore has a password needed to read. */
    private final boolean hasPassword;

    /** The type of the keystore, as passed to {@link java.security.KeyStore#getInstance(String)} */
    private final String type;

    /** A factory necessary for constructing instances of secrets in a {@link KeyStore}. */
    private final SecretKeyFactory secretFactory;

    /** The raw bytes of the encrypted keystore. */
    private final byte[] keystoreBytes;

    /** The loaded keystore. See {@link #decrypt(char[])}. */
    private final SetOnce<KeyStore> keystore = new SetOnce<>();

    /** The password for the keystore. See {@link #decrypt(char[])}. */
    private final SetOnce<KeyStore.PasswordProtection> keystorePassword = new SetOnce<>();

    /** The setting names contained in the loaded keystore. */
    private final Set<String> settingNames = new HashSet<>();

    private KeyStoreWrapper(boolean hasPassword, String type, String secretKeyAlgo, byte[] keystoreBytes) {
        this.hasPassword = hasPassword;
        this.type = type;
        try {
            secretFactory = SecretKeyFactory.getInstance(secretKeyAlgo);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        this.keystoreBytes = keystoreBytes;
    }

    /** Returns a path representing the ES keystore in the given config dir. */
    static Path keystorePath(Path configDir) {
        return configDir.resolve(KEYSTORE_FILENAME);
    }

    /** Constructs a new keystore with the given password. */
    static KeyStoreWrapper create(char[] password) throws Exception {
        KeyStoreWrapper wrapper = new KeyStoreWrapper(password.length != 0, NEW_KEYSTORE_TYPE, NEW_KEYSTORE_SECRET_KEY_ALGO, null);
        KeyStore keyStore = KeyStore.getInstance(NEW_KEYSTORE_TYPE);
        keyStore.load(null, null);
        wrapper.keystore.set(keyStore);
        wrapper.keystorePassword.set(new KeyStore.PasswordProtection(password));
        return wrapper;
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
            CodecUtil.checkHeader(input, KEYSTORE_FILENAME, FORMAT_VERSION, FORMAT_VERSION);
            byte hasPasswordByte = input.readByte();
            boolean hasPassword = hasPasswordByte == 1;
            if (hasPassword == false && hasPasswordByte != 0) {
                throw new IllegalStateException("hasPassword boolean is corrupt: "
                    + String.format(Locale.ROOT, "%02x", hasPasswordByte));
            }
            String type = input.readString();
            String secretKeyAlgo = input.readString();
            byte[] keystoreBytes = new byte[input.readInt()];
            input.readBytes(keystoreBytes, 0, keystoreBytes.length);
            CodecUtil.checkFooter(input);
            return new KeyStoreWrapper(hasPassword, type, secretKeyAlgo, keystoreBytes);
        }
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

        // convert keystore aliases enum into a set for easy lookup
        Enumeration<String> aliases = keystore.get().aliases();
        while (aliases.hasMoreElements()) {
            settingNames.add(aliases.nextElement());
        }
    }

    /** Write the keystore to the given config directory. */
    void save(Path configDir) throws Exception {
        char[] password = this.keystorePassword.get().getPassword();

        SimpleFSDirectory directory = new SimpleFSDirectory(configDir);
        // write to tmp file first, then overwrite
        String tmpFile = KEYSTORE_FILENAME + ".tmp";
        try (IndexOutput output = directory.createOutput(tmpFile, IOContext.DEFAULT)) {
            CodecUtil.writeHeader(output, KEYSTORE_FILENAME, FORMAT_VERSION);
            output.writeByte(password.length == 0 ? (byte)0 : (byte)1);
            output.writeString(type);
            output.writeString(secretFactory.getAlgorithm());

            ByteArrayOutputStream keystoreBytesStream = new ByteArrayOutputStream();
            keystore.get().store(keystoreBytesStream, password);
            byte[] keystoreBytes = keystoreBytesStream.toByteArray();
            output.writeInt(keystoreBytes.length);
            output.writeBytes(keystoreBytes, keystoreBytes.length);
            CodecUtil.writeFooter(output);
        }

        Path keystoreFile = keystorePath(configDir);
        Files.move(configDir.resolve(tmpFile), keystoreFile, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        PosixFileAttributeView attrs = Files.getFileAttributeView(keystoreFile, PosixFileAttributeView.class);
        if (attrs != null) {
            // don't rely on umask: ensure the keystore has minimal permissions
            attrs.setPermissions(PosixFilePermissions.fromString("rw-------"));
        }
    }

    @Override
    public Set<String> getSettingNames() {
        return settingNames;
    }

    // TODO: make settings accessible only to code that registered the setting
    /** Retrieve a string setting. The {@link SecureString} should be closed once it is used. */
    @Override
    public SecureString getString(String setting) throws GeneralSecurityException {
        KeyStore.Entry entry = keystore.get().getEntry(setting, keystorePassword.get());
        if (entry instanceof KeyStore.SecretKeyEntry == false) {
            throw new IllegalStateException("Secret setting " + setting + " is not a string");
        }
        // TODO: only allow getting a setting once?
        KeyStore.SecretKeyEntry secretKeyEntry = (KeyStore.SecretKeyEntry) entry;
        PBEKeySpec keySpec = (PBEKeySpec) secretFactory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);
        SecureString value = new SecureString(keySpec.getPassword());
        keySpec.clearPassword();
        return value;
    }

    /**
     * Set a string setting.
     *
     * @throws IllegalArgumentException if the value is not ASCII
     */
    void setString(String setting, char[] value) throws GeneralSecurityException {
        if (ASCII_ENCODER.canEncode(CharBuffer.wrap(value)) == false) {
            throw new IllegalArgumentException("Value must be ascii");
        }
        SecretKey secretKey = secretFactory.generateSecret(new PBEKeySpec(value));
        keystore.get().setEntry(setting, new KeyStore.SecretKeyEntry(secretKey), keystorePassword.get());
        settingNames.add(setting);
    }

    /** Remove the given setting from the keystore. */
    void remove(String setting) throws KeyStoreException {
        keystore.get().deleteEntry(setting);
        settingNames.remove(setting);
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
