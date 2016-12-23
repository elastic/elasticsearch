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
import java.io.Closeable;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Set;

import org.apache.lucene.util.SetOnce;

/**
 * A wrapper around a Java KeyStore which provides supplements the keystore with extra metadata.
 *
 * Loading a keystore has 2 phases. First, call {@link #loadMetadata(Path)}. Then call
 * {@link #loadKeystore(char[])} with the keystore password, or an empty char array if
 * {@link #hasPassword()} is {@code false}.
 */
public class KeyStoreWrapper implements Closeable {

    /** The version of the metadata written before the keystore data. */
    private static final int FORMAT_VERSION = 1;

    /** The keystore type for a newly created keystore. */
    private static final String NEW_KEYSTORE_TYPE = "PKCS12";

    /** A factory necessary for constructing instances of secrets in a {@link KeyStore}. */
    private static final SecretKeyFactory passwordFactory;
    static {
        try {
            passwordFactory = SecretKeyFactory.getInstance("PBE");
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    /** True iff the keystore has a password needed to read. */
    private final boolean hasPassword;

    /** The type of the keystore, as passed to {@link java.security.KeyStore#getInstance(String)} */
    private final String type;

    /** A stream of the actual keystore data. */
    private final InputStream input;

    /** The loaded keystore. See {@link #loadKeystore(char[])}. */
    private final SetOnce<KeyStore> keystore = new SetOnce<>();

    /** The password for the keystore. See {@link #loadKeystore(char[])}. */
    private final SetOnce<KeyStore.PasswordProtection> keystorePassword = new SetOnce<>();

    /** The setting names contained in the loaded keystore. */
    private final Set<String> settingNames = new HashSet<>();

    private KeyStoreWrapper(boolean hasPassword, String type, InputStream input) {
        this.hasPassword = hasPassword;
        this.type = type;
        this.input = input;
    }

    /** Returns a path representing the ES keystore in the given config dir. */
    static Path keystorePath(Path configDir) {
        return configDir.resolve("elasticsearch.keystore");
    }

    /** Constructs a new keystore with the given password. */
    static KeyStoreWrapper create(char[] password) throws Exception {
        KeyStoreWrapper wrapper = new KeyStoreWrapper(password.length != 0, NEW_KEYSTORE_TYPE, null);
        KeyStore keyStore = KeyStore.getInstance(NEW_KEYSTORE_TYPE);
        keyStore.load(null, null);
        wrapper.keystore.set(keyStore);
        wrapper.keystorePassword.set(new KeyStore.PasswordProtection(password));
        return wrapper;
    }

    /**
     * Loads information about the Elasticsearch keystore from the provided config directory.
     *
     * {@link #loadKeystore(char[])} must be called before reading or writing any entries.
     * Returns {@code null} if no keystore exists.
     */
    public static KeyStoreWrapper loadMetadata(Path configDir) throws IOException {
        Path keystoreFile = keystorePath(configDir);
        if (Files.exists(keystoreFile) == false) {
            return null;
        }
        DataInputStream inputStream = new DataInputStream(Files.newInputStream(keystoreFile));
        int format = inputStream.readInt();
        if (format != FORMAT_VERSION) {
            throw new IllegalStateException("Unknown keystore metadata format [" + format + "]");
        }
        boolean hasPassword = inputStream.readBoolean();
        String type = inputStream.readUTF();
        return new KeyStoreWrapper(hasPassword, type, inputStream);
    }

    /** Returns true iff {@link #loadKeystore(char[])} has been called. */
    public boolean isLoaded() {
        return keystore.get() != null;
    }

    /** Return true iff calling {@link #loadKeystore(char[])} requires a non-empty password. */
    public boolean hasPassword() {
        return hasPassword;
    }

    /** Loads the keystore this metadata wraps. This may only be called once. */
    public void loadKeystore(char[] password) throws GeneralSecurityException, IOException {
        this.keystore.set(KeyStore.getInstance(type));
        try (InputStream in = input) {
            keystore.get().load(in, password);
        }

        this.keystorePassword.set(new KeyStore.PasswordProtection(password));
        Arrays.fill(password, '\0');

        // convert keystore aliases enum into a set for easy lookup
        Enumeration<String> aliases = keystore.get().aliases();
        while (aliases.hasMoreElements()) {
            settingNames.add(aliases.nextElement());
        }
    }

    /** Write the keystore to the given config directory. */
    void save(Path configDir) throws Exception {
        Path keystoreFile = keystorePath(configDir);
        try (DataOutputStream outputStream = new DataOutputStream(Files.newOutputStream(keystoreFile))) {
            outputStream.writeInt(FORMAT_VERSION);
            char[] password = this.keystorePassword.get().getPassword();
            outputStream.writeBoolean(password.length != 0);
            outputStream.writeUTF(type);
            keystore.get().store(outputStream, password);
        }
        PosixFileAttributeView attrs = Files.getFileAttributeView(keystoreFile, PosixFileAttributeView.class);
        if (attrs != null) {
            // don't rely on umask: ensure the keystore has minimal permissions
            attrs.setPermissions(PosixFilePermissions.fromString("rw-------"));
        }
    }

    /** Returns the names of all settings in this keystore. */
    public Set<String> getSettings() {
        return settingNames;
    }

    /** Retrieve a string setting. The {@link SecureString} should be closed once it is used. */
    SecureString getStringSetting(String setting) throws GeneralSecurityException {
        KeyStore.Entry entry = keystore.get().getEntry(setting, keystorePassword.get());
        if (entry instanceof KeyStore.SecretKeyEntry == false) {
            throw new IllegalStateException("Secret setting " + setting + " is not a string");
        }
        // TODO: only allow getting a setting once?
        KeyStore.SecretKeyEntry secretKeyEntry = (KeyStore.SecretKeyEntry)entry;
        PBEKeySpec keySpec = (PBEKeySpec)passwordFactory.getKeySpec(secretKeyEntry.getSecretKey(), PBEKeySpec.class);
        SecureString value = new SecureString(keySpec.getPassword());
        keySpec.clearPassword();
        return value;
    }

    /** Set a string setting. */
    void setStringSetting(String setting, char[] value) throws GeneralSecurityException {
        SecretKey secretKey = passwordFactory.generateSecret(new PBEKeySpec(value));
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
