/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.ssl.CertParsingUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.function.Function;

/**
 * The keystore format that the security CLI tools write, and expect when the user provides a keystore as input.
 * <p>
 * The format follows from the JCA providers that the JVM is configured with. The tools never register a provider themselves.
 */
final class CliKeyStoreFormat {

    static final CliKeyStoreFormat PKCS12 = new CliKeyStoreFormat("PKCS12", "PKCS#12", ".p12", null);

    private final String type;
    private final String description;
    private final String extension;
    @Nullable
    private final String settingsType;

    private CliKeyStoreFormat(String type, String description, String extension, @Nullable String settingsType) {
        this.type = type;
        this.description = description;
        this.extension = extension;
        this.settingsType = settingsType;
    }

    /**
     * The format to use on this JVM.
     */
    static CliKeyStoreFormat forRuntime() {
        return PKCS12;
    }

    /**
     * The name of this format to show to users, e.g. {@code PKCS#12}.
     */
    String description() {
        return description;
    }

    /**
     * The file name extension for keystores of this format, including the leading dot, e.g. {@code .p12}.
     */
    String extension() {
        return extension;
    }

    /**
     * The value for {@code *.keystore.type} and {@code *.truststore.type} settings that point to a keystore of this format, or
     * {@code null} if Elasticsearch infers the type from the {@link #extension()} and the setting can be omitted.
     */
    @Nullable
    String settingsType() {
        return settingsType;
    }

    /**
     * Creates a new, empty, in-memory keystore of this format.
     */
    KeyStore newKeyStore() throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        final KeyStore keyStore = KeyStore.getInstance(type);
        keyStore.load(null);
        return keyStore;
    }

    /**
     * Reads all certificate-key pairs from a keystore file of this format.
     *
     * @param keyPassword Returns the password for the key entry with the given alias.
     */
    Map<Certificate, Key> readKeyPairs(Path path, char[] password, Function<String, char[]> keyPassword) throws GeneralSecurityException,
        IOException {
        final KeyStore keyStore = KeyStore.getInstance(type);
        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, password);
        }
        return CertParsingUtils.readKeyPairsFromKeystore(keyStore, keyPassword);
    }
}
