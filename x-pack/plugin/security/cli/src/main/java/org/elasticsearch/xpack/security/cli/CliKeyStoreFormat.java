/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.cli;

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

    static final CliKeyStoreFormat PKCS12 = new CliKeyStoreFormat("PKCS12");

    private final String type;

    private CliKeyStoreFormat(String type) {
        this.type = type;
    }

    /**
     * The format to use on this JVM.
     */
    static CliKeyStoreFormat forRuntime() {
        return PKCS12;
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
