/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.cli.bc;

import org.bouncycastle.jce.provider.BouncyCastleProvider;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.util.Locale;

/**
 * The keystore formats that the security CLI tools can write, and read when the user supplies a keystore as input.
 * <p>
 * PKCS#12 is the historical default and is provided by the JDK. BCFKS is the format required by Elasticsearch in FIPS 140 mode; it is
 * provided here by the bundled (relocated) Bouncy Castle provider, so the tools can produce it on any JVM without the FIPS provider
 * being installed or registered. A BCFKS file written this way is byte-for-byte the same format the Bouncy Castle FIPS provider
 * reads and writes.
 */
public enum KeyStoreFormat {
    PKCS12("PKCS12", "PKCS#12", ".p12"),
    BCFKS("BCFKS", "BCFKS", ".bcfks");

    private static final BouncyCastleProvider BC_PROV = new BouncyCastleProvider();

    private final String type;
    private final String description;
    private final String extension;

    KeyStoreFormat(String type, String description, String extension) {
        this.type = type;
        this.description = description;
        this.extension = extension;
    }

    /**
     * The format to assume for a keystore file the user has provided, based on its extension. Unrecognised extensions are
     * treated as PKCS#12, which is what the tools accepted before BCFKS was supported.
     */
    public static KeyStoreFormat forFileName(String fileName) {
        return fileName != null && fileName.toLowerCase(Locale.ROOT).endsWith(BCFKS.extension) ? BCFKS : PKCS12;
    }

    /** The name to pass to {@link KeyStore#getInstance(String)} */
    public String type() {
        return type;
    }

    /** The name to show to users, e.g. {@code PKCS#12} */
    public String description() {
        return description;
    }

    /** The file extension, including the leading dot, e.g. {@code .p12} */
    public String extension() {
        return extension;
    }

    /** Creates a new, empty, in-memory keystore of this format */
    public KeyStore newKeyStore() throws GeneralSecurityException, IOException {
        final KeyStore keyStore = getInstance();
        keyStore.load(null);
        return keyStore;
    }

    /** Reads a keystore of this format from the given file */
    public KeyStore readKeyStore(Path path, char[] password) throws GeneralSecurityException, IOException {
        final KeyStore keyStore = getInstance();
        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, password);
        }
        return keyStore;
    }

    private KeyStore getInstance() throws GeneralSecurityException {
        return switch (this) {
            case PKCS12 -> KeyStore.getInstance(type);
            case BCFKS -> KeyStore.getInstance(type, BC_PROV);
        };
    }
}
