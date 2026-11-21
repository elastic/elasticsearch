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
import java.security.Provider;
import java.security.Security;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * The keystore format that the security CLI tools write, and expect when the user provides a keystore as input.
 * <p>
 * The format follows from the JCA providers that the JVM is configured with. The tools never register a provider themselves.
 */
final class CliKeyStoreFormat {

    static final CliKeyStoreFormat PKCS12 = new CliKeyStoreFormat("PKCS12", "PKCS#12", ".p12", null, null);

    static final String BC_FIPS_PROVIDER_NAME = "BCFIPS";

    private final String type;
    private final String description;
    private final String extension;
    @Nullable
    private final String settingsType;
    @Nullable
    private final Provider provider;

    private CliKeyStoreFormat(
        String type,
        String description,
        String extension,
        @Nullable String settingsType,
        @Nullable Provider provider
    ) {
        this.type = type;
        this.description = description;
        this.extension = extension;
        this.settingsType = settingsType;
        this.provider = provider;
    }

    /**
     * The BCFKS format, as implemented by the given provider. The provider does not need to be registered with the JVM.
     */
    static CliKeyStoreFormat bcfks(Provider provider) {
        return new CliKeyStoreFormat("BCFKS", "BCFKS", ".bcfks", "BCFKS", Objects.requireNonNull(provider));
    }

    /**
     * The format to use on this JVM: BCFKS if the Bouncy Castle FIPS provider is registered, because PKCS#12 keystores are not FIPS
     * 140 approved, and PKCS#12 otherwise.
     */
    static CliKeyStoreFormat forRuntime() {
        return forProviders(Security::getProvider);
    }

    /**
     * @param providers Returns the registered provider with the given name, or {@code null} if there is none.
     */
    static CliKeyStoreFormat forProviders(Function<String, Provider> providers) {
        final Provider bcFips = providers.apply(BC_FIPS_PROVIDER_NAME);
        return bcFips == null ? PKCS12 : bcfks(bcFips);
    }

    /**
     * The JCA keystore type, e.g. {@code PKCS12}.
     */
    String type() {
        return type;
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
     * Explains why {@code password} cannot protect a keystore of this format, or returns {@code null} if it can.
     */
    @Nullable
    String passwordError(char[] password) {
        return switch (type) {
            case "PKCS12" -> CertificateTool.isAscii(password) ? null : description + " passwords must be plain ASCII";
            // BC-FIPS enforces a 112 bit minimum for PBKDF2 in approved-only mode, but not for the keys that protect a BCFKS keystore
            case "BCFKS" -> null;
            default -> throw new IllegalStateException("unexpected keystore type [" + type + "]");
        };
    }

    /**
     * What users should know about where keystores of this format can be used, or {@code null} if they can be used widely.
     */
    @Nullable
    String compatibilityNote() {
        return switch (type) {
            case "PKCS12" -> null;
            case "BCFKS" -> "BCFKS keystores can only be read by Java applications that use the Bouncy Castle FIPS provider.";
            default -> throw new IllegalStateException("unexpected keystore type [" + type + "]");
        };
    }

    /**
     * Creates a new, empty, in-memory keystore of this format.
     */
    KeyStore newKeyStore() throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
        final KeyStore keyStore = getInstance();
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
        final KeyStore keyStore = getInstance();
        try (InputStream in = Files.newInputStream(path)) {
            keyStore.load(in, password);
        }
        return CertParsingUtils.readKeyPairsFromKeystore(keyStore, keyPassword);
    }

    private KeyStore getInstance() throws KeyStoreException {
        return provider == null ? KeyStore.getInstance(type) : KeyStore.getInstance(type, provider);
    }
}
