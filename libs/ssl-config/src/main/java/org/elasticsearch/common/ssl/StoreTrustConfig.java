/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.X509ExtendedTrustManager;
import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link SslTrustConfig} that builds a Trust Manager from a keystore file.
 */
public final class StoreTrustConfig implements SslTrustConfig {
    private final String truststorePath;
    private final char[] password;
    private final String type;
    private final String algorithm;
    private final boolean requireTrustAnchors;
    private final Path configBasePath;

    /**
     * @param path      The path to the keystore file
     * @param password  The password for the keystore
     * @param type      The {@link KeyStore#getType() type} of the keystore (typically "PKCS12" or "jks").
 *                  See {@link KeyStoreUtil#inferKeyStoreType}.
     * @param algorithm The algorithm to use for the Trust Manager (see {@link javax.net.ssl.TrustManagerFactory#getAlgorithm()}).
     * @param requireTrustAnchors If true, the truststore will be checked to ensure that it contains at least one valid trust anchor.
     * @param configBasePath The base path for the configuration directory
     */
    public StoreTrustConfig(String path, char[] password, String type, String algorithm, boolean requireTrustAnchors, Path configBasePath) {
        this.truststorePath = Objects.requireNonNull(path, "Truststore path cannot be null");
        this.type = Objects.requireNonNull(type, "Truststore type cannot be null");
        this.algorithm = Objects.requireNonNull(algorithm, "Truststore algorithm cannot be null");
        this.password = Objects.requireNonNull(password, "Truststore password cannot be null (but may be empty)");
        this.requireTrustAnchors = requireTrustAnchors;
        this.configBasePath = configBasePath;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return List.of(resolvePath());
    }

    private Path resolvePath() {
        return configBasePath.resolve(this.truststorePath);
    }

    @Override
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        final Path path = resolvePath();
        final KeyStore trustStore = readKeyStore(path);
        return KeyStoreUtil.stream(trustStore, ex -> keystoreException(path, ex))
            .map(entry -> {
                final X509Certificate certificate = entry.getX509Certificate();
                if (certificate != null) {
                    final boolean hasKey = entry.isKeyEntry();
                    return new StoredCertificate(certificate, this.truststorePath, this.type, entry.getAlias(), hasKey);
                } else {
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public X509ExtendedTrustManager createTrustManager() {
        final Path path = resolvePath();
        try {
            final KeyStore store = readKeyStore(path);
            if (requireTrustAnchors) {
                checkTrustStore(store, path);
            }
            return KeyStoreUtil.createTrustManager(store, algorithm);
        } catch (GeneralSecurityException e) {
            throw keystoreException(path, e);
        }
    }

    private KeyStore readKeyStore(Path path) {
        try {
            return KeyStoreUtil.readKeyStore(path, type, password);
        } catch (AccessControlException e) {
            throw SslFileUtil.accessControlFailure(fileTypeForException(), List.of(path), e, configBasePath);
        } catch (IOException e) {
            throw SslFileUtil.ioException(fileTypeForException(), List.of(path), e, getAdditionalErrorDetails());
        } catch (GeneralSecurityException e) {
            throw keystoreException(path, e);
        }
    }

    private SslConfigException keystoreException(Path path, GeneralSecurityException e) {
        final String extra = getAdditionalErrorDetails();
        return SslFileUtil.securityException(fileTypeForException(), List.of(path), e, extra);
    }

    private String getAdditionalErrorDetails() {
        final String extra;
        if (password.length == 0) {
             extra = "(no password was provided)";
         } else {
             extra = "(a keystore password was provided)";
         }
        return extra;
    }

    private String fileTypeForException() {
        return "[" + type + "] keystore (as a truststore)";
    }

    /**
     * Verifies that the keystore contains at least 1 trusted certificate entry.
     */
    private void checkTrustStore(KeyStore store, Path path) throws GeneralSecurityException {
        Enumeration<String> aliases = store.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (store.isCertificateEntry(alias)) {
                return;
            }
        }
        throw new SslConfigException("the truststore [" + path + "] does not contain any trusted certificate entries");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreTrustConfig that = (StoreTrustConfig) o;
        return truststorePath.equals(that.truststorePath)
            && Arrays.equals(password, that.password)
            && type.equals(that.type)
            && algorithm.equals(that.algorithm);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(truststorePath, type, algorithm);
        result = 31 * result + Arrays.hashCode(password);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StoreTrustConfig{");
        sb.append("path=").append(truststorePath);
        sb.append(", password=").append(password.length == 0 ? "<empty>" : "<non-empty>");
        sb.append(", type=").append(type);
        sb.append(", algorithm=").append(algorithm);
        sb.append('}');
        return sb.toString();
    }
}
