/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.core.Tuple;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link SslKeyConfig} that builds a Key Manager from a keystore file.
 */
public class StoreKeyConfig implements SslKeyConfig {
    private final String keystorePath;
    private final String type;
    private final char[] storePassword;
    private final char[] keyPassword;
    private final String algorithm;
    private final Path configBasePath;

    /**
     * @param path          The path to the keystore file
     * @param storePassword The password for the keystore
     * @param type          The {@link KeyStore#getType() type} of the keystore (typically "PKCS12" or "jks").
     *                      See {@link KeyStoreUtil#inferKeyStoreType}.
     * @param keyPassword   The password for the key(s) within the keystore
     *                      (see {@link javax.net.ssl.KeyManagerFactory#init(KeyStore, char[])}).
     * @param algorithm     The algorithm to use for the Key Manager (see {@link KeyManagerFactory#getAlgorithm()}).
     * @param configBasePath The base path for configuration files (used for error handling)
     */
    public StoreKeyConfig(String path, char[] storePassword, String type, char[] keyPassword, String algorithm, Path configBasePath) {
        this.storePassword = Objects.requireNonNull(storePassword, "Keystore password cannot be null (but may be empty)");
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key password cannot be null (but may be empty)");
        this.algorithm = Objects.requireNonNull(algorithm, "Keystore algorithm cannot be null");
        this.configBasePath = Objects.requireNonNull(configBasePath, "Config path cannot be null");
        this.keystorePath = Objects.requireNonNull(path, "Keystore path cannot be null");
        this.type = Objects.requireNonNull(type, "Keystore type cannot be null");
    }

    @Override
    public SslTrustConfig asTrustConfig() {
        final String trustStoreAlgorithm = TrustManagerFactory.getDefaultAlgorithm();
        return new StoreTrustConfig(keystorePath, storePassword, type, trustStoreAlgorithm, false, configBasePath);
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return List.of(resolvePath());
    }

    @Override
    public boolean hasKeyMaterial() {
        return true;
    }

    private Path resolvePath() {
        return configBasePath.resolve(keystorePath);
    }

    @Override
    public List<Tuple<PrivateKey, X509Certificate>> getKeys() {
        final Path path = resolvePath();
        final KeyStore keyStore = readKeyStore(path);
        return KeyStoreUtil.stream(keyStore, ex -> keystoreException(path, ex))
            .filter(KeyStoreUtil.KeyStoreEntry::isKeyEntry)
            .map(entry -> {
                final X509Certificate certificate = entry.getX509Certificate();
                if (certificate != null) {
                    return new Tuple<>(entry.getKey(keyPassword), certificate);
                }
                return null;
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public Collection<StoredCertificate> getConfiguredCertificates() {
        final Path path = resolvePath();
        final KeyStore keyStore = readKeyStore(path);
        return KeyStoreUtil.stream(keyStore, ex -> keystoreException(path, ex))
            .flatMap(entry -> {
                final List<StoredCertificate> certificates = new ArrayList<>();
                boolean firstElement = true;
                for (X509Certificate certificate : entry.getX509CertificateChain()) {
                    certificates.add(new StoredCertificate(certificate, keystorePath, type, entry.getAlias(), firstElement));
                    firstElement = false;
                }
                return certificates.stream();
            })
            .collect(Collectors.toUnmodifiableList());
    }

    @Override
    public X509ExtendedKeyManager createKeyManager() {
        final Path path = resolvePath();
        return createKeyManager(path);
    }

    private X509ExtendedKeyManager createKeyManager(Path path) {
        try {
            final KeyStore keyStore = readKeyStore(path);
            checkKeyStore(keyStore, path);
            return KeyStoreUtil.createKeyManager(keyStore, keyPassword, algorithm);
        } catch (GeneralSecurityException e) {
            throw keystoreException(path, e);
        }
    }

    private KeyStore readKeyStore(Path path) {
        try {
            return KeyStoreUtil.readKeyStore(path, type, storePassword);
        } catch (AccessControlException e) {
            throw SslFileUtil.accessControlFailure("[" + type + "] keystore", List.of(path), e, configBasePath);
        } catch (IOException e) {
            throw SslFileUtil.ioException("[" + type + "] keystore", List.of(path), e);
        } catch (GeneralSecurityException e) {
            throw keystoreException(path, e);
        }
    }

    private SslConfigException keystoreException(Path path, GeneralSecurityException e) {
        String extra = null;
        if (e instanceof UnrecoverableKeyException) {
            extra = "this is usually caused by an incorrect key-password";
            if (keyPassword.length == 0) {
                extra += " (no key-password was provided)";
            } else if (Arrays.equals(storePassword, keyPassword)) {
                extra += " (we tried to access the key using the same password as the keystore)";
            }
        }
        return SslFileUtil.securityException("[" + type + "] keystore", path == null ? List.of() : List.of(path), e, extra);
    }

    /**
     * Verifies that the keystore contains at least 1 private key entry.
     */
    private void checkKeyStore(KeyStore keyStore, Path path) throws KeyStoreException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                return;
            }
        }
        String message = "the " + keyStore.getType() + " keystore";
        if (path != null) {
            message += " [" + path + "]";
        }
        message += "does not contain a private key entry";
        throw new SslConfigException(message);
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getSimpleName());
        sb.append('{');

        String path = keystorePath;
        if (path != null) {
            sb.append("path=").append(path).append(", ");
        }
        sb.append("type=").append(type);
        sb.append(", storePassword=").append(storePassword.length == 0 ? "<empty>" : "<non-empty>");
        sb.append(", keyPassword=");
        if (keyPassword.length == 0) {
            sb.append("<empty>");
        } else if (Arrays.equals(storePassword, keyPassword)) {
            sb.append("<same-as-store-password>");
        } else {
            sb.append("<set>");
        }
        sb.append(", algorithm=").append(algorithm);
        sb.append('}');
        return sb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreKeyConfig that = (StoreKeyConfig) o;
        return this.keystorePath.equals(that.keystorePath)
            && this.type.equals(that.type)
            && this.algorithm.equals(that.algorithm)
            && Arrays.equals(this.storePassword, that.storePassword)
            && Arrays.equals(this.keyPassword, that.keyPassword);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(keystorePath, type, algorithm);
        result = 31 * result + Arrays.hashCode(storePassword);
        result = 31 * result + Arrays.hashCode(keyPassword);
        return result;
    }
}
