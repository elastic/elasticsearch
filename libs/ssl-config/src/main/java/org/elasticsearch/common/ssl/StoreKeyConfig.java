/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.UnrecoverableKeyException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link SslKeyConfig} that builds a Key Manager from a keystore file.
 */
public class StoreKeyConfig implements SslKeyConfig {
    private final Path path;
    private final char[] storePassword;
    private final String type;
    private final char[] keyPassword;
    private final String algorithm;

    /**
     * @param path          The path to the keystore file
     * @param storePassword The password for the keystore
     * @param type          The {@link KeyStore#getType() type} of the keystore (typically "PKCS12" or "jks").
     *                      See {@link KeyStoreUtil#inferKeyStoreType(Path)}.
     * @param keyPassword   The password for the key(s) within the keystore
     *                      (see {@link javax.net.ssl.KeyManagerFactory#init(KeyStore, char[])}).
     * @param algorithm     The algorithm to use for the Key Manager (see {@link KeyManagerFactory#getAlgorithm()}).
     */
    StoreKeyConfig(Path path, char[] storePassword, String type, char[] keyPassword, String algorithm) {
        this.path = Objects.requireNonNull(path, "Keystore path cannot be null");
        this.storePassword = Objects.requireNonNull(storePassword, "Keystore password cannot be null (but may be empty)");
        this.type = Objects.requireNonNull(type, "Keystore type cannot be null");
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key password cannot be null (but may be empty)");
        this.algorithm = Objects.requireNonNull(algorithm, "Keystore algorithm cannot be null");
    }

    @Override
    public SslTrustConfig asTrustConfig() {
        return new StoreTrustConfig(path, storePassword, type, algorithm, false);
    }

    @Override
    public boolean hasKeyMaterial() {
        return true;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Collections.singleton(path);
    }

    @Override
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        try {
            final KeyStore keyStore = readKeyStore();
            return KeyStoreUtil.stream(keyStore)
                .flatMap(entry -> {
                    try {
                        final List<StoredCertificate> certificates = new ArrayList<>();
                        boolean firstElement = true;
                        for (X509Certificate certificate : entry.getX509CertificateChain()) {
                            certificates.add(new StoredCertificate(certificate, this.path, this.type, entry.getAlias(), firstElement));
                            firstElement = false;
                        }
                        return certificates.stream();
                    } catch (KeyStoreException ex) {
                        throw keystoreException(ex, "read keystore certificates");
                    }
                })
                .collect(Collectors.toUnmodifiableList());
        } catch (GeneralSecurityException e) {
            throw keystoreException(e, "process keystore");
        }
    }

    @Override
    public X509ExtendedKeyManager createKeyManager() {
        try {
            final KeyStore keyStore = readKeyStore();
            checkKeyStore(keyStore);
            return KeyStoreUtil.createKeyManager(keyStore, keyPassword, algorithm);
        } catch (GeneralSecurityException e) {
            throw keystoreException(e, "initialize SSL KeyManager");
        }
    }

    private KeyStore readKeyStore() {
        try {
            return KeyStoreUtil.readKeyStore(path, type, storePassword);
        } catch (GeneralSecurityException e) {
            throw keystoreException(e, "read keystore");
        }
    }

    private SslConfigException keystoreException(GeneralSecurityException e, String context) {
        String message = "failed to " + context + " for [" + path + "] and type [" + type + "]";
        if (e instanceof UnrecoverableKeyException) {
            message += " this is usually caused by an incorrect key-password";
            if (keyPassword.length == 0) {
                message += " (no key-password was provided)";
            } else if (Arrays.equals(storePassword, keyPassword)) {
                message += " (we tried to access the key using the same password as the keystore)";
            }
        }
        return new SslConfigException(message, e);
    }

    /**
     * Verifies that the keystore contains at least 1 private key entry.
     */
    private void checkKeyStore(KeyStore keyStore) throws KeyStoreException {
        Enumeration<String> aliases = keyStore.aliases();
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            if (keyStore.isKeyEntry(alias)) {
                return;
            }
        }
        throw new SslConfigException("the keystore [" + path + "] does not contain a private key entry");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreKeyConfig that = (StoreKeyConfig) o;
        return path.equals(that.path)
            && Arrays.equals(storePassword, that.storePassword)
            && type.equals(that.type)
            && Arrays.equals(keyPassword, that.keyPassword)
            && algorithm.equals(that.algorithm);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(path, type, algorithm);
        result = 31 * result + Arrays.hashCode(storePassword);
        result = 31 * result + Arrays.hashCode(keyPassword);
        return result;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("StoreKeyConfig{");
        sb.append("path=").append(path);
        sb.append(", storePassword=").append(storePassword.length == 0 ? "<empty>" : "<non-empty>");
        sb.append(", type=").append(type);
        sb.append(", keyPassword=").append(Arrays.equals(storePassword, keyPassword) ? "<not-set>" : "<set>");
        sb.append(", algorithm=").append(algorithm);
        sb.append('}');
        return sb.toString();
    }
}
