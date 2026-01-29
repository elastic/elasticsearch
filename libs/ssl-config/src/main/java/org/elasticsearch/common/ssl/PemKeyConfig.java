/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.core.Tuple;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;

/**
 * A {@link SslKeyConfig} that reads from PEM formatted paths.
 */
public final class PemKeyConfig implements SslKeyConfig {

    private static final String KEY_FILE_TYPE = "PEM private key";
    private static final String CERT_FILE_TYPE = "PEM certificate";

    private final String certificate;
    private final String key;
    private final char[] keyPassword;
    private final Path configBasePath;

    /**
     * @param certificatePath Path to the PEM formatted certificate
     * @param keyPath         Path to the PEM formatted private key for {@code certificate}
     * @param keyPassword     Password for the private key (or empty is the key is not encrypted)
     * @param configBasePath  The base directory from which config files should be read (used for diagnostic exceptions)
     */
    public PemKeyConfig(String certificatePath, String keyPath, char[] keyPassword, Path configBasePath) {
        this.certificate = Objects.requireNonNull(certificatePath, "Certificate path cannot be null");
        this.key = Objects.requireNonNull(keyPath, "Key path cannot be null");
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key password cannot be null (but may be empty)");
        this.configBasePath = Objects.requireNonNull(configBasePath, "Config base path cannot be null");
    }

    @Override
    public boolean hasKeyMaterial() {
        return true;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Arrays.asList(resolve(certificate), resolve(key));
    }

    private Path resolve(String fileName) {
        return configBasePath.resolve(fileName);
    }

    @Override
    public Collection<StoredCertificate> getConfiguredCertificates() {
        final List<Certificate> certificates = getCertificates(resolve(this.certificate));
        final List<StoredCertificate> info = new ArrayList<>(certificates.size());
        boolean first = true;
        for (Certificate cert : certificates) {
            if (cert instanceof X509Certificate x509Certificate) {
                info.add(new StoredCertificate(x509Certificate, this.certificate, "PEM", null, first));
            }
            first = false;
        }
        return info;
    }

    @Override
    public X509ExtendedKeyManager createKeyManager() {
        final Path keyPath = resolve(key);
        final PrivateKey privateKey = getPrivateKey(keyPath);
        final Path certPath = resolve(this.certificate);
        final List<Certificate> certificates = getCertificates(certPath);
        try {
            final KeyStore keyStore = KeyStoreUtil.buildKeyStore(certificates, privateKey, keyPassword);
            return KeyStoreUtil.createKeyManager(keyStore, keyPassword, KeyManagerFactory.getDefaultAlgorithm());
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("failed to load a KeyManager for certificate/key pair [" + certPath + "], [" + keyPath + "]", e);
        }
    }

    @Override
    public List<Tuple<PrivateKey, X509Certificate>> getKeys() {
        final Path keyPath = resolve(key);
        final Path certPath = resolve(this.certificate);
        final List<Certificate> certificates = getCertificates(certPath);
        if (certificates.isEmpty()) {
            return List.of();
        }
        final Certificate leafCertificate = certificates.get(0);
        if (leafCertificate instanceof X509Certificate x509Certificate) {
            return List.of(Tuple.tuple(getPrivateKey(keyPath), x509Certificate));
        } else {
            return List.of();
        }
    }

    @Override
    public SslTrustConfig asTrustConfig() {
        return new PemTrustConfig(List.of(certificate), configBasePath);
    }

    private PrivateKey getPrivateKey(Path path) {
        try {
            final PrivateKey privateKey = PemUtils.parsePrivateKey(path, () -> keyPassword);
            if (privateKey == null) {
                throw new SslConfigException("could not load ssl private key file [" + path + "]");
            }
            return privateKey;
        } catch (SecurityException e) {
            throw SslFileUtil.accessControlFailure(KEY_FILE_TYPE, List.of(path), e, configBasePath);
        } catch (IOException e) {
            throw SslFileUtil.ioException(KEY_FILE_TYPE, List.of(path), e);
        } catch (GeneralSecurityException e) {
            throw SslFileUtil.securityException(KEY_FILE_TYPE, List.of(path), e);
        }
    }

    private List<Certificate> getCertificates(Path path) {
        try {
            return PemUtils.readCertificates(Collections.singleton(path));
        } catch (SecurityException e) {
            throw SslFileUtil.accessControlFailure(CERT_FILE_TYPE, List.of(path), e, configBasePath);
        } catch (IOException e) {
            throw SslFileUtil.ioException(CERT_FILE_TYPE, List.of(path), e);
        } catch (GeneralSecurityException e) {
            throw SslFileUtil.securityException(CERT_FILE_TYPE, List.of(path), e);
        }
    }

    @Override
    public String toString() {
        return "PEM-key-config{cert=" + certificate + " key=" + key + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PemKeyConfig that = (PemKeyConfig) o;
        return Objects.equals(this.certificate, that.certificate)
            && Objects.equals(this.key, that.key)
            && Arrays.equals(this.keyPassword, that.keyPassword);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(certificate, key);
        result = 31 * result + Arrays.hashCode(keyPassword);
        return result;
    }
}
