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
import java.io.IOException;
import java.nio.file.Path;
import java.security.AccessControlException;
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

/**
 * A {@link SslKeyConfig} that reads from PEM formatted paths.
 */
public final class PemKeyConfig implements SslKeyConfig {

    private static final String KEY_FILE_TYPE = "PEM private key";
    private static final String CERT_FILE_TYPE = "PEM certificate";

    private final Path certificate;
    private final Path key;
    private final char[] keyPassword;
    private final Path configBasePath;

    /**
     * @param certificate    Path to the PEM formatted certificate
     * @param key            Path to the PEM formatted private key for {@code certificate}
     * @param keyPassword    Password for the private key (or empty is the key is not encrypted)
     * @param configBasePath The base directory from which config files should be read (used for diagnostic exceptions)
     */
    public PemKeyConfig(Path certificate, Path key, char[] keyPassword, Path configBasePath) {
        this.certificate = Objects.requireNonNull(certificate, "Certificate cannot be null");
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key password cannot be null (but may be empty)");
        this.configBasePath = Objects.requireNonNull(configBasePath, "Config base path cannot be null");
    }

    @Override
    public boolean hasKeyMaterial() {
        return true;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Arrays.asList(certificate, key);
    }

    @Override
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        final List<Certificate> certificates = getCertificates();
        final List<StoredCertificate> info = new ArrayList<>(certificates.size());
        boolean first = true;
        for (Certificate cert : certificates) {
            if (cert instanceof X509Certificate) {
                info.add(new StoredCertificate((X509Certificate) cert, this.certificate, "PEM", null, first));
            }
            first = false;
        }
        return info;
    }

    @Override
    public X509ExtendedKeyManager createKeyManager() {
        PrivateKey privateKey = getPrivateKey();
        List<Certificate> certificates = getCertificates();
        try {
            final KeyStore keyStore = KeyStoreUtil.buildKeyStore(certificates, privateKey, keyPassword);
            return KeyStoreUtil.createKeyManager(keyStore, keyPassword, KeyManagerFactory.getDefaultAlgorithm());
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("failed to load a KeyManager for certificate/key pair [" + certificate + "], [" + key + "]", e);
        }
    }

    @Override
    public SslTrustConfig asTrustConfig() {
        return new PemTrustConfig(List.of(certificate), configBasePath);
    }

    private PrivateKey getPrivateKey() {
        try {
            final PrivateKey privateKey = PemUtils.readPrivateKey(key, () -> keyPassword);
            if (privateKey == null) {
                throw new SslConfigException("could not load ssl private key file [" + key + "]");
            }
            return privateKey;
        } catch (AccessControlException e) {
            throw SslFileUtil.accessControlFailure(KEY_FILE_TYPE, List.of(key), e, configBasePath);
        } catch (IOException e) {
            throw SslFileUtil.ioException(KEY_FILE_TYPE, List.of(key), e);
        } catch (GeneralSecurityException e) {
            throw SslFileUtil.securityException(KEY_FILE_TYPE, List.of(key), e);
        }
    }

    private List<Certificate> getCertificates() {
        try {
            return PemUtils.readCertificates(Collections.singleton(certificate));
        } catch (AccessControlException e) {
            throw SslFileUtil.accessControlFailure(CERT_FILE_TYPE, List.of(certificate), e, configBasePath);
        } catch (IOException e) {
            throw SslFileUtil.ioException(CERT_FILE_TYPE, List.of(certificate), e);
        } catch (GeneralSecurityException e) {
            throw SslFileUtil.securityException(CERT_FILE_TYPE, List.of(certificate), e);
        }
    }

    @Override
    public String toString() {
        return "PEM-key-config{cert=" + certificate.toAbsolutePath() + " key=" + key.toAbsolutePath() + "}";
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
        return Objects.equals(this.certificate, that.certificate) &&
            Objects.equals(this.key, that.key) &&
            Arrays.equals(this.keyPassword, that.keyPassword);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(certificate, key);
        result = 31 * result + Arrays.hashCode(keyPassword);
        return result;
    }
}
