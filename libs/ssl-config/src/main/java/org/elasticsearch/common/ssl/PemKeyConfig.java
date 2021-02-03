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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * A {@link SslKeyConfig} that reads from PEM formatted paths.
 */
public final class PemKeyConfig implements SslKeyConfig {
    private final Path certificate;
    private final Path key;
    private final char[] keyPassword;

    public PemKeyConfig(Path certificate, Path key, char[] keyPassword) {
        this.certificate = Objects.requireNonNull(certificate, "Certificate cannot be null");
        this.key = Objects.requireNonNull(key, "Key cannot be null");
        this.keyPassword = Objects.requireNonNull(keyPassword, "Key password cannot be null (but may be empty)");
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return Arrays.asList(certificate, key);
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

    private PrivateKey getPrivateKey() {
        try {
            final PrivateKey privateKey = PemUtils.readPrivateKey(key, () -> keyPassword);
            if (privateKey == null) {
                throw new SslConfigException("could not load ssl private key file [" + key + "]");
            }
            return privateKey;
        } catch (FileNotFoundException | NoSuchFileException e) {
            throw new SslConfigException("the configured ssl private key file [" + key.toAbsolutePath() + "] does not exist", e);
        } catch (IOException e) {
            throw new SslConfigException("the configured ssl private key file [" + key.toAbsolutePath() + "] cannot be read", e);
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("cannot load ssl private key file [" + key.toAbsolutePath() + "]", e);
        }
    }

    private List<Certificate> getCertificates() {
        try {
            return PemUtils.readCertificates(Collections.singleton(certificate));
        } catch (FileNotFoundException | NoSuchFileException e) {
            throw new SslConfigException("the configured ssl certificate file [" + certificate.toAbsolutePath() + "] does not exist", e);
        } catch (IOException e) {
            throw new SslConfigException("the configured ssl certificate file [" + certificate .toAbsolutePath()+ "] cannot be read", e);
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("cannot load ssl certificate from [" + certificate.toAbsolutePath() + "]", e);
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
