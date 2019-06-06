/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
