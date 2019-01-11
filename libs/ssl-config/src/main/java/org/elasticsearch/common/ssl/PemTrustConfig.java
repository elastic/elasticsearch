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

import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedTrustManager;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A {@link org.elasticsearch.common.ssl.SslTrustConfig} that reads a list of PEM encoded trusted certificates (CAs) from the file
 * system.
 * Strictly speaking, this class does not require PEM certificates, and will load any file that can be read by
 * {@link java.security.cert.CertificateFactory#generateCertificate(InputStream)}.
 */
public final class PemTrustConfig implements SslTrustConfig {
    private final List<Path> certificateAuthorities;

    /**
     * Construct a new trust config for the provided paths.
     * The paths are stored as-is, and are not read until {@link #createTrustManager()} is called.
     * This means that
     * <ol>
     * <li>validation of the file (contents and accessibility) is deferred, and this constructor will <em>not fail</em> on missing
     * of invalid files.</li>
     * <li>
     * if the contents of the files are modified, then subsequent calls {@link #createTrustManager()} will return a new trust
     * manager that trust a different set of CAs.
     * </li>
     * </ol>
     */
    public PemTrustConfig(List<Path> certificateAuthorities) {
        this.certificateAuthorities = Collections.unmodifiableList(certificateAuthorities);
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return certificateAuthorities;
    }

    @Override
    public X509ExtendedTrustManager createTrustManager() {
        try {
            final List<Certificate> certificates = loadCertificates();
            KeyStore store = KeyStoreUtil.buildTrustStore(certificates);
            return KeyStoreUtil.createTrustManager(store, TrustManagerFactory.getDefaultAlgorithm());
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("cannot create trust using PEM certificates [" + caPathsAsString() + "]", e);
        }
    }

    private List<Certificate> loadCertificates() throws CertificateException {
        try {
            return PemUtils.readCertificates(this.certificateAuthorities);
        } catch (FileNotFoundException | NoSuchFileException e) {
            throw new SslConfigException("cannot configure trust using PEM certificates [" + caPathsAsString()
                + "] because one or more files do not exist", e);
        } catch (IOException e) {
            throw new SslConfigException("cannot configure trust using PEM certificates [" + caPathsAsString()
                + "] because one or more files cannot be read", e);
        }
    }

    @Override
    public String toString() {
        return "PEM-trust{" + caPathsAsString() + "}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final PemTrustConfig that = (PemTrustConfig) o;
        return Objects.equals(this.certificateAuthorities, that.certificateAuthorities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(certificateAuthorities);
    }

    private String caPathsAsString() {
        return certificateAuthorities.stream()
            .map(Path::toAbsolutePath)
            .map(Object::toString)
            .collect(Collectors.joining(","));
    }

}
