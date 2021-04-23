/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import java.security.cert.X509Certificate;
import java.util.ArrayList;
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
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        final List<StoredCertificate> info = new ArrayList<>(certificateAuthorities.size());
        for (Path path : certificateAuthorities) {
            for (Certificate cert : readCertificates(List.of(path))) {
                if (cert instanceof X509Certificate) {
                    info.add(new StoredCertificate((X509Certificate) cert, path, "PEM", null, false));
                }
            }
        }
        return info;
    }

    @Override
    public X509ExtendedTrustManager createTrustManager() {
        try {
            final List<Certificate> certificates = readCertificates(this.certificateAuthorities);
            final KeyStore store = KeyStoreUtil.buildTrustStore(certificates);
            return KeyStoreUtil.createTrustManager(store, TrustManagerFactory.getDefaultAlgorithm());
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("cannot create trust using PEM certificates [" + pathsToString(certificateAuthorities) + "]", e);
        }
    }

    private List<Certificate> readCertificates(List<Path> paths) {
        try {
            return PemUtils.readCertificates(paths);
        } catch (FileNotFoundException | NoSuchFileException e) {
            if (paths.size() == 1) {
                throw new SslConfigException("cannot read configured PEM certificate authority [" + pathsToString(paths)
                    + "] because the file does not exist", e);
            } else {
                throw new SslConfigException("cannot read configured PEM certificate authorities [" + pathsToString(paths)
                    + "] because one of more files do not exist", e);
            }
        } catch (IOException e) {
            if (paths.size() == 1) {
                throw new SslConfigException("cannot read configured PEM certificate authority [" + pathsToString(paths)
                    + "] because the file cannot be read", e);
            } else {
                throw new SslConfigException("cannot read configured PEM certificate authorities [" + pathsToString(paths)
                    + "] because one of more files cannot be read", e);
            }
        } catch (CertificateException e) {
            throw new SslConfigException("cannot read configured PEM certificate authorities [" + pathsToString(paths) + "]", e);
        }
    }

    @Override
    public String toString() {
        return "PEM-trust{" + pathsToString(certificateAuthorities) + "}";
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

    private String pathsToString(List<Path> paths) {
        return paths.stream()
            .map(Path::toAbsolutePath)
            .map(Object::toString)
            .collect(Collectors.joining(","));
    }

}
