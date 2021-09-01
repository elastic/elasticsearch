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
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.Certificate;
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

    private static final String CA_FILE_TYPE = "PEM " + SslConfigurationKeys.CERTIFICATE_AUTHORITIES;
    private final List<String> certificateAuthorities;
    private final Path basePath;

    /**
     * Construct a new trust config for the provided paths (which will be resolved relative to the basePath).
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
    public PemTrustConfig(List<String> certificateAuthorities, Path basePath) {
        this.certificateAuthorities = Collections.unmodifiableList(certificateAuthorities);
        this.basePath = basePath;
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return resolveFiles();
    }

    @Override
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        final List<StoredCertificate> info = new ArrayList<>(certificateAuthorities.size());
        for (String caPath : certificateAuthorities) {
            for (Certificate cert : readCertificates(List.of(resolveFile(caPath)))) {
                if (cert instanceof X509Certificate) {
                    info.add(new StoredCertificate((X509Certificate) cert, caPath, "PEM", null, false));
                }
            }
        }
        return info;
    }

    @Override
    public X509ExtendedTrustManager createTrustManager() {
        final List<Path> paths = resolveFiles();
        try {
            final List<Certificate> certificates = readCertificates(paths);
            final KeyStore store = KeyStoreUtil.buildTrustStore(certificates);
            return KeyStoreUtil.createTrustManager(store, TrustManagerFactory.getDefaultAlgorithm());
        } catch (GeneralSecurityException e) {
            throw new SslConfigException(
                "cannot create trust using PEM certificates [" + SslFileUtil.pathsToString(paths) + "]", e);
        }
    }

    private List<Path> resolveFiles() {
        return this.certificateAuthorities.stream().map(this::resolveFile).collect(Collectors.toUnmodifiableList());
    }

    private Path resolveFile(String other) {
        return basePath.resolve(other);
    }

    private List<Certificate> readCertificates(List<Path> paths) {
        try {
            return PemUtils.readCertificates(paths);
        } catch (AccessControlException e) {
            throw SslFileUtil.accessControlFailure(CA_FILE_TYPE, paths, e, basePath);
        } catch (IOException e) {
            throw SslFileUtil.ioException(CA_FILE_TYPE, paths, e);
        } catch (GeneralSecurityException e) {
            throw SslFileUtil.securityException(CA_FILE_TYPE, paths, e);
        } catch (SslConfigException e) {
            throw SslFileUtil.configException(CA_FILE_TYPE, paths, e);
        }
    }

    @Override
    public String toString() {
        return "PEM-trust{" + SslFileUtil.pathsToString(resolveFiles()) + "}";
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

}
