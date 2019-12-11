/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import javax.net.ssl.X509ExtendedTrustManager;

import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.AccessControlException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Implementation of trust configuration that is backed by PEM encoded certificate files.
 */
class PEMTrustConfig extends TrustConfig {

    private static final String CA_FILE = "certificate_authorities";
    
    private final List<String> caPaths;

    /**
     * Create a new trust configuration that is built from the certificate files
     *
     * @param caPaths the paths to the certificate files to trust
     */
    PEMTrustConfig(List<String> caPaths) {
        this.caPaths = Objects.requireNonNull(caPaths, "ca paths must be specified");
    }

    @Override
    X509ExtendedTrustManager createTrustManager(@Nullable Environment environment) {
        try {
            Certificate[] certificates = CertParsingUtils.readCertificates(caPaths, environment);
            return CertParsingUtils.trustManager(certificates);
        } catch (NoSuchFileException noSuchFileException) {
            final Path missingPath = CertParsingUtils.resolvePath(noSuchFileException.getFile(), environment);
            throw missingTrustConfigFile(noSuchFileException, CA_FILE, missingPath);
        } catch (AccessDeniedException accessDeniedException) {
            final Path missingPath = CertParsingUtils.resolvePath(accessDeniedException.getFile(), environment);
            throw unreadableTrustConfigFile(accessDeniedException, CA_FILE, missingPath);
        } catch (AccessControlException accessControlException) {
            final List<Path> paths = CertParsingUtils.resolvePaths(caPaths, environment);
            throw blockedTrustConfigFile(accessControlException, environment, CA_FILE, paths);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize SSL TrustManager", e);
        }
    }

    @Override
    Collection<CertificateInfo> certificates(Environment environment) throws CertificateException, IOException {
        final List<CertificateInfo> info = new ArrayList<>(caPaths.size());
        for (String path : caPaths) {
            Certificate[] chain = CertParsingUtils.readCertificates(Collections.singletonList(path), environment);
            for (final Certificate cert : chain) {
                if (cert instanceof X509Certificate) {
                    info.add(new CertificateInfo(path, "PEM", null, false, (X509Certificate) cert));
                }
            }
        }
        return info;
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        List<Path> paths = new ArrayList<>(caPaths.size());
        for (String path : caPaths) {
            paths.add(CertParsingUtils.resolvePath(path, environment));
        }
        return paths;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PEMTrustConfig that = (PEMTrustConfig) o;

        return caPaths != null ? caPaths.equals(that.caPaths) : that.caPaths == null;

    }

    @Override
    public int hashCode() {
        return caPaths != null ? caPaths.hashCode() : 0;
    }

    @Override
    public String toString() {
        return "ca=[" + Strings.collectionToCommaDelimitedString(caPaths) + "]";
    }
}
