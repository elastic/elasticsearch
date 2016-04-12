/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.env.Environment;

import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.security.cert.Certificate;
import java.util.ArrayList;
import java.util.List;

class PEMTrustConfig extends TrustConfig {

    final List<String> caPaths;

    PEMTrustConfig(boolean includeSystem, boolean reloadEnabled, List<String> caPaths) {
        super(includeSystem, reloadEnabled);
        this.caPaths = caPaths;
    }

    @Override
    X509ExtendedTrustManager[] nonSystemTrustManagers(@Nullable Environment environment) {
        try {
            Certificate[] certificates = CertUtils.readCertificates(caPaths, environment);
            return CertUtils.trustManagers(certificates);
        } catch (Exception e) {
            throw new ElasticsearchException("failed to initialize a TrustManagerFactory", e);
        }
    }

    @Override
    void validate() {
        if (caPaths == null) {
            throw new IllegalArgumentException("no ca paths have been configured");
        }
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        List<Path> paths = new ArrayList<>(caPaths.size());
        for (String path : caPaths) {
            paths.add(CertUtils.resolvePath(path, environment));
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
