/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslTrustConfig;
import org.elasticsearch.common.ssl.StoredCertificate;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import javax.net.ssl.X509ExtendedTrustManager;

/**
 * An implementation of {@link SslTrustConfig} that constructs a {@link RestrictedTrustManager}.
 * This implementation always wraps another <code>TrustConfig</code> to perform the
 * underlying certificate validation.
 */
public final class RestrictedTrustConfig implements SslTrustConfig {

    private static final String RESTRICTIONS_KEY_SUBJECT_NAME = "trust.subject_name";
    private final Path groupConfigPath;
    private final SslTrustConfig delegate;

    RestrictedTrustConfig(Path groupConfigPath, SslTrustConfig delegate) {
        this.groupConfigPath = Objects.requireNonNull(groupConfigPath);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    public RestrictedTrustManager createTrustManager() {
        try {
            final X509ExtendedTrustManager delegateTrustManager = delegate.createTrustManager();
            final CertificateTrustRestrictions trustGroupConfig = readTrustGroup(groupConfigPath);
            return new RestrictedTrustManager(delegateTrustManager, trustGroupConfig);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to initialize TrustManager for {}", e, toString());
        }
    }

    @Override
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        return delegate.getConfiguredCertificates();
    }

    @Override
    public Collection<Path> getDependentFiles() {
        return CollectionUtils.appendToCopy(delegate.getDependentFiles(), groupConfigPath);
    }

    @Override
    public String toString() {
        return "restrictedTrust=[" + groupConfigPath + ']';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RestrictedTrustConfig that = (RestrictedTrustConfig) o;
        return this.groupConfigPath.equals(that.groupConfigPath) && this.delegate.equals(that.delegate);
    }

    @Override
    public int hashCode() {
        int result = groupConfigPath.hashCode();
        result = 31 * result + delegate.hashCode();
        return result;
    }

    private CertificateTrustRestrictions readTrustGroup(Path path) throws IOException {
        Settings settings = Settings.builder().loadFromPath(path).build();
        final List<String> trustNodeNames = settings.getAsList(RESTRICTIONS_KEY_SUBJECT_NAME);
        return new CertificateTrustRestrictions(trustNodeNames);
    }
}
