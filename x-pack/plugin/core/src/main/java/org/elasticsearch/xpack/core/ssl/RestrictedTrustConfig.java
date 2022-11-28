/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import javax.net.ssl.X509ExtendedTrustManager;

/**
 * An implementation of {@link TrustConfig} that constructs a {@link RestrictedTrustManager}.
 * This implementation always wraps another <code>TrustConfig</code> to perform the
 * underlying certificate validation.
 */
public final class RestrictedTrustConfig extends TrustConfig {

    private static final String RESTRICTIONS_KEY_SUBJECT_NAME = "trust.subject_name";
    public static final String SAN_OTHER_COMMON = "subjectAltName.otherName.commonName";
    public static final String SAN_DNS = "subjectAltName.dnsName";
    static final Set<String> SUPPORTED_X_509_FIELDS = org.elasticsearch.core.Set.of(SAN_OTHER_COMMON, SAN_DNS);
    private final String groupConfigPath;
    private final TrustConfig delegate;
    private final Set<String> configuredX509Fields;

    RestrictedTrustConfig(String groupConfigPath, Set<String> configuredX509Fields, TrustConfig delegate) {
        this.configuredX509Fields = configuredX509Fields;
        this.groupConfigPath = Objects.requireNonNull(groupConfigPath);
        this.delegate = Objects.requireNonNull(delegate);
    }

    @Override
    RestrictedTrustManager createTrustManager(@Nullable Environment environment) {
        try {
            final X509ExtendedTrustManager delegateTrustManager = delegate.createTrustManager(environment);
            final CertificateTrustRestrictions trustGroupConfig = readTrustGroup(resolveGroupConfigPath(environment));
            return new RestrictedTrustManager(delegateTrustManager, trustGroupConfig, configuredX509Fields);
        } catch (IOException e) {
            throw new ElasticsearchException("failed to initialize TrustManager for {}", e, toString());
        }
    }

    @Override
    Collection<CertificateInfo> certificates(Environment environment) throws GeneralSecurityException, IOException {
        return delegate.certificates(environment);
    }

    @Override
    List<Path> filesToMonitor(@Nullable Environment environment) {
        return CollectionUtils.appendToCopy(delegate.filesToMonitor(environment), resolveGroupConfigPath(environment));
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

    private Path resolveGroupConfigPath(@Nullable Environment environment) {
        return CertParsingUtils.resolvePath(groupConfigPath, environment);
    }

    private CertificateTrustRestrictions readTrustGroup(Path path) throws IOException {
        Settings settings = Settings.builder().loadFromPath(path).build();
        final List<String> trustNodeNames = settings.getAsList(RESTRICTIONS_KEY_SUBJECT_NAME);
        return new CertificateTrustRestrictions(trustNodeNames);
    }
}
