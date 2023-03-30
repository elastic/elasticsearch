/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;

/**
 * A object encapsulating all necessary configuration for an SSL context (client or server).
 * The configuration itself is immutable, but the {@link #keyConfig() key config} and
 * {@link #trustConfig() trust config} may depend on reading key and certificate material
 * from files (see {@link #getDependentFiles()}, and the content of those files may change.
 */
public record SslConfiguration(
    String settingPrefix,
    boolean explicitlyConfigured,
    SslTrustConfig trustConfig,
    SslKeyConfig keyConfig,
    SslVerificationMode verificationMode,
    SslClientAuthenticationMode clientAuth,
    List<String> ciphers,
    List<String> supportedProtocols
) {

    /**
     * An ordered map of protocol algorithms to SSLContext algorithms. The map is ordered from most
     * secure to least secure. The names in this map are taken from the
     * <a href="https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#sslcontext-algorithms">
     * Java Security Standard Algorithm Names Documentation for Java 11</a>.
     */
    static final Map<String, String> ORDERED_PROTOCOL_ALGORITHM_MAP;

    static {
        LinkedHashMap<String, String> protocolAlgorithmMap = new LinkedHashMap<>();
        try {
            SSLContext.getInstance("TLSv1.3");
            protocolAlgorithmMap.put("TLSv1.3", "TLSv1.3");
        } catch (NoSuchAlgorithmException e) {
            // ignore since we support JVMs using BCJSSE in FIPS mode which doesn't support TLSv1.3
        }
        protocolAlgorithmMap.put("TLSv1.2", "TLSv1.2");
        protocolAlgorithmMap.put("TLSv1.1", "TLSv1.1");
        protocolAlgorithmMap.put("TLSv1", "TLSv1");
        protocolAlgorithmMap.put("SSLv3", "SSLv3");
        protocolAlgorithmMap.put("SSLv2", "SSL");
        protocolAlgorithmMap.put("SSLv2Hello", "SSL");
        ORDERED_PROTOCOL_ALGORITHM_MAP = Collections.unmodifiableMap(protocolAlgorithmMap);
    }

    public SslConfiguration(
        String settingPrefix,
        boolean explicitlyConfigured,
        SslTrustConfig trustConfig,
        SslKeyConfig keyConfig,
        SslVerificationMode verificationMode,
        SslClientAuthenticationMode clientAuth,
        List<String> ciphers,
        List<String> supportedProtocols
    ) {
        this.settingPrefix = settingPrefix;
        this.explicitlyConfigured = explicitlyConfigured;
        if (ciphers == null || ciphers.isEmpty()) {
            throw new SslConfigException("cannot configure SSL/TLS without any supported cipher suites");
        }
        if (supportedProtocols == null || supportedProtocols.isEmpty()) {
            throw new SslConfigException("cannot configure SSL/TLS without any supported protocols");
        }
        this.trustConfig = Objects.requireNonNull(trustConfig, "trust config cannot be null");
        this.keyConfig = Objects.requireNonNull(keyConfig, "key config cannot be null");
        this.verificationMode = Objects.requireNonNull(verificationMode, "verification mode cannot be null");
        this.clientAuth = Objects.requireNonNull(clientAuth, "client authentication cannot be null");
        this.ciphers = Collections.unmodifiableList(ciphers);
        this.supportedProtocols = Collections.unmodifiableList(supportedProtocols);
    }

    public List<String> getCipherSuites() {
        return ciphers;
    }

    /**
     * @return A collection of files that are used by this SSL configuration. If the contents of these files change, then any
     * subsequent call to {@link #createSslContext()} (or similar methods) may create a context with different behaviour.
     * It is recommended that these files be monitored for changes, and a new ssl-context is created whenever any of the files are modified.
     */
    public Collection<Path> getDependentFiles() {
        Set<Path> paths = new HashSet<>(keyConfig.getDependentFiles());
        paths.addAll(trustConfig.getDependentFiles());
        return paths;
    }

    /**
     * @return A collection of {@link StoredCertificate certificates} that are used by this SSL configuration.
     * This includes certificates used for identity (with a private key) and those used for trust, but excludes
     * certificates that are provided by the JRE.
     */
    public Collection<? extends StoredCertificate> getConfiguredCertificates() {
        List<StoredCertificate> certificates = new ArrayList<>();
        certificates.addAll(keyConfig.getConfiguredCertificates());
        certificates.addAll(trustConfig.getConfiguredCertificates());
        return certificates;
    }

    /**
     * Dynamically create a new SSL context based on the current state of the configuration.
     * Because the {@link #keyConfig() key config} and {@link #trustConfig() trust config} may change based on the
     * contents of their referenced files (see {@link #getDependentFiles()}, consecutive calls to this method may
     * return ssl-contexts with different configurations.
     */
    public SSLContext createSslContext() {
        final X509ExtendedKeyManager keyManager = keyConfig.createKeyManager();
        final X509ExtendedTrustManager trustManager = trustConfig.createTrustManager();
        try {
            SSLContext sslContext = SSLContext.getInstance(contextProtocol());
            sslContext.init(new X509ExtendedKeyManager[] { keyManager }, new X509ExtendedTrustManager[] { trustManager }, null);
            return sslContext;
        } catch (GeneralSecurityException e) {
            throw new SslConfigException("cannot create ssl context", e);
        }
    }

    /**
     * Picks the best (highest security / most recent standard) SSL/TLS protocol (/version) that is supported by the
     * {@link #supportedProtocols() configured protocols}.
     */
    private String contextProtocol() {
        if (supportedProtocols.isEmpty()) {
            throw new SslConfigException("no SSL/TLS protocols have been configured");
        }
        for (Entry<String, String> entry : ORDERED_PROTOCOL_ALGORITHM_MAP.entrySet()) {
            if (supportedProtocols.contains(entry.getKey())) {
                return entry.getValue();
            }
        }
        throw new SslConfigException(
            "no supported SSL/TLS protocol was found in the configured supported protocols: " + supportedProtocols
        );
    }

    // TODO Add explicitlyConfigured to equals&hashCode?
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SslConfiguration that = (SslConfiguration) o;
        return Objects.equals(this.settingPrefix, that.settingPrefix)
            && Objects.equals(this.trustConfig, that.trustConfig)
            && Objects.equals(this.keyConfig, that.keyConfig)
            && this.verificationMode == that.verificationMode
            && this.clientAuth == that.clientAuth
            && Objects.equals(this.ciphers, that.ciphers)
            && Objects.equals(this.supportedProtocols, that.supportedProtocols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(settingPrefix, trustConfig, keyConfig, verificationMode, clientAuth, ciphers, supportedProtocols);
    }
}
