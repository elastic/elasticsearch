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

import javax.net.ssl.SSLContext;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509ExtendedTrustManager;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

/**
 * A object encapsulating all necessary configuration for an SSL context (client or server).
 * The configuration itself is immutable, but the {@link #getKeyConfig() key config} and
 * {@link #getTrustConfig() trust config} may depend on reading key and certificate material
 * from files (see {@link #getDependentFiles()}, and the content of those files may change.
 */
public class SslConfiguration {

    /**
     * An ordered map of protocol algorithms to SSLContext algorithms. The map is ordered from most
     * secure to least secure. The names in this map are taken from the
     * <a href="https://docs.oracle.com/en/java/javase/11/docs/specs/security/standard-names.html#sslcontext-algorithms">
     * Java Security Standard Algorithm Names Documentation for Java 11</a>.
     */
    static final Map<String, String> ORDERED_PROTOCOL_ALGORITHM_MAP;
    static {
        LinkedHashMap<String, String> protocolAlgorithmMap = new LinkedHashMap<>();
        protocolAlgorithmMap.put("TLSv1.3", "TLSv1.3");
        protocolAlgorithmMap.put("TLSv1.2", "TLSv1.2");
        protocolAlgorithmMap.put("TLSv1.1", "TLSv1.1");
        protocolAlgorithmMap.put("TLSv1", "TLSv1");
        protocolAlgorithmMap.put("SSLv3", "SSLv3");
        protocolAlgorithmMap.put("SSLv2", "SSL");
        protocolAlgorithmMap.put("SSLv2Hello", "SSL");
        ORDERED_PROTOCOL_ALGORITHM_MAP = Collections.unmodifiableMap(protocolAlgorithmMap);
    }

    private final SslTrustConfig trustConfig;
    private final SslKeyConfig keyConfig;
    private final SslVerificationMode verificationMode;
    private final SslClientAuthenticationMode clientAuth;
    private final List<String> ciphers;
    private final List<String> supportedProtocols;

    public SslConfiguration(SslTrustConfig trustConfig, SslKeyConfig keyConfig, SslVerificationMode verificationMode,
                            SslClientAuthenticationMode clientAuth, List<String> ciphers, List<String> supportedProtocols) {
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

    public SslTrustConfig getTrustConfig() {
        return trustConfig;
    }

    public SslKeyConfig getKeyConfig() {
        return keyConfig;
    }

    public SslVerificationMode getVerificationMode() {
        return verificationMode;
    }

    public SslClientAuthenticationMode getClientAuth() {
        return clientAuth;
    }

    public List<String> getCipherSuites() {
        return ciphers;
    }

    public List<String> getSupportedProtocols() {
        return supportedProtocols;
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
     * Dynamically create a new SSL context based on the current state of the configuration.
     * Because the {@link #getKeyConfig() key config} and {@link #getTrustConfig() trust config} may change based on the
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
     * {@link #getSupportedProtocols() configured protocols}.
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
        throw new SslConfigException("no supported SSL/TLS protocol was found in the configured supported protocols: "
            + supportedProtocols);
    }

    @Override
    public String toString() {
        return getClass().getSimpleName() + '{' +
            "trustConfig=" + trustConfig +
            ", keyConfig=" + keyConfig +
            ", verificationMode=" + verificationMode +
            ", clientAuth=" + clientAuth +
            ", ciphers=" + ciphers +
            ", supportedProtocols=" + supportedProtocols +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final SslConfiguration that = (SslConfiguration) o;
        return Objects.equals(this.trustConfig, that.trustConfig) &&
            Objects.equals(this.keyConfig, that.keyConfig) &&
            this.verificationMode == that.verificationMode &&
            this.clientAuth == that.clientAuth &&
            Objects.equals(this.ciphers, that.ciphers) &&
            Objects.equals(this.supportedProtocols, that.supportedProtocols);
    }

    @Override
    public int hashCode() {
        return Objects.hash(trustConfig, keyConfig, verificationMode, clientAuth, ciphers, supportedProtocols);
    }
}
