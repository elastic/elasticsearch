/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.ssl.cert.CertificateInfo;

import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents the configuration for an SSLContext
 */
public final class SSLConfiguration {

    // These settings are never registered, but they exist so that we can parse the values defined under grouped settings. Also, some are
    // implemented as optional settings, which provides a declarative manner for fallback as we typically fallback to values from a
    // different configuration
    static final SSLConfigurationSettings SETTINGS_PARSER = SSLConfigurationSettings.withoutPrefix(true);

    private final KeyConfig keyConfig;
    private final TrustConfig trustConfig;
    private final List<String> ciphers;
    private final List<String> supportedProtocols;
    private final SSLClientAuth sslClientAuth;
    private final VerificationMode verificationMode;
    private final boolean explicitlyConfigured;

    /**
     * Creates a new SSLConfiguration from the given settings. There is no fallback configuration when invoking this constructor so
     * un-configured aspects will take on their default values.
     *
     * @param settings the SSL specific settings; only the settings under a *.ssl. prefix
     */
    public SSLConfiguration(Settings settings) {
        this.keyConfig = createKeyConfig(settings);
        this.trustConfig = createTrustConfig(settings, keyConfig);
        this.ciphers = getListOrDefault(SETTINGS_PARSER.ciphers, settings, XPackSettings.DEFAULT_CIPHERS);
        this.supportedProtocols = getListOrDefault(SETTINGS_PARSER.supportedProtocols, settings, XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS);
        this.sslClientAuth = SETTINGS_PARSER.clientAuth.get(settings).orElse(XPackSettings.CLIENT_AUTH_DEFAULT);
        this.verificationMode = SETTINGS_PARSER.verificationMode.get(settings).orElse(XPackSettings.VERIFICATION_MODE_DEFAULT);
        this.explicitlyConfigured = settings.isEmpty() == false;
    }

    /**
     * The configuration for the key, if any, that will be used as part of this ssl configuration
     */
    public KeyConfig keyConfig() {
        return keyConfig;
    }

    /**
     * The configuration of trust material that will be used as part of this ssl configuration
     */
    TrustConfig trustConfig() {
        return trustConfig;
    }

    /**
     * The cipher suites that will be used for this ssl configuration
     */
    List<String> cipherSuites() {
        return ciphers;
    }

    /**
     * The protocols that are supported by this configuration
     */
    List<String> supportedProtocols() {
        return supportedProtocols;
    }

    /**
     * The verification mode for this configuration; this mode controls certificate and hostname verification
     */
    public VerificationMode verificationMode() {
        return verificationMode;
    }

    /**
     * The client auth configuration
     */
    SSLClientAuth sslClientAuth() {
        return sslClientAuth;
    }

    /**
     * Provides the list of paths to files that back this configuration
     */
    List<Path> filesToMonitor(@Nullable Environment environment) {
        if (keyConfig() == trustConfig()) {
            return keyConfig().filesToMonitor(environment);
        }
        List<Path> paths = new ArrayList<>(keyConfig().filesToMonitor(environment));
        paths.addAll(trustConfig().filesToMonitor(environment));
        return paths;
    }

    public boolean isExplicitlyConfigured() {
        return explicitlyConfigured;
    }

    @Override
    public String toString() {
        return "SSLConfiguration{" +
                "keyConfig=[" + keyConfig +
                "], trustConfig=" + trustConfig +
                "], cipherSuites=[" + ciphers +
                "], supportedProtocols=[" + supportedProtocols +
                "], sslClientAuth=[" + sslClientAuth +
                "], verificationMode=[" + verificationMode +
                "]}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if ((o instanceof SSLConfiguration) == false) return false;

        SSLConfiguration that = (SSLConfiguration) o;

        return Objects.equals(this.keyConfig(), that.keyConfig())
            && Objects.equals(this.trustConfig(), that.trustConfig())
            && Objects.equals(this.cipherSuites(), that.cipherSuites())
            && Objects.equals(this.supportedProtocols(), that.supportedProtocols())
            && Objects.equals(this.verificationMode(), that.verificationMode())
            && Objects.equals(this.sslClientAuth(), that.sslClientAuth());
    }

    @Override
    public int hashCode() {
        int result = this.keyConfig() != null ? this.keyConfig().hashCode() : 0;
        result = 31 * result + (this.trustConfig() != null ? this.trustConfig().hashCode() : 0);
        result = 31 * result + (this.cipherSuites() != null ? this.cipherSuites().hashCode() : 0);
        result = 31 * result + (this.supportedProtocols() != null ? this.supportedProtocols().hashCode() : 0);
        result = 31 * result + this.verificationMode().hashCode();
        result = 31 * result + this.sslClientAuth().hashCode();
        return result;
    }

    private static KeyConfig createKeyConfig(Settings settings) {
        final String trustStoreAlgorithm = SETTINGS_PARSER.truststoreAlgorithm.get(settings);
        final KeyConfig config = CertParsingUtils.createKeyConfig(SETTINGS_PARSER.x509KeyPair, settings, trustStoreAlgorithm);
        return config == null ? KeyConfig.NONE : config;
    }

    private static TrustConfig createTrustConfig(Settings settings, KeyConfig keyConfig) {
        final TrustConfig trustConfig = createCertChainTrustConfig(settings, keyConfig);
        return SETTINGS_PARSER.trustRestrictionsPath.get(settings)
                .map(path -> (TrustConfig) new RestrictedTrustConfig(path, trustConfig))
                .orElse(trustConfig);
    }

    private static TrustConfig createCertChainTrustConfig(Settings settings, KeyConfig keyConfig) {
        String trustStorePath = SETTINGS_PARSER.truststorePath.get(settings).orElse(null);
        String trustStoreType = SSLConfigurationSettings.getKeyStoreType(SETTINGS_PARSER.truststoreType, settings, trustStorePath);
        List<String> caPaths = getListOrNull(SETTINGS_PARSER.caPaths, settings);
        if (trustStorePath != null && caPaths != null) {
            throw new IllegalArgumentException("you cannot specify a truststore and ca files");
        }

        VerificationMode verificationMode = SETTINGS_PARSER.verificationMode.get(settings).orElse(XPackSettings.VERIFICATION_MODE_DEFAULT);
        if (verificationMode.isCertificateVerificationEnabled() == false) {
            return TrustAllConfig.INSTANCE;
        } else if (caPaths != null) {
            return new PEMTrustConfig(caPaths);
        } else if (trustStorePath != null) {
            String trustStoreAlgorithm = SETTINGS_PARSER.truststoreAlgorithm.get(settings);
            SecureString trustStorePassword = SETTINGS_PARSER.truststorePassword.get(settings);
            return new StoreTrustConfig(trustStorePath, trustStoreType, trustStorePassword, trustStoreAlgorithm);
        } else if (keyConfig != KeyConfig.NONE) {
            return DefaultJDKTrustConfig.merge(keyConfig, getDefaultTrustStorePassword(settings));
        } else {
            return new DefaultJDKTrustConfig(getDefaultTrustStorePassword(settings));
        }
    }

    private static SecureString getDefaultTrustStorePassword(Settings settings) {
        // We only handle the default store password if it's a PKCS#11 token
        if (System.getProperty("javax.net.ssl.trustStoreType", "").equalsIgnoreCase("PKCS11")) {
            try (SecureString systemTrustStorePassword =
                     new SecureString(System.getProperty("javax.net.ssl.trustStorePassword", "").toCharArray())) {
                if (systemTrustStorePassword.length() == 0) {
                    try (SecureString trustStorePassword = SETTINGS_PARSER.truststorePassword.get(settings)) {
                        return trustStorePassword;
                    }
                }
                return systemTrustStorePassword;
            }
        }
        return null;
    }

    private static List<String> getListOrNull(Setting<List<String>> listSetting, Settings settings) {
        return getListOrDefault(listSetting, settings, null);
    }

    private static List<String> getListOrDefault(Setting<List<String>> listSetting, Settings settings, List<String> defaultValue) {
        if (listSetting.exists(settings)) {
            return listSetting.get(settings);
        }
        return defaultValue;
    }

    /**
     * Returns information about each certificate that referenced by this SSL configurations.
     * This includes certificates used for identity (with a private key) and those used for trust, but excludes
     * certificates that are provided by the JRE.
     * @see TrustConfig#certificates(Environment)
     */
    List<CertificateInfo> getDefinedCertificates(@Nullable Environment environment) throws GeneralSecurityException, IOException {
        List<CertificateInfo> certificates = new ArrayList<>();
        certificates.addAll(keyConfig.certificates(environment));
        certificates.addAll(trustConfig.certificates(environment));
        return certificates;
    }
}
