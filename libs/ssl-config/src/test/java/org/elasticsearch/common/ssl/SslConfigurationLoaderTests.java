/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.ssl;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class SslConfigurationLoaderTests extends ESTestCase {

    private final Path certRoot = getDataPath("/certs/ca1/ca.crt").getParent().getParent();

    private Settings settings;
    private MockSecureSettings secureSettings = new MockSecureSettings();
    private SslConfigurationLoader loader = new SslConfigurationLoader("test.ssl.") {
        @Override
        protected boolean hasSettings(String prefix) {
            return true;
        }

        @Override
        protected String getSettingAsString(String key) throws Exception {
            return settings.get(key);
        }

        @Override
        protected char[] getSecureSetting(String key) throws Exception {
            final SecureString secStr = secureSettings.getString(key);
            return secStr == null ? null : secStr.getChars();
        }

        @Override
        protected List<String> getSettingAsList(String key) throws Exception {
            return settings.getAsList(key);
        }
    };

    /**
     * A test for non-trust, non-key configurations.
     * These are straight forward and can all be tested together
     */
    public void testBasicConfigurationOptions() {
        final SslVerificationMode verificationMode = randomFrom(SslVerificationMode.values());
        final SslClientAuthenticationMode clientAuth = randomFrom(SslClientAuthenticationMode.values());
        final String[] ciphers = generateRandomStringArray(8, 12, false, false);
        final String[] protocols = generateRandomStringArray(4, 5, false, false);
        settings = Settings.builder()
            .put("test.ssl.verification_mode", verificationMode.name().toLowerCase(Locale.ROOT))
            .put("test.ssl.client_authentication", clientAuth.name().toLowerCase(Locale.ROOT))
            .putList("test.ssl.cipher_suites", ciphers)
            .putList("test.ssl.supported_protocols", protocols)
            .build();
        final SslConfiguration configuration = loader.load(certRoot);
        assertThat(configuration.clientAuth(), is(clientAuth));
        assertThat(configuration.verificationMode(), is(verificationMode));
        assertThat(configuration.getCipherSuites(), equalTo(Arrays.asList(ciphers)));
        assertThat(configuration.supportedProtocols(), equalTo(Arrays.asList(protocols)));
        if (verificationMode == SslVerificationMode.NONE) {
            final SslTrustConfig trustConfig = configuration.trustConfig();
            assertThat(trustConfig, instanceOf(TrustEverythingConfig.class));
        }
    }

    public void testLoadTrustFromPemCAs() {
        settings = Settings.builder().putList("test.ssl.certificate_authorities", "ca1/ca.crt", "ca2/ca.crt", "ca3/ca.crt").build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslTrustConfig trustConfig = configuration.trustConfig();
        assertThat(trustConfig, instanceOf(PemTrustConfig.class));
        assertThat(
            trustConfig.getDependentFiles(),
            containsInAnyOrder(getDataPath("/certs/ca1/ca.crt"), getDataPath("/certs/ca2/ca.crt"), getDataPath("/certs/ca3/ca.crt"))
        );
        assertThat(trustConfig.createTrustManager(), notNullValue());
    }

    public void testLoadTrustFromPkcs12() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.truststore.path", "ca-all/ca.p12");
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.password", "p12-pass");
        } else {
            secureSettings.setString("test.ssl.truststore.secure_password", "p12-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.truststore.type", "PKCS12");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.algorithm", TrustManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslTrustConfig trustConfig = configuration.trustConfig();
        assertThat(trustConfig, instanceOf(StoreTrustConfig.class));
        assertThat(trustConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/ca-all/ca.p12")));
        assertThat(trustConfig.createTrustManager(), notNullValue());
    }

    public void testLoadTrustFromJKS() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.truststore.path", "ca-all/ca.jks");
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.password", "jks-pass");
        } else {
            secureSettings.setString("test.ssl.truststore.secure_password", "jks-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.truststore.type", "jks");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.truststore.algorithm", TrustManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslTrustConfig trustConfig = configuration.trustConfig();
        assertThat(trustConfig, instanceOf(StoreTrustConfig.class));
        assertThat(trustConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/ca-all/ca.jks")));
        assertThat(trustConfig.createTrustManager(), notNullValue());
    }

    public void testLoadKeysFromPemFiles() {
        final boolean usePassword = randomBoolean();
        final boolean useLegacyPassword = usePassword && randomBoolean();
        final String certName = usePassword ? "cert2" : "cert1";
        final Settings.Builder builder = Settings.builder()
            .put("test.ssl.certificate", certName + "/" + certName + ".crt")
            .put("test.ssl.key", certName + "/" + certName + ".key");
        if (usePassword) {
            if (useLegacyPassword) {
                builder.put("test.ssl.key_passphrase", "c2-pass");
            } else {
                secureSettings.setString("test.ssl.secure_key_passphrase", "c2-pass");
            }
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslKeyConfig keyConfig = configuration.keyConfig();
        assertThat(keyConfig, instanceOf(PemKeyConfig.class));
        assertThat(
            keyConfig.getDependentFiles(),
            containsInAnyOrder(
                getDataPath("/certs/" + certName + "/" + certName + ".crt"),
                getDataPath("/certs/" + certName + "/" + certName + ".key")
            )
        );
        assertThat(keyConfig.createKeyManager(), notNullValue());
    }

    public void testLoadKeysFromPKCS12() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.keystore.path", "cert-all/certs.p12");
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.password", "p12-pass");
        } else {
            secureSettings.setString("test.ssl.keystore.secure_password", "p12-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.keystore.type", "PKCS12");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.algorithm", KeyManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslKeyConfig keyConfig = configuration.keyConfig();
        assertThat(keyConfig, instanceOf(StoreKeyConfig.class));
        assertThat(keyConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/cert-all/certs.p12")));
        assertThat(keyConfig.createKeyManager(), notNullValue());
    }

    public void testLoadKeysFromJKS() {
        assumeFalse("Can't use JKS/PKCS12 keystores in a FIPS JVM", inFipsJvm());
        final Settings.Builder builder = Settings.builder().put("test.ssl.keystore.path", "cert-all/certs.jks");
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.password", "jks-pass");
        } else {
            secureSettings.setString("test.ssl.keystore.secure_password", "jks-pass");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.key_password", "key-pass");
        } else {
            secureSettings.setString("test.ssl.keystore.secure_key_password", "key-pass");
        }
        if (randomBoolean()) {
            // If this is not set, the loader will guess from the extension
            builder.put("test.ssl.keystore.type", "jks");
        }
        if (randomBoolean()) {
            builder.put("test.ssl.keystore.algorithm", KeyManagerFactory.getDefaultAlgorithm());
        }
        settings = builder.build();
        final SslConfiguration configuration = loader.load(certRoot);
        final SslKeyConfig keyConfig = configuration.keyConfig();
        assertThat(keyConfig, instanceOf(StoreKeyConfig.class));
        assertThat(keyConfig.getDependentFiles(), containsInAnyOrder(getDataPath("/certs/cert-all/certs.jks")));
        assertThat(keyConfig.createKeyManager(), notNullValue());
    }

    public void testChaCha20InCiphersOnJdk12Plus() {
        assertThat(SslConfigurationLoader.DEFAULT_CIPHERS, hasItem("TLS_CHACHA20_POLY1305_SHA256"));
        assertThat(SslConfigurationLoader.DEFAULT_CIPHERS, hasItem("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"));
        assertThat(SslConfigurationLoader.DEFAULT_CIPHERS, hasItem("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"));
    }
}
