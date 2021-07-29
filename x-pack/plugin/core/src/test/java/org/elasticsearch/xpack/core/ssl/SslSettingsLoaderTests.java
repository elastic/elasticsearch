/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.CompositeTrustConfig;
import org.elasticsearch.common.ssl.DefaultJdkTrustConfig;
import org.elasticsearch.common.ssl.EmptyKeyConfig;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.ssl.StoreKeyConfig;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.XPackSettings;
import org.junit.Before;

import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.stream.Collectors;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedKeyManager;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SslSettingsLoaderTests extends ESTestCase {

    private static final char[] PASSWORD = "password".toCharArray();
    private static final char[] KEYPASS = "keypass".toCharArray();
    private static final String KEY_MGR_ALGORITHM = KeyManagerFactory.getDefaultAlgorithm();

    private SSLConfigurationSettings configurationSettings;
    private Environment environment;

    @Before
    public void setupTest() {
        configurationSettings = SSLConfigurationSettings.withoutPrefix(true);
        environment = newEnvironment();
    }

    public void testThatSslConfigurationHasCorrectDefaults() {
        SslConfiguration globalConfig = getSslConfiguration(Settings.EMPTY);
        assertThat(globalConfig.getKeyConfig(), sameInstance(EmptyKeyConfig.INSTANCE));
        assertThat(globalConfig.getTrustConfig().getClass().getSimpleName(), is("DefaultJdkTrustConfig"));
        assertThat(globalConfig.getSupportedProtocols(), equalTo(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS));
        assertThat(globalConfig.getSupportedProtocols(), not(hasItem("TLSv1")));
    }

    public void testThatOnlyKeystoreInSettingsSetsTruststoreSettings() {
        final Path path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "testnode");
        Settings settings = Settings.builder()
            .put("keystore.path", path)
            .setSecureSettings(secureSettings)
            .build();
        // Pass settings in as component settings
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        SslKeyConfig keyStore = sslConfiguration.getKeyConfig();

        assertThat(keyStore.getDependentFiles(), contains(path));
        assertThat(keyStore.hasKeyMaterial(), is(true));
        final X509ExtendedKeyManager keyManager = keyStore.createKeyManager();
        assertThat(keyManager, notNullValue());
        assertThat(keyStore.getKeys(), hasSize(3)); // testnode_ec, testnode_rsa, testnode_dsa
        assertThat(
            keyStore.getKeys().stream().map(t -> t.v1().getAlgorithm()).collect(Collectors.toUnmodifiableSet()),
            containsInAnyOrder("RSA", "DSA", "EC")
        );

        assertCombiningTrustConfigContainsCorrectIssuers(sslConfiguration);
    }

    private SslConfiguration getSslConfiguration(Settings settings) {
        return SslSettingsLoader.load(settings, null, environment);
    }

    public void testKeystorePassword() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        Settings settings = Settings.builder()
            .put("keystore.path", "path")
            .put("keystore.type", "type")
            .setSecureSettings(secureSettings)
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(
                new StoreKeyConfig("path", PASSWORD, "type", PASSWORD, KEY_MGR_ALGORITHM, environment.configFile())
            )
        );
    }

    public void testKeystorePasswordBackcompat() {
        Settings settings = Settings.builder()
            .put("keystore.path", "path")
            .put("keystore.type", "type")
            .put("keystore.password", "password")
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(
                new StoreKeyConfig("path", PASSWORD, "type", PASSWORD, KEY_MGR_ALGORITHM, environment.configFile())
            )
        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            configurationSettings.x509KeyPair.legacyKeystorePassword});
    }

    public void testKeystoreKeyPassword() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        Settings settings = Settings.builder()
            .put("keystore.path", "path")
            .put("keystore.type", "type")
            .setSecureSettings(secureSettings)
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(
                new StoreKeyConfig("path", PASSWORD, "type", KEYPASS, KEY_MGR_ALGORITHM, environment.configFile())
            )
        );
    }

    public void testKeystoreKeyPasswordBackcompat() {
        Settings settings = Settings.builder()
            .put("keystore.path", "path")
            .put("keystore.type", "type")
            .put("keystore.password", "password")
            .put("keystore.key_password", "keypass")
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(
                new StoreKeyConfig("path", PASSWORD, "type", KEYPASS, KEY_MGR_ALGORITHM, environment.configFile())
            )
        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{
            configurationSettings.x509KeyPair.legacyKeystorePassword,
            configurationSettings.x509KeyPair.legacyKeystoreKeyPassword
        });
    }

    public void testInferKeystoreTypeFromJksFile() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        Settings settings = Settings.builder()
            .put("keystore.path", "xpack/tls/path.jks")
            .setSecureSettings(secureSettings)
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(
                new StoreKeyConfig("xpack/tls/path.jks", PASSWORD, "jks", KEYPASS, KEY_MGR_ALGORITHM, environment.configFile())
            )
        );
    }

    public void testInferKeystoreTypeFromPkcs12File() {
        final String ext = randomFrom("p12", "pfx", "pkcs12");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        final String path = "xpack/tls/path." + ext;
        Settings settings = Settings.builder()
            .put("keystore.path", path)
            .setSecureSettings(secureSettings)
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(
                new StoreKeyConfig(path, PASSWORD, "PKCS12", KEYPASS, KEY_MGR_ALGORITHM, environment.configFile())
            )
        );
    }

    public void testInferKeystoreTypeFromUnrecognised() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        Settings settings = Settings.builder()
            .put("keystore.path", "xpack/tls/path.foo")
            .setSecureSettings(secureSettings)
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig("xpack/tls/path.foo", PASSWORD, "jks", KEYPASS, KEY_MGR_ALGORITHM, environment.configFile()))
        );
    }

    public void testExplicitKeystoreType() {
        final String ext = randomFrom("p12", "jks");
        final String type = randomAlphaOfLengthBetween(2, 8);
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        final String path = "xpack/tls/path." + ext;
        Settings settings = Settings.builder()
            .put("keystore.path", path)
            .put("keystore.type", type)
            .setSecureSettings(secureSettings)
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.getKeyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.getKeyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig(path, PASSWORD, type, KEYPASS, KEY_MGR_ALGORITHM, environment.configFile()))
        );
    }

    public void testThatEmptySettingsAreEqual() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.EMPTY);
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.EMPTY);
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentKeystoresAreNotEqual() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder()
            .put("keystore.path", "path")
            .build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder()
            .put("keystore.path", "path1")
            .build());
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(false)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(false)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentTruststoresAreNotEqual() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder()
            .put("truststore.path", "/trust")
            .build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder()
            .put("truststore.path", "/truststore")
            .build());
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(false)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(false)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));
    }

    public void testThatEmptySettingsHaveSameHashCode() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.EMPTY);
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.EMPTY);
        assertThat(sslConfiguration.hashCode(), is(equalTo(sslConfiguration1.hashCode())));
    }

    public void testThatSettingsWithDifferentKeystoresHaveDifferentHashCode() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder()
            .put("keystore.path", "path")
            .build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder()
            .put("keystore.path", "path1")
            .build());
        assertThat(sslConfiguration.hashCode(), is(not(equalTo(sslConfiguration1.hashCode()))));
    }

    public void testThatSettingsWithDifferentTruststoresHaveDifferentHashCode() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder()
            .put("truststore.path", "/trust")
            .build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder()
            .put("truststore.path", "/truststore")
            .build());
        assertThat(sslConfiguration.hashCode(), is(not(equalTo(sslConfiguration1.hashCode()))));
    }

    public void testPEMFile() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("certificate", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .setSecureSettings(secureSettings)
            .build();

        SslConfiguration config = getSslConfiguration(settings);
        assertThat(config.getKeyConfig(), instanceOf(PemKeyConfig.class));
        PemKeyConfig keyConfig = (PemKeyConfig) config.getKeyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertCombiningTrustConfigContainsCorrectIssuers(config);
    }

    public void testPEMFileBackcompat() {
        Settings settings = Settings.builder()
            .put("key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("key_passphrase", "testnode")
            .put("certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .build();

        SslConfiguration config = getSslConfiguration(settings);
        assertThat(config.getKeyConfig(), instanceOf(PemKeyConfig.class));
        SslKeyConfig keyConfig = config.getKeyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertCombiningTrustConfigContainsCorrectIssuers(config);
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{configurationSettings.x509KeyPair.legacyKeyPassword});
    }

    public void testPEMKeyAndTrustFiles() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("certificate", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .putList("certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt").toString(),
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString())
            .setSecureSettings(secureSettings)
            .build();

        SslConfiguration config = getSslConfiguration(settings);
        assertThat(config.getKeyConfig(), instanceOf(PemKeyConfig.class));
        PemKeyConfig keyConfig = (PemKeyConfig) config.getKeyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertThat(config.getTrustConfig(), not(sameInstance(keyConfig)));
        assertThat(config.getTrustConfig(), instanceOf(PemTrustConfig.class));
        TrustManager trustManager = keyConfig.asTrustConfig().createTrustManager();
        assertNotNull(trustManager);
    }

    public void testPEMKeyAndTrustFilesBackcompat() {
        Settings settings = Settings.builder()
            .put("key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("key_passphrase", "testnode")
            .put("certificate", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .putList("certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt").toString(),
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString())
            .build();

        SslConfiguration config = getSslConfiguration(settings);
        assertThat(config.getKeyConfig(), instanceOf(PemKeyConfig.class));
        PemKeyConfig keyConfig = (PemKeyConfig) config.getKeyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertThat(config.getTrustConfig(), not(sameInstance(keyConfig)));
        assertThat(config.getTrustConfig(), instanceOf(PemTrustConfig.class));
        TrustManager trustManager = keyConfig.asTrustConfig().createTrustManager();
        assertNotNull(trustManager);
        assertSettingDeprecationsAndWarnings(new Setting<?>[]{configurationSettings.x509KeyPair.legacyKeyPassword});
    }

    public void testExplicitlyConfigured() {
        assertThat(SslSettingsLoader.load(Settings.EMPTY, null, environment).isExplicitlyConfigured(), is(false));
        assertThat(SslSettingsLoader.load(
            Settings.builder()
                .put("cluster.name", randomAlphaOfLength(8))
                .put("xpack.security.transport.ssl.certificate", randomAlphaOfLength(12))
                .put("xpack.security.transport.ssl.key", randomAlphaOfLength(12))
                .build(),
            "xpack.http.ssl.",
            environment
        ).isExplicitlyConfigured(), is(false));

        assertThat(SslSettingsLoader.load(
            Settings.builder().put("verification_mode", randomFrom(SslVerificationMode.values()).name()).build(),
            null,
            environment
        ).isExplicitlyConfigured(), is(true));

        assertThat(SslSettingsLoader.load(
            Settings.builder().putList("xpack.security.transport.ssl.truststore.path", "truststore.p12").build(),
            "xpack.security.transport.ssl.",
            environment
        ).isExplicitlyConfigured(), is(true));
    }

    private void assertCombiningTrustConfigContainsCorrectIssuers(SslConfiguration sslConfig) {
        assertThat(sslConfig.getTrustConfig(), instanceOf(CompositeTrustConfig.class));
        X509Certificate[] trustConfAcceptedIssuers = sslConfig.getTrustConfig().createTrustManager().getAcceptedIssuers();
        X509Certificate[] keyConfAcceptedIssuers = sslConfig.getKeyConfig().asTrustConfig().createTrustManager().getAcceptedIssuers();
        X509Certificate[] defaultAcceptedIssuers = DefaultJdkTrustConfig.DEFAULT_INSTANCE.createTrustManager().getAcceptedIssuers();
        assertEquals(keyConfAcceptedIssuers.length + defaultAcceptedIssuers.length, trustConfAcceptedIssuers.length);
        assertThat(Arrays.asList(keyConfAcceptedIssuers), everyItem(is(in(trustConfAcceptedIssuers))));
        assertThat(Arrays.asList(defaultAcceptedIssuers), everyItem(is(in(trustConfAcceptedIssuers))));
    }
}
