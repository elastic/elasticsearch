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
import org.elasticsearch.common.ssl.KeyStoreUtil;
import org.elasticsearch.common.ssl.PemKeyConfig;
import org.elasticsearch.common.ssl.PemTrustConfig;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslConfigurationKeys;
import org.elasticsearch.common.ssl.SslKeyConfig;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.common.ssl.StoreKeyConfig;
import org.elasticsearch.env.Environment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.xpack.core.XPackSettings;
import org.junit.Before;

import java.nio.file.Path;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Map;
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
import static org.hamcrest.Matchers.hasKey;
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
        assertThat(globalConfig.keyConfig(), sameInstance(EmptyKeyConfig.INSTANCE));
        assertThat(globalConfig.trustConfig().getClass().getSimpleName(), is("DefaultJdkTrustConfig"));
        assertThat(globalConfig.supportedProtocols(), equalTo(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS));
        assertThat(globalConfig.supportedProtocols(), not(hasItem("TLSv1")));
    }

    public void testRemoteClusterSslConfigurationsWhenPortNotEnabled() {
        final Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            builder.put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), false);
        }
        final Map<String, Settings> settingsMap = SSLService.getSSLSettingsMap(builder.build());
        // Server (SSL is not built when port is not enabled)
        assertThat(settingsMap, not(hasKey(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX)));
        // Client (SSL is always built)
        assertThat(settingsMap, hasKey(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_PREFIX));
        final SslConfiguration sslConfiguration = getSslConfiguration(settingsMap.get(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_PREFIX));
        assertThat(sslConfiguration.keyConfig(), sameInstance(EmptyKeyConfig.INSTANCE));
        assertThat(sslConfiguration.trustConfig().getClass().getSimpleName(), is("DefaultJdkTrustConfig"));
        assertThat(sslConfiguration.supportedProtocols(), equalTo(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS));
        assertThat(sslConfiguration.supportedProtocols(), not(hasItem("TLSv1")));
        assertThat(sslConfiguration.verificationMode(), is(SslVerificationMode.FULL));
    }

    /**
     * Tests that the Remote Cluster port is configured if enabled and properly uses the default settings.
     */
    public void testRemoteClusterPortConfigurationIsInjectedWithDefaults() {
        Settings testSettings = Settings.builder().put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true).build();
        Map<String, Settings> settingsMap = SSLService.getSSLSettingsMap(testSettings);
        // Server
        assertThat(settingsMap, hasKey(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX));
        SslConfiguration sslConfiguration = getSslConfiguration(settingsMap.get(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX));
        assertThat(sslConfiguration.keyConfig(), sameInstance(EmptyKeyConfig.INSTANCE));
        assertThat(sslConfiguration.trustConfig().getClass().getSimpleName(), is("DefaultJdkTrustConfig"));
        assertThat(sslConfiguration.supportedProtocols(), equalTo(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS));
        assertThat(sslConfiguration.supportedProtocols(), not(hasItem("TLSv1")));
        assertThat(sslConfiguration.clientAuth(), is(SslClientAuthenticationMode.NONE));
        // Client
        assertThat(settingsMap, hasKey(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_PREFIX));
    }

    /**
     * Tests that settings are correctly read from the remote cluster SSL settings to set up the {@link SslConfiguration} object used by
     * all the SSL infrastructure to define per-profile settings. Makes sure to use both regular and secure settings to be sure both are
     * covered.
     */
    public void testRemoteClusterPortConfigurationIsInjectedWithItsSettings() {
        final Path path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString(
            XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX + SslConfigurationKeys.KEYSTORE_SECURE_PASSWORD,
            "testnode"
        );
        Settings testSettings = Settings.builder()
            .put(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.getKey(), true)
            .put(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX + SslConfigurationKeys.KEYSTORE_PATH, path)
            .putList(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX + SslConfigurationKeys.PROTOCOLS, "TLSv1.3", "TLSv1.2")
            .put(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX + SslConfigurationKeys.CLIENT_AUTH, "required")
            .put(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_PREFIX + SslConfigurationKeys.VERIFICATION_MODE, "certificate")
            .setSecureSettings(secureSettings)
            .build();
        Map<String, Settings> settingsMap = SSLService.getSSLSettingsMap(testSettings);

        // Server
        assertThat(settingsMap, hasKey(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX));
        SslConfiguration sslConfiguration = getSslConfiguration(settingsMap.get(XPackSettings.REMOTE_CLUSTER_SERVER_SSL_PREFIX));
        assertThat(sslConfiguration.supportedProtocols(), contains("TLSv1.3", "TLSv1.2"));
        assertThat(sslConfiguration.clientAuth(), is(SslClientAuthenticationMode.REQUIRED));

        SslKeyConfig keyStore = sslConfiguration.keyConfig();
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

        // Client
        assertThat(settingsMap, hasKey(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_PREFIX));
        final SslConfiguration clientSslConfiguration = getSslConfiguration(
            settingsMap.get(XPackSettings.REMOTE_CLUSTER_CLIENT_SSL_PREFIX)
        );
        assertThat(clientSslConfiguration.verificationMode(), is(SslVerificationMode.CERTIFICATE));
    }

    public void testThatOnlyKeystoreInSettingsSetsTruststoreSettings() {
        final Path path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "testnode");
        Settings settings = Settings.builder().put("keystore.path", path).setSecureSettings(secureSettings).build();
        // Pass settings in as component settings
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        SslKeyConfig keyStore = sslConfiguration.keyConfig();

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

    public void testFilterAppliedToKeystore() {
        final Path path = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.p12");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "testnode");
        Settings settings = Settings.builder().put("keystore.path", path).setSecureSettings(secureSettings).build();
        final SslConfiguration sslConfiguration = SslSettingsLoader.load(
            settings,
            null,
            environment,
            ks -> KeyStoreUtil.filter(ks, e -> e.getAlias().endsWith("sa")) // "_dsa" & "_rsa" but not "_ec"
        );
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig keyStore = (StoreKeyConfig) sslConfiguration.keyConfig();

        assertThat(keyStore.getDependentFiles(), contains(path));
        assertThat(keyStore.hasKeyMaterial(), is(true));

        assumeFalse("Cannot create Key Manager from a PKCS#12 file in FIPS", inFipsJvm());
        assertThat(keyStore.createKeyManager(), notNullValue());
        assertThat(keyStore.getKeys(false), hasSize(3)); // testnode_ec, testnode_rsa, testnode_dsa
        assertThat(keyStore.getKeys(true), hasSize(2)); // testnode_rsa, testnode_dsa
        assertThat(
            keyStore.getKeys(true).stream().map(t -> t.v1().getAlgorithm()).collect(Collectors.toUnmodifiableSet()),
            containsInAnyOrder("RSA", "DSA")
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
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig("path", PASSWORD, "type", null, PASSWORD, KEY_MGR_ALGORITHM, environment.configDir()))
        );
    }

    public void testKeystorePasswordBackcompat() {
        Settings settings = Settings.builder()
            .put("keystore.path", "path")
            .put("keystore.type", "type")
            .put("keystore.password", "password")
            .build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig("path", PASSWORD, "type", null, PASSWORD, KEY_MGR_ALGORITHM, environment.configDir()))
        );
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { configurationSettings.x509KeyPair.legacyKeystorePassword });
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
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig("path", PASSWORD, "type", null, KEYPASS, KEY_MGR_ALGORITHM, environment.configDir()))
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
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig("path", PASSWORD, "type", null, KEYPASS, KEY_MGR_ALGORITHM, environment.configDir()))
        );
        assertSettingDeprecationsAndWarnings(
            new Setting<?>[] {
                configurationSettings.x509KeyPair.legacyKeystorePassword,
                configurationSettings.x509KeyPair.legacyKeystoreKeyPassword }
        );
    }

    public void testInferKeystoreTypeFromJksFile() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        Settings settings = Settings.builder().put("keystore.path", "xpack/tls/path.jks").setSecureSettings(secureSettings).build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig("xpack/tls/path.jks", PASSWORD, "jks", null, KEYPASS, KEY_MGR_ALGORITHM, environment.configDir()))
        );
    }

    public void testInferKeystoreTypeFromPkcs12File() {
        final String ext = randomFrom("p12", "pfx", "pkcs12");
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        final String path = "xpack/tls/path." + ext;
        Settings settings = Settings.builder().put("keystore.path", path).setSecureSettings(secureSettings).build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig(path, PASSWORD, "PKCS12", null, KEYPASS, KEY_MGR_ALGORITHM, environment.configDir()))
        );
    }

    public void testInferKeystoreTypeFromUnrecognised() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("keystore.secure_password", "password");
        secureSettings.setString("keystore.secure_key_password", "keypass");
        Settings settings = Settings.builder().put("keystore.path", "xpack/tls/path.foo").setSecureSettings(secureSettings).build();
        SslConfiguration sslConfiguration = getSslConfiguration(settings);
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(
            ksKeyInfo,
            equalTo(new StoreKeyConfig("xpack/tls/path.foo", PASSWORD, "jks", null, KEYPASS, KEY_MGR_ALGORITHM, environment.configDir()))
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
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(ksKeyInfo, equalTo(new StoreKeyConfig(path, PASSWORD, type, null, KEYPASS, KEY_MGR_ALGORITHM, environment.configDir())));
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
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder().put("keystore.path", "path").build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder().put("keystore.path", "path1").build());
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(false)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(false)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentTruststoresAreNotEqual() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder().put("truststore.path", "/trust").build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder().put("truststore.path", "/truststore").build());
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
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder().put("keystore.path", "path").build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder().put("keystore.path", "path1").build());
        assertThat(sslConfiguration.hashCode(), is(not(equalTo(sslConfiguration1.hashCode()))));
    }

    public void testThatSettingsWithDifferentTruststoresHaveDifferentHashCode() {
        SslConfiguration sslConfiguration = getSslConfiguration(Settings.builder().put("truststore.path", "/trust").build());
        SslConfiguration sslConfiguration1 = getSslConfiguration(Settings.builder().put("truststore.path", "/truststore").build());
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
        assertThat(config.keyConfig(), instanceOf(PemKeyConfig.class));
        PemKeyConfig keyConfig = (PemKeyConfig) config.keyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertCombiningTrustConfigContainsCorrectIssuers(config);
    }

    public void testPEMFileBackcompat() {
        Settings settings = Settings.builder()
            .put("key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("key_passphrase", "testnode")
            .put("certificate", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .build();

        SslConfiguration config = getSslConfiguration(settings);
        assertThat(config.keyConfig(), instanceOf(PemKeyConfig.class));
        SslKeyConfig keyConfig = config.keyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertCombiningTrustConfigContainsCorrectIssuers(config);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { configurationSettings.x509KeyPair.legacyKeyPassword });
    }

    public void testPEMKeyAndTrustFiles() {
        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("certificate", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .putList(
                "certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt").toString(),
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString()
            )
            .setSecureSettings(secureSettings)
            .build();

        SslConfiguration config = getSslConfiguration(settings);
        assertThat(config.keyConfig(), instanceOf(PemKeyConfig.class));
        PemKeyConfig keyConfig = (PemKeyConfig) config.keyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertThat(config.trustConfig(), not(sameInstance(keyConfig)));
        assertThat(config.trustConfig(), instanceOf(PemTrustConfig.class));
        TrustManager trustManager = keyConfig.asTrustConfig().createTrustManager();
        assertNotNull(trustManager);
    }

    public void testPEMKeyAndTrustFilesBackcompat() {
        Settings settings = Settings.builder()
            .put("key", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("key_passphrase", "testnode")
            .put("certificate", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .putList(
                "certificate_authorities",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt").toString(),
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString()
            )
            .build();

        SslConfiguration config = getSslConfiguration(settings);
        assertThat(config.keyConfig(), instanceOf(PemKeyConfig.class));
        PemKeyConfig keyConfig = (PemKeyConfig) config.keyConfig();
        KeyManager keyManager = keyConfig.createKeyManager();
        assertNotNull(keyManager);
        assertThat(config.trustConfig(), not(sameInstance(keyConfig)));
        assertThat(config.trustConfig(), instanceOf(PemTrustConfig.class));
        TrustManager trustManager = keyConfig.asTrustConfig().createTrustManager();
        assertNotNull(trustManager);
        assertSettingDeprecationsAndWarnings(new Setting<?>[] { configurationSettings.x509KeyPair.legacyKeyPassword });
    }

    public void testExplicitlyConfigured() {
        assertThat(SslSettingsLoader.load(Settings.EMPTY, null, environment).explicitlyConfigured(), is(false));
        assertThat(
            SslSettingsLoader.load(
                Settings.builder()
                    .put("cluster.name", randomAlphaOfLength(8))
                    .put("xpack.security.transport.ssl.certificate", randomAlphaOfLength(12))
                    .put("xpack.security.transport.ssl.key", randomAlphaOfLength(12))
                    .build(),
                "xpack.http.ssl.",
                environment
            ).explicitlyConfigured(),
            is(false)
        );

        assertThat(
            SslSettingsLoader.load(
                Settings.builder().put("verification_mode", randomFrom(SslVerificationMode.values()).name()).build(),
                null,
                environment
            ).explicitlyConfigured(),
            is(true)
        );

        assertThat(
            SslSettingsLoader.load(
                Settings.builder().putList("xpack.security.transport.ssl.truststore.path", "truststore.p12").build(),
                "xpack.security.transport.ssl.",
                environment
            ).explicitlyConfigured(),
            is(true)
        );
    }

    private void assertCombiningTrustConfigContainsCorrectIssuers(SslConfiguration sslConfig) {
        assertThat(sslConfig.trustConfig(), instanceOf(CompositeTrustConfig.class));
        X509Certificate[] trustConfAcceptedIssuers = sslConfig.trustConfig().createTrustManager().getAcceptedIssuers();
        X509Certificate[] keyConfAcceptedIssuers = sslConfig.keyConfig().asTrustConfig().createTrustManager().getAcceptedIssuers();
        X509Certificate[] defaultAcceptedIssuers = DefaultJdkTrustConfig.DEFAULT_INSTANCE.createTrustManager().getAcceptedIssuers();
        assertEquals(keyConfAcceptedIssuers.length + defaultAcceptedIssuers.length, trustConfAcceptedIssuers.length);
        assertThat(Arrays.asList(keyConfAcceptedIssuers), everyItem(is(in(trustConfAcceptedIssuers))));
        assertThat(Arrays.asList(defaultAcceptedIssuers), everyItem(is(in(trustConfAcceptedIssuers))));
    }
}
