/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.env.Environment;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Custom;
import org.elasticsearch.xpack.security.ssl.SSLConfiguration.Global;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class SSLConfigurationTests extends ESTestCase {

    public void testThatSSLConfigurationHasCorrectDefaults() {
        SSLConfiguration globalConfig = new Global(Settings.EMPTY);
        assertThat(globalConfig.keyConfig(), sameInstance(KeyConfig.NONE));
        assertThat(globalConfig.trustConfig(), is(not((globalConfig.keyConfig()))));
        assertThat(globalConfig.trustConfig(), instanceOf(StoreTrustConfig.class));
        assertThat(globalConfig.sessionCacheSize(), is(equalTo(Global.DEFAULT_SESSION_CACHE_SIZE)));
        assertThat(globalConfig.sessionCacheTimeout(), is(equalTo(Global.DEFAULT_SESSION_CACHE_TIMEOUT)));
        assertThat(globalConfig.protocol(), is(equalTo(Global.DEFAULT_PROTOCOL)));

        SSLConfiguration scopedConfig = new Custom(Settings.EMPTY, globalConfig);
        assertThat(scopedConfig.keyConfig(), sameInstance(globalConfig.keyConfig()));
        assertThat(scopedConfig.trustConfig(), sameInstance(globalConfig.trustConfig()));
        assertThat(globalConfig.sessionCacheSize(), is(equalTo(Global.DEFAULT_SESSION_CACHE_SIZE)));
        assertThat(globalConfig.sessionCacheTimeout(), is(equalTo(Global.DEFAULT_SESSION_CACHE_TIMEOUT)));
        assertThat(globalConfig.protocol(), is(equalTo(Global.DEFAULT_PROTOCOL)));
    }

    public void testThatSSLConfigurationWithoutAutoGenHasCorrectDefaults() {
        SSLConfiguration globalSettings = new Global(Settings.EMPTY);
        SSLConfiguration scopedSettings = new Custom(Settings.EMPTY, globalSettings);
        for (SSLConfiguration sslConfiguration : Arrays.asList(globalSettings, scopedSettings)) {
            assertThat(sslConfiguration.keyConfig(), sameInstance(KeyConfig.NONE));
            assertThat(sslConfiguration.sessionCacheSize(), is(equalTo(Global.DEFAULT_SESSION_CACHE_SIZE)));
            assertThat(sslConfiguration.sessionCacheTimeout(), is(equalTo(Global.DEFAULT_SESSION_CACHE_TIMEOUT)));
            assertThat(sslConfiguration.protocol(), is(equalTo(Global.DEFAULT_PROTOCOL)));
            assertThat(sslConfiguration.trustConfig(), notNullValue());
            assertThat(sslConfiguration.trustConfig(), is(instanceOf(StoreTrustConfig.class)));

            StoreTrustConfig ksTrustInfo = (StoreTrustConfig) sslConfiguration.trustConfig();
            assertThat(ksTrustInfo.trustStorePath, is(nullValue()));
            assertThat(ksTrustInfo.trustStorePassword, is(nullValue()));
            assertThat(ksTrustInfo.trustStoreAlgorithm, is(nullValue()));
        }
    }

    public void testThatOnlyKeystoreInSettingsSetsTruststoreSettings() {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path")
                .put("xpack.security.ssl.keystore.password", "password")
                .build();
        Settings profileSettings = settings.getByPrefix("xpack.security.ssl.");
        // Pass settings in as component settings
        SSLConfiguration globalSettings = new Global(settings);
        SSLConfiguration scopedSettings = new Custom(profileSettings, globalSettings);
        SSLConfiguration scopedEmptyGlobalSettings =
                new Custom(profileSettings, new Global(Settings.EMPTY));
        for (SSLConfiguration sslConfiguration : Arrays.asList(globalSettings, scopedSettings, scopedEmptyGlobalSettings)) {
            assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
            StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();

            assertThat(ksKeyInfo.keyStorePath, is(equalTo("path")));
            assertThat(ksKeyInfo.keyStorePassword, is(equalTo("password")));
            assertThat(ksKeyInfo.keyPassword, is(equalTo(ksKeyInfo.keyStorePassword)));
            assertThat(ksKeyInfo.keyStoreAlgorithm, is(KeyManagerFactory.getDefaultAlgorithm()));
            assertThat(sslConfiguration.trustConfig(), is(sameInstance(ksKeyInfo)));
        }
    }

    public void testThatKeyPasswordCanBeSet() {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path")
                .put("xpack.security.ssl.keystore.password", "password")
                .put("xpack.security.ssl.keystore.key_password", "key")
                .build();
        SSLConfiguration sslConfiguration = new Global(settings);
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(ksKeyInfo.keyStorePassword, is(equalTo("password")));
        assertThat(ksKeyInfo.keyPassword, is(equalTo("key")));

        // Pass settings in as profile settings
        Settings profileSettings = settings.getByPrefix("xpack.security.ssl.");
        SSLConfiguration sslConfiguration1 = new Custom(profileSettings,
                randomBoolean() ? sslConfiguration : new Global(Settings.EMPTY));
        assertThat(sslConfiguration1.keyConfig(), instanceOf(StoreKeyConfig.class));
        ksKeyInfo = (StoreKeyConfig) sslConfiguration1.keyConfig();
        assertThat(ksKeyInfo.keyStorePassword, is(equalTo("password")));
        assertThat(ksKeyInfo.keyPassword, is(equalTo("key")));
    }


    public void testThatProfileSettingsOverrideServiceSettings() {
        Settings profileSettings = Settings.builder()
                .put("keystore.path", "path")
                .put("keystore.password", "password")
                .put("keystore.key_password", "key")
                .put("keystore.algorithm", "algo")
                .put("truststore.path", "trust path")
                .put("truststore.password", "password for trust")
                .put("truststore.algorithm", "trusted")
                .put("protocol", "ssl")
                .put("session.cache_size", "3")
                .put("session.cache_timeout", "10m")
                .build();

        Settings serviceSettings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", "comp path")
                .put("xpack.security.ssl.keystore.password", "comp password")
                .put("xpack.security.ssl.keystore.key_password", "comp key")
                .put("xpack.security.ssl.keystore.algorithm", "comp algo")
                .put("xpack.security.ssl.truststore.path", "comp trust path")
                .put("xpack.security.ssl.truststore.password", "comp password for trust")
                .put("xpack.security.ssl.truststore.algorithm", "comp trusted")
                .put("xpack.security.ssl.protocol", "tls")
                .put("xpack.security.ssl.session.cache_size", "7")
                .put("xpack.security.ssl.session.cache_timeout", "20m")
                .build();

        SSLConfiguration globalSettings = new Global(serviceSettings);
        SSLConfiguration sslConfiguration = new Custom(profileSettings, globalSettings);
        assertThat(sslConfiguration.keyConfig(), instanceOf(StoreKeyConfig.class));
        StoreKeyConfig ksKeyInfo = (StoreKeyConfig) sslConfiguration.keyConfig();
        assertThat(ksKeyInfo.keyStorePath, is(equalTo("path")));
        assertThat(ksKeyInfo.keyStorePassword, is(equalTo("password")));
        assertThat(ksKeyInfo.keyPassword, is(equalTo("key")));
        assertThat(ksKeyInfo.keyStoreAlgorithm, is(equalTo("algo")));
        assertThat(sslConfiguration.trustConfig(), instanceOf(StoreTrustConfig.class));
        StoreTrustConfig ksTrustInfo = (StoreTrustConfig) sslConfiguration.trustConfig();
        assertThat(ksTrustInfo.trustStorePath, is(equalTo("trust path")));
        assertThat(ksTrustInfo.trustStorePassword, is(equalTo("password for trust")));
        assertThat(ksTrustInfo.trustStoreAlgorithm, is(equalTo("trusted")));
        assertThat(sslConfiguration.protocol(), is(equalTo("ssl")));
        assertThat(sslConfiguration.sessionCacheSize(), is(equalTo(3)));
        assertThat(sslConfiguration.sessionCacheTimeout(), is(equalTo(TimeValue.timeValueMinutes(10L))));
    }

    public void testThatEmptySettingsAreEqual() {
        SSLConfiguration sslConfiguration = new Global(Settings.EMPTY);
        SSLConfiguration sslConfiguration1 = new Global(Settings.EMPTY);
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));

        SSLConfiguration profileSSLConfiguration = new Custom(Settings.EMPTY, sslConfiguration);
        assertThat(sslConfiguration.equals(profileSSLConfiguration), is(equalTo(true)));
        assertThat(profileSSLConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(profileSSLConfiguration.equals(profileSSLConfiguration), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentKeystoresAreNotEqual() {
        SSLConfiguration sslConfiguration = new Global(Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path").build());
        SSLConfiguration sslConfiguration1 = new Global(Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path1").build());
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(false)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(false)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentProtocolsAreNotEqual() {
        SSLConfiguration sslConfiguration = new Global(Settings.builder()
                .put("xpack.security.ssl.protocol", "ssl").build());
        SSLConfiguration sslConfiguration1 = new Global(Settings.builder()
                .put("xpack.security.ssl.protocol", "tls").build());
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(false)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(false)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentTruststoresAreNotEqual() {
        SSLConfiguration sslConfiguration = new Global(Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/trust").build());
        SSLConfiguration sslConfiguration1 = new Global(Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/truststore").build());
        assertThat(sslConfiguration.equals(sslConfiguration1), is(equalTo(false)));
        assertThat(sslConfiguration1.equals(sslConfiguration), is(equalTo(false)));
        assertThat(sslConfiguration.equals(sslConfiguration), is(equalTo(true)));
        assertThat(sslConfiguration1.equals(sslConfiguration1), is(equalTo(true)));
    }

    public void testThatEmptySettingsHaveSameHashCode() {
        SSLConfiguration sslConfiguration = new Global(Settings.EMPTY);
        SSLConfiguration sslConfiguration1 = new Global(Settings.EMPTY);
        assertThat(sslConfiguration.hashCode(), is(equalTo(sslConfiguration1.hashCode())));

        SSLConfiguration profileSettings = new Custom(Settings.EMPTY, sslConfiguration);
        assertThat(profileSettings.hashCode(), is(equalTo(sslConfiguration.hashCode())));
    }

    public void testThatSettingsWithDifferentKeystoresHaveDifferentHashCode() {
        SSLConfiguration sslConfiguration = new Global(Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path").build());
        SSLConfiguration sslConfiguration1 = new Global(Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path1").build());
        assertThat(sslConfiguration.hashCode(), is(not(equalTo(sslConfiguration1.hashCode()))));
    }

    public void testThatSettingsWithDifferentProtocolsHaveDifferentHashCode() {
        SSLConfiguration sslConfiguration = new Global(Settings.builder()
                .put("xpack.security.ssl.protocol", "ssl").build());
        SSLConfiguration sslConfiguration1 = new Global(Settings.builder()
                .put("xpack.security.ssl.protocol", "tls").build());
        assertThat(sslConfiguration.hashCode(), is(not(equalTo(sslConfiguration1.hashCode()))));
    }

    public void testThatSettingsWithDifferentTruststoresHaveDifferentHashCode() {
        SSLConfiguration sslConfiguration = new Global(Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/trust").build());
        SSLConfiguration sslConfiguration1 = new Global(Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/truststore").build());
        assertThat(sslConfiguration.hashCode(), is(not(equalTo(sslConfiguration1.hashCode()))));
    }

    public void testConfigurationUsingPEMKeyFiles() {
        Environment env = randomBoolean() ? null :
                new Environment(Settings.builder().put("path.home", createTempDir()).build());
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.key.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
                .put("xpack.security.ssl.key.password", "testnode")
                .put("xpack.security.ssl.cert", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
                .build();

        SSLConfiguration config = new Global(settings);
        assertThat(config.keyConfig(), instanceOf(PEMKeyConfig.class));
        PEMKeyConfig keyConfig = (PEMKeyConfig) config.keyConfig();
        KeyManager[] keyManagers = keyConfig.keyManagers(env);
        assertThat(keyManagers.length, is(1));
        assertThat(config.trustConfig(), sameInstance(keyConfig));
        TrustManager[] trustManagers = keyConfig.trustManagers(env);
        assertThat(trustManagers.length, is(1));
    }

    public void testConfigurationUsingPEMKeyAndTrustFiles() {
        Environment env = randomBoolean() ? null :
                new Environment(Settings.builder().put("path.home", createTempDir()).build());
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.key.path",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
                .put("xpack.security.ssl.key.password", "testnode")
                .put("xpack.security.ssl.cert", getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
                .putArray("xpack.security.ssl.ca",
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt").toString(),
                        getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testclient.crt").toString())
                .build();

        SSLConfiguration config = new Global(settings);
        assertThat(config.keyConfig(), instanceOf(PEMKeyConfig.class));
        PEMKeyConfig keyConfig = (PEMKeyConfig) config.keyConfig();
        KeyManager[] keyManagers = keyConfig.keyManagers(env);
        assertThat(keyManagers.length, is(1));
        assertThat(config.trustConfig(), not(sameInstance(keyConfig)));
        assertThat(config.trustConfig(), instanceOf(PEMTrustConfig.class));
        TrustManager[] trustManagers = keyConfig.trustManagers(env);
        assertThat(trustManagers.length, is(1));
    }
}
