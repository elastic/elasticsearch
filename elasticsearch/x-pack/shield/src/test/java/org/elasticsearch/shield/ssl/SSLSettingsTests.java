/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class SSLSettingsTests extends ESTestCase {
    public void testThatSSLSettingsWithEmptySettingsHaveCorrectDefaults() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslSettings.keyStorePath, nullValue());
        assertThat(sslSettings.keyStorePassword, nullValue());
        assertThat(sslSettings.keyPassword, nullValue());
        assertThat(sslSettings.keyStoreAlgorithm, is(equalTo(KeyManagerFactory.getDefaultAlgorithm())));
        assertThat(sslSettings.sessionCacheSize, is(equalTo(SSLSettings.Globals.DEFAULT_SESSION_CACHE_SIZE)));
        assertThat(sslSettings.sessionCacheTimeout, is(equalTo(SSLSettings.Globals.DEFAULT_SESSION_CACHE_TIMEOUT)));
        assertThat(sslSettings.sslProtocol, is(equalTo(SSLSettings.Globals.DEFAULT_PROTOCOL)));
        assertThat(sslSettings.trustStoreAlgorithm, is(equalTo(TrustManagerFactory.getDefaultAlgorithm())));
        assertThat(sslSettings.trustStorePassword, nullValue());
        assertThat(sslSettings.trustStorePath, nullValue());
    }

    public void testThatOnlyKeystoreInSettingsSetsTruststoreSettings() {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path")
                .put("xpack.security.ssl.keystore.password", "password")
                .build();
        // Pass settings in as component settings
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, settings);
        assertThat(sslSettings.keyStorePath, is(equalTo("path")));
        assertThat(sslSettings.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings.trustStorePath, is(equalTo(sslSettings.keyStorePath)));
        assertThat(sslSettings.trustStorePassword, is(equalTo(sslSettings.keyStorePassword)));

        // Pass settings in as profile settings
        settings = Settings.builder()
                .put("keystore.path", "path")
                .put("keystore.password", "password")
                .build();
        SSLSettings sslSettings1 = new SSLSettings(settings, Settings.EMPTY);
        assertThat(sslSettings1.keyStorePath, is(equalTo("path")));
        assertThat(sslSettings1.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings1.trustStorePath, is(equalTo(sslSettings1.keyStorePath)));
        assertThat(sslSettings1.trustStorePassword, is(equalTo(sslSettings1.keyStorePassword)));
    }

    public void testThatKeystorePasswordIsDefaultKeyPassword() {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.password", "password")
                .build();
        // Pass settings in as component settings
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, settings);
        assertThat(sslSettings.keyPassword, is(equalTo(sslSettings.keyStorePassword)));

        settings = Settings.builder()
                .put("keystore.password", "password")
                .build();
        // Pass settings in as profile settings
        SSLSettings sslSettings1 = new SSLSettings(settings, Settings.EMPTY);
        assertThat(sslSettings1.keyPassword, is(equalTo(sslSettings1.keyStorePassword)));
    }

    public void testThatKeyPasswordCanBeSet() {
        Settings settings = Settings.builder()
                .put("xpack.security.ssl.keystore.password", "password")
                .put("xpack.security.ssl.keystore.key_password", "key")
                .build();
        // Pass settings in as component settings
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, settings);
        assertThat(sslSettings.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings.keyPassword, is(equalTo("key")));

        // Pass settings in as profile settings
        settings = Settings.builder()
                .put("keystore.password", "password")
                .put("keystore.key_password", "key")
                .build();
        SSLSettings sslSettings1 = new SSLSettings(settings, Settings.EMPTY);
        assertThat(sslSettings1.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings1.keyPassword, is(equalTo("key")));
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

        SSLSettings sslSettings = new SSLSettings(profileSettings, serviceSettings);
        assertThat(sslSettings.keyStorePath, is(equalTo("path")));
        assertThat(sslSettings.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings.keyPassword, is(equalTo("key")));
        assertThat(sslSettings.keyStoreAlgorithm, is(equalTo("algo")));
        assertThat(sslSettings.trustStorePath, is(equalTo("trust path")));
        assertThat(sslSettings.trustStorePassword, is(equalTo("password for trust")));
        assertThat(sslSettings.trustStoreAlgorithm, is(equalTo("trusted")));
        assertThat(sslSettings.sslProtocol, is(equalTo("ssl")));
        assertThat(sslSettings.sessionCacheSize, is(equalTo(3)));
        assertThat(sslSettings.sessionCacheTimeout, is(equalTo(TimeValue.timeValueMinutes(10L))));
    }

    public void testThatEmptySettingsAreEqual() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.EMPTY);
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentKeystoresAreNotEqual() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path").build());
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path1").build());
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(false)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(false)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentProtocolsAreNotEqual() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.protocol", "ssl").build());
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.protocol", "tls").build());
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(false)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(false)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    public void testThatSettingsWithDifferentTruststoresAreNotEqual() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/trust").build());
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/truststore").build());
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(false)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(false)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    public void testThatEmptySettingsHaveSameHashCode() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.EMPTY);
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.EMPTY);
        assertThat(sslSettings.hashCode(), is(equalTo(sslSettings1.hashCode())));
    }

    public void testThatSettingsWithDifferentKeystoresHaveDifferentHashCode() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path").build());
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.keystore.path", "path1").build());
        assertThat(sslSettings.hashCode(), is(not(equalTo(sslSettings1.hashCode()))));
    }

    public void testThatSettingsWithDifferentProtocolsHaveDifferentHashCode() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.protocol", "ssl").build());
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.protocol", "tls").build());
        assertThat(sslSettings.hashCode(), is(not(equalTo(sslSettings1.hashCode()))));
    }

    public void testThatSettingsWithDifferentTruststoresHaveDifferentHashCode() {
        SSLSettings sslSettings = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/trust").build());
        SSLSettings sslSettings1 = new SSLSettings(Settings.EMPTY, Settings.builder()
                .put("xpack.security.ssl.truststore.path", "/truststore").build());
        assertThat(sslSettings.hashCode(), is(not(equalTo(sslSettings1.hashCode()))));
    }
}
