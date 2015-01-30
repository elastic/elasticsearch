/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.ssl;

import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.shield.ssl.AbstractSSLService.SSLSettings;
import org.junit.Test;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.hamcrest.Matchers.*;

public class SSLSettingsTests extends ElasticsearchTestCase {

    @Test
    public void testThatSSLSettingsWithEmptySettingsHaveCorrectDefaults() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, ImmutableSettings.EMPTY);
        assertThat(sslSettings.keyStorePath, is(nullValue()));
        assertThat(sslSettings.keyStorePassword, is(nullValue()));
        assertThat(sslSettings.keyPassword, is(nullValue()));
        assertThat(sslSettings.keyStoreAlgorithm, is(equalTo(KeyManagerFactory.getDefaultAlgorithm())));
        assertThat(sslSettings.sessionCacheSize, is(equalTo(AbstractSSLService.DEFAULT_SESSION_CACHE_SIZE)));
        assertThat(sslSettings.sessionCacheTimeout, is(equalTo(AbstractSSLService.DEFAULT_SESSION_CACHE_TIMEOUT)));
        assertThat(sslSettings.sslProtocol, is(equalTo(AbstractSSLService.DEFAULT_PROTOCOL)));
        assertThat(sslSettings.trustStoreAlgorithm, is(equalTo(TrustManagerFactory.getDefaultAlgorithm())));
        assertThat(sslSettings.trustStorePassword, is(nullValue()));
        assertThat(sslSettings.trustStorePath, is(nullValue()));
    }

    @Test
    public void testThatOnlyKeystoreInSettingsSetsTruststoreSettings() {
        Settings settings = settingsBuilder()
                .put("keystore.path", "path")
                .put("keystore.password", "password")
                .build();
        // Pass settings in as component settings
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settings);
        assertThat(sslSettings.keyStorePath, is(equalTo("path")));
        assertThat(sslSettings.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings.trustStorePath, is(equalTo(sslSettings.keyStorePath)));
        assertThat(sslSettings.trustStorePassword, is(equalTo(sslSettings.keyStorePassword)));

        // Pass settings in as profile settings
        SSLSettings sslSettings1 = new SSLSettings(settings, ImmutableSettings.EMPTY);
        assertThat(sslSettings1.keyStorePath, is(equalTo("path")));
        assertThat(sslSettings1.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings1.trustStorePath, is(equalTo(sslSettings1.keyStorePath)));
        assertThat(sslSettings1.trustStorePassword, is(equalTo(sslSettings1.keyStorePassword)));
    }

    @Test
    public void testThatKeystorePasswordIsDefaultKeyPassword() {
        Settings settings = settingsBuilder()
                .put("keystore.password", "password")
                .build();
        // Pass settings in as component settings
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settings);
        assertThat(sslSettings.keyPassword, is(equalTo(sslSettings.keyStorePassword)));

        // Pass settings in as profile settings
        SSLSettings sslSettings1 = new SSLSettings(settings, ImmutableSettings.EMPTY);
        assertThat(sslSettings1.keyPassword, is(equalTo(sslSettings1.keyStorePassword)));
    }

    @Test
    public void testThatKeyPasswordCanBeSet() {
        Settings settings = settingsBuilder()
                .put("keystore.password", "password")
                .put("keystore.key_password", "key")
                .build();
        // Pass settings in as component settings
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settings);
        assertThat(sslSettings.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings.keyPassword, is(equalTo("key")));

        // Pass settings in as profile settings
        SSLSettings sslSettings1 = new SSLSettings(settings, ImmutableSettings.EMPTY);
        assertThat(sslSettings1.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings1.keyPassword, is(equalTo("key")));
    }

    @Test
    public void testThatProfileSettingsOverrideComponentSettings() {
        Settings profileSettings = settingsBuilder()
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

        Settings componentSettings = settingsBuilder()
                .put("keystore.path", "comp path")
                .put("keystore.password", "comp password")
                .put("keystore.key_password", "comp key")
                .put("keystore.algorithm", "comp algo")
                .put("truststore.path", "comp trust path")
                .put("truststore.password", "comp password for trust")
                .put("truststore.algorithm", "comp trusted")
                .put("protocol", "tls")
                .put("session.cache_size", "7")
                .put("session.cache_timeout", "20m")
                .build();

        SSLSettings sslSettings = new SSLSettings(profileSettings, componentSettings);
        assertThat(sslSettings.keyStorePath, is(equalTo("path")));
        assertThat(sslSettings.keyStorePassword, is(equalTo("password")));
        assertThat(sslSettings.keyPassword, is(equalTo("key")));
        assertThat(sslSettings.keyStoreAlgorithm, is(equalTo("algo")));
        assertThat(sslSettings.trustStorePath, is(equalTo("trust path")));
        assertThat(sslSettings.trustStorePassword, is(equalTo("password for trust")));
        assertThat(sslSettings.trustStoreAlgorithm, is(equalTo("trusted")));
        assertThat(sslSettings.sslProtocol, is(equalTo("ssl")));
        assertThat(sslSettings.sessionCacheSize, is(equalTo(3)));
        assertThat(sslSettings.sessionCacheTimeout, is(equalTo(TimeValue.parseTimeValue("10m", null))));
    }

    @Test
    public void testThatEmptySettingsAreEqual() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, ImmutableSettings.EMPTY);
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, ImmutableSettings.EMPTY);
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    @Test
    public void testThatSettingsWithDifferentKeystoresAreNotEqual() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("keystore.path", "path").build());
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("keystore.path", "path1").build());
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(false)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(false)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    @Test
    public void testThatSettingsWithDifferentProtocolsAreNotEqual() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("protocol", "ssl").build());
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("protocol", "tls").build());
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(false)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(false)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    @Test
    public void testThatSettingsWithDifferentTruststoresAreNotEqual() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("truststore.path", "/trust").build());
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("truststore.path", "/truststore").build());
        assertThat(sslSettings.equals(sslSettings1), is(equalTo(false)));
        assertThat(sslSettings1.equals(sslSettings), is(equalTo(false)));
        assertThat(sslSettings.equals(sslSettings), is(equalTo(true)));
        assertThat(sslSettings1.equals(sslSettings1), is(equalTo(true)));
    }

    @Test
    public void testThatEmptySettingsHaveSameHashCode() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, ImmutableSettings.EMPTY);
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, ImmutableSettings.EMPTY);
        assertThat(sslSettings.hashCode(), is(equalTo(sslSettings1.hashCode())));
    }

    @Test
    public void testThatSettingsWithDifferentKeystoresHaveDifferentHashCode() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("keystore.path", "path").build());
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("keystore.path", "path1").build());
        assertThat(sslSettings.hashCode(), is(not(equalTo(sslSettings1.hashCode()))));
    }

    @Test
    public void testThatSettingsWithDifferentProtocolsHaveDifferentHashCode() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("protocol", "ssl").build());
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("protocol", "tls").build());
        assertThat(sslSettings.hashCode(), is(not(equalTo(sslSettings1.hashCode()))));
    }

    @Test
    public void testThatSettingsWithDifferentTruststoresHaveDifferentHashCode() {
        SSLSettings sslSettings = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("truststore.path", "/trust").build());
        SSLSettings sslSettings1 = new SSLSettings(ImmutableSettings.EMPTY, settingsBuilder()
                .put("truststore.path", "/truststore").build());
        assertThat(sslSettings.hashCode(), is(not(equalTo(sslSettings1.hashCode()))));
    }
}
