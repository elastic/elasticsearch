/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Guice;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.settings.SettingsModule;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.LdapRealmSettings;
import org.elasticsearch.xpack.core.security.authc.ldap.PoolingSessionFactorySettings;
import org.elasticsearch.xpack.security.LocalStateSecurity;
import org.hamcrest.Matcher;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.is;

public class SettingsFilterTests extends ESTestCase {

    private Settings.Builder configuredSettingsBuilder = Settings.builder();
    private Map<String, Matcher> settingsMatcherMap = new HashMap<>();
    private MockSecureSettings mockSecureSettings = new MockSecureSettings();

    public void testFiltering() throws Exception {
        final boolean useLegacyLdapBindPassword = randomBoolean();

        configureUnfilteredSetting("xpack.security.authc.realms.file.file1.enabled", "true");

        // ldap realm filtering
        configureUnfilteredSetting("xpack.security.authc.realms.ldap.ldap1.enabled", "false");
        configureUnfilteredSetting("xpack.security.authc.realms.ldap.ldap1.url", "ldap://host.domain");
        configureFilteredSetting("xpack.security.authc.realms.ldap.ldap1.hostname_verification", Boolean.toString(randomBoolean()));
        configureFilteredSetting("xpack.security.authc.realms.ldap.ldap1.bind_dn", randomAlphaOfLength(5));
        if (useLegacyLdapBindPassword) {
            configureFilteredSetting("xpack.security.authc.realms.ldap.ldap1.bind_password", randomAlphaOfLength(5));
        } else {
            configureSecureSetting("xpack.security.authc.realms.ldap.ldap1.secure_bind_password", randomAlphaOfLengthBetween(3, 8));
        }

        // active directory filtering
        configureUnfilteredSetting("xpack.security.authc.realms.active_directory.ad1.enabled", "false");
        configureUnfilteredSetting("xpack.security.authc.realms.active_directory.ad1.url", "ldap://host.domain");
        configureFilteredSetting("xpack.security.authc.realms.active_directory.ad1.hostname_verification",
                Boolean.toString(randomBoolean()));

        // pki filtering
        configureUnfilteredSetting("xpack.security.authc.realms.pki.pki1.order", "0");
        if (inFipsJvm() == false) {
            configureFilteredSetting("xpack.security.authc.realms.pki.pki1.truststore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/truststore-testnode-only.jks").toString());
            configureFilteredSetting("xpack.security.transport.ssl.keystore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks").toString());
        }
        configureSecureSetting("xpack.security.authc.realms.pki.pki1.truststore.secure_password", "truststore-testnode-only");
        configureFilteredSetting("xpack.security.authc.realms.pki.pki1.truststore.algorithm", "SunX509");


        configureFilteredSetting("xpack.security.transport.ssl.cipher_suites",
                Strings.arrayToCommaDelimitedString(XPackSettings.DEFAULT_CIPHERS.toArray()));
        configureFilteredSetting("xpack.security.transport.ssl.supported_protocols", randomFrom("TLSv1", "TLSv1.1", "TLSv1.2"));
        configureSecureSetting("xpack.security.transport.ssl.keystore.secure_password", "testnode");
        configureFilteredSetting("xpack.security.transport.ssl.keystore.algorithm", KeyManagerFactory.getDefaultAlgorithm());
        configureSecureSetting("xpack.security.transport.ssl.keystore.secure_key_password", "testnode");
        configureSecureSetting("xpack.security.transport.ssl.truststore.secure_password", randomAlphaOfLength(5));
        configureFilteredSetting("xpack.security.transport.ssl.truststore.algorithm", TrustManagerFactory.getDefaultAlgorithm());

        // client profile
        configureUnfilteredSetting("transport.profiles.client.port", "9500-9600");
        if (inFipsJvm() == false) {
            configureFilteredSetting("transport.profiles.client.xpack.security.ssl.keystore.path",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks").toString());
        }
        configureFilteredSetting("transport.profiles.client.xpack.security.ssl.cipher_suites",
                Strings.arrayToCommaDelimitedString(XPackSettings.DEFAULT_CIPHERS.toArray()));
        configureFilteredSetting("transport.profiles.client.xpack.security.ssl.supported_protocols",
                randomFrom("TLSv1", "TLSv1.1", "TLSv1.2"));
        configureSecureSetting("transport.profiles.client.xpack.security.ssl.keystore.secure_password", "testnode");
        configureFilteredSetting("transport.profiles.client.xpack.security.ssl.keystore.algorithm",
                KeyManagerFactory.getDefaultAlgorithm());
        configureSecureSetting("transport.profiles.client.xpack.security.ssl.keystore.secure_key_password", "testnode");
        configureSecureSetting("transport.profiles.client.xpack.security.ssl.truststore.secure_password", randomAlphaOfLength(5));
        configureFilteredSetting("transport.profiles.client.xpack.security.ssl.truststore.algorithm",
                TrustManagerFactory.getDefaultAlgorithm());

        // custom settings, potentially added by a plugin
        configureFilteredSetting("foo.bar", "_secret");
        configureFilteredSetting("foo.baz", "_secret");
        configureFilteredSetting("bar.baz", "_secret");
        configureUnfilteredSetting("baz.foo", "_not_a_secret");
        configureFilteredSetting("xpack.security.hide_settings", "foo.*,bar.baz");

        Settings settings = Settings.builder()
                .put("path.home", createTempDir())
                .put(configuredSettingsBuilder.build())
                .setSecureSettings(mockSecureSettings)
                .build();

        LocalStateSecurity securityPlugin = new LocalStateSecurity(settings, null);

        List<Setting<?>> settingList = new ArrayList<>();
        settingList.add(Setting.simpleString("foo.bar", Setting.Property.NodeScope));
        settingList.add(Setting.simpleString("foo.baz", Setting.Property.NodeScope));
        settingList.add(Setting.simpleString("bar.baz", Setting.Property.NodeScope));
        settingList.add(Setting.simpleString("baz.foo", Setting.Property.NodeScope));
        settingList.addAll(securityPlugin.getSettings());
        List<String> settingsFilterList = new ArrayList<>();
        settingsFilterList.addAll(securityPlugin.getSettingsFilter());
        // custom settings, potentially added by a plugin
        SettingsModule settingsModule = new SettingsModule(settings, settingList, settingsFilterList, Collections.emptySet());

        Injector injector = Guice.createInjector(settingsModule);
        SettingsFilter settingsFilter = injector.getInstance(SettingsFilter.class);

        Settings filteredSettings = settingsFilter.filter(settings);
        for (Map.Entry<String, Matcher> entry : settingsMatcherMap.entrySet()) {
            assertThat(filteredSettings.get(entry.getKey()), entry.getValue());
        }

        if (useLegacyLdapBindPassword) {
            assertSettingDeprecationsAndWarnings(new Setting<?>[]{PoolingSessionFactorySettings.LEGACY_BIND_PASSWORD
                    .apply(LdapRealmSettings.LDAP_TYPE)
                    .getConcreteSettingForNamespace("ldap1")});
        }
    }

    private void configureUnfilteredSetting(String settingName, String value) {
        configureSetting(settingName, value, is(value));
    }

    private void configureFilteredSetting(String settingName, String value) {
        configureSetting(settingName, value, is(nullValue()));
    }

    private void configureSecureSetting(String settingName, String value) {
        mockSecureSettings.setString(settingName, value);
        settingsMatcherMap.put(settingName, is(nullValue()));
    }

    private void configureSetting(String settingName, String value, Matcher expectedMatcher) {
        configuredSettingsBuilder.put(settingName, value);
        settingsMatcherMap.put(settingName, expectedMatcher);
    }
}
