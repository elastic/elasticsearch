/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ssl;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;

import static org.elasticsearch.test.TestMatchers.throwableWithMessage;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class SSLConfigurationSettingsTests extends ESTestCase {

    public void testParseCipherSettingsWithoutPrefix() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withoutPrefix();
        assertThat(ssl.ciphers.match("cipher_suites"), is(true));
        assertThat(ssl.ciphers.match("ssl.cipher_suites"), is(false));
        assertThat(ssl.ciphers.match("xpack.transport.security.ssl.cipher_suites"), is(false));

        final Settings settings = Settings.builder()
            .put("cipher_suites.0", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256")
            .put("cipher_suites.1", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256")
            .put("cipher_suites.2", "TLS_RSA_WITH_AES_128_CBC_SHA256")
            .build();
        assertThat(
            ssl.ciphers.get(settings),
            is(
                Arrays.asList(
                    "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256",
                    "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256",
                    "TLS_RSA_WITH_AES_128_CBC_SHA256"
                )
            )
        );
    }

    public void testParseClientAuthWithPrefix() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withPrefix("xpack.security.http.ssl.");
        assertThat(ssl.clientAuth.match("xpack.security.http.ssl.client_authentication"), is(true));
        assertThat(ssl.clientAuth.match("client_authentication"), is(false));

        final Settings settings = Settings.builder()
            .put("xpack.security.http.ssl.client_authentication", SSLClientAuth.OPTIONAL.name())
            .build();
        assertThat(ssl.clientAuth.get(settings).get(), is(SSLClientAuth.OPTIONAL));
    }

    public void testParseKeystoreAlgorithmWithPrefix() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withPrefix("xpack.security.authc.realms.ldap1.ssl.");
        assertThat(ssl.x509KeyPair.keystoreAlgorithm.match("xpack.security.authc.realms.ldap1.ssl.keystore.algorithm"), is(true));

        final String algo = randomAlphaOfLength(16);
        final Settings settings = Settings.builder().put("xpack.security.authc.realms.ldap1.ssl.keystore.algorithm", algo).build();
        assertThat(ssl.x509KeyPair.keystoreAlgorithm.get(settings), is(algo));
    }

    public void testParseProtocolsListWithPrefix() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withPrefix("ssl.");
        assertThat(ssl.supportedProtocols.match("ssl.supported_protocols"), is(true));

        final Settings settings = Settings.builder().putList("ssl.supported_protocols", "SSLv3", "SSLv2Hello", "SSLv2").build();
        assertThat(ssl.supportedProtocols.get(settings), is(Arrays.asList("SSLv3", "SSLv2Hello", "SSLv2")));
    }

    public void testParseTrustRestrictionsListWithPrefix() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withPrefix("ssl.");
        assertThat(ssl.trustRestrictionsX509Fields.match("ssl.trust_restrictions.x509_fields"), is(true));

        // explicit configuration
        Settings settings = Settings.builder()
            .putList("ssl.trust_restrictions.x509_fields", "subjectAltName.otherName.commonName", "subjectAltName.dnsName")
            .build();
        assertThat(
            ssl.trustRestrictionsX509Fields.get(settings),
            is(Arrays.asList("subjectAltName.otherName.commonName", "subjectAltName.dnsName"))
        );

        // implicit configuration
        settings = Settings.builder().build();
        assertThat(ssl.trustRestrictionsX509Fields.get(settings), is(Arrays.asList("subjectAltName.otherName.commonName")));

        // invalid configuration
        final Settings invalid = Settings.builder().putList("ssl.trust_restrictions.x509_fields", "foo.bar").build();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ssl.trustRestrictionsX509Fields.get(invalid));
        assertThat(e.getCause(), throwableWithMessage(containsString("foo.bar is not a supported x509 field for trust restrictions.")));
        assertThat(e.getCause(), throwableWithMessage(containsString("Recognised values are")));
        assertThat(e.getCause(), throwableWithMessage(containsString("subjectAltName.otherName.commonName")));
        assertThat(e.getCause(), throwableWithMessage(containsString("subjectAltName.dnsName")));
    }

    public void testEmptySettingsParsesToDefaults() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withoutPrefix();
        final Settings settings = Settings.EMPTY;
        assertThat(ssl.caPaths.get(settings).size(), is(0));
        assertThat(ssl.x509KeyPair.certificatePath.get(settings).isPresent(), is(false));
        assertThat(ssl.ciphers.get(settings).size(), is(0));
        assertThat(ssl.clientAuth.get(settings).isPresent(), is(false));
        assertThat(ssl.x509KeyPair.keyPassword.exists(settings), is(false));
        assertThat(ssl.x509KeyPair.keyPath.get(settings).isPresent(), is(false));
        assertThat(ssl.x509KeyPair.keystoreAlgorithm.get(settings), is(KeyManagerFactory.getDefaultAlgorithm()));
        assertThat(ssl.x509KeyPair.keystoreType.get(settings).isPresent(), is(false));
        assertThat(ssl.x509KeyPair.keystoreKeyPassword.exists(settings), is(false));
        assertThat(ssl.x509KeyPair.keystorePassword.exists(settings), is(false));
        assertThat(ssl.x509KeyPair.keystorePath.get(settings).isPresent(), is(false));
        assertThat(ssl.supportedProtocols.get(settings).size(), is(0));
        assertThat(ssl.truststoreAlgorithm.get(settings), is(TrustManagerFactory.getDefaultAlgorithm()));
        assertThat(ssl.truststoreType.get(settings).isPresent(), is(false));
        assertThat(ssl.truststorePassword.exists(settings), is(false));
        assertThat(ssl.truststorePath.get(settings).isPresent(), is(false));
        assertThat(ssl.trustRestrictionsPath.get(settings).isPresent(), is(false));
        assertThat(
            ssl.trustRestrictionsX509Fields.get(settings),
            is(org.elasticsearch.core.List.of("subjectAltName.otherName.commonName"))
        );
        assertThat(ssl.verificationMode.get(settings).isPresent(), is(false));

        assertThat(SSLConfigurationSettings.getKeyStoreType(ssl.x509KeyPair.keystoreType, settings, null), is("jks"));
        assertThat(SSLConfigurationSettings.getKeyStoreType(ssl.truststoreType, settings, null), is("jks"));
    }

    public void testRealmSettingPrefixes() {
        SSLConfigurationSettings.getRealmSettings("_type").forEach(affix -> {
            final String key = affix.getConcreteSettingForNamespace("_name").getKey();
            assertThat(key, startsWith("xpack.security.authc.realms._type._name.ssl."));
        });
    }

    public void testProfileSettingPrefixes() {
        SSLConfigurationSettings.getProfileSettings().forEach(affix -> {
            assertThat(affix, instanceOf(Setting.AffixSetting.class));
            final String key = ((Setting.AffixSetting) affix).getConcreteSettingForNamespace("_name").getKey();
            assertThat(key, startsWith("transport.profiles._name.xpack.security.ssl."));
        });
    }

}
