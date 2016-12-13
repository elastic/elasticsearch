/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ssl;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import java.util.Arrays;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.is;

public class SSLConfigurationSettingsTests extends ESTestCase {

    public void testParseCipherSettingsWithoutPrefix() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withoutPrefix();
        assertThat(ssl.ciphers.match("cipher_suites"), is(true));
        assertThat(ssl.ciphers.match("ssl.cipher_suites"), is(false));
        assertThat(ssl.ciphers.match("xpack.ssl.cipher_suites"), is(false));

        final Settings settings = Settings.builder()
                .put("cipher_suites.0", "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256")
                .put("cipher_suites.1", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256")
                .put("cipher_suites.2", "TLS_RSA_WITH_AES_128_CBC_SHA256")
                .build();
        assertThat(ssl.ciphers.get(settings), is(Arrays.asList(
                "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA256", "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA256", "TLS_RSA_WITH_AES_128_CBC_SHA256"
        )));
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
        assertThat(ssl.keystoreAlgorithm.match("xpack.security.authc.realms.ldap1.ssl.keystore.algorithm"), is(true));

        final String algo = randomAsciiOfLength(16);
        final Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.ldap1.ssl.keystore.algorithm", algo)
                .build();
        assertThat(ssl.keystoreAlgorithm.get(settings), is(algo));
    }

    public void testParseProtocolsListWithPrefix() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withPrefix("ssl.");
        assertThat(ssl.supportedProtocols.match("ssl.supported_protocols"), is(true));

        final Settings settings = Settings.builder()
                .putArray("ssl.supported_protocols", "SSLv3", "SSLv2Hello", "SSLv2")
                .build();
        assertThat(ssl.supportedProtocols.get(settings), is(Arrays.asList("SSLv3", "SSLv2Hello", "SSLv2")));
    }

    public void testKeyStoreKeyPasswordDefaultsToKeystorePassword() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withPrefix("xpack.ssl.");

        assertThat(ssl.keystorePassword.match("xpack.ssl.keystore.password"), is(true));
        assertThat(ssl.keystoreKeyPassword.match("xpack.ssl.keystore.key_password"), is(true));

        assertThat(ssl.keystorePassword.match("xpack.ssl.keystore.key_password"), is(false));
        assertThat(ssl.keystoreKeyPassword.match("xpack.ssl.keystore.password"), is(false));

        final String password = randomAsciiOfLength(16);
        final Settings settings = Settings.builder()
                .put("xpack.ssl.keystore.password", password)
                .build();
        assertThat(ssl.keystoreKeyPassword.get(settings).get(), is(password));
    }

    public void testEmptySettingsParsesToDefaults() {
        final SSLConfigurationSettings ssl = SSLConfigurationSettings.withoutPrefix();
        final Settings settings = Settings.EMPTY;
        assertThat(ssl.caPaths.get(settings).size(), is(0));
        assertThat(ssl.cert.get(settings).isPresent(), is(false));
        assertThat(ssl.ciphers.get(settings).size(), is(0));
        assertThat(ssl.clientAuth.get(settings).isPresent(), is(false));
        assertThat(ssl.keyPassword.get(settings).isPresent(), is(false));
        assertThat(ssl.keyPath.get(settings).isPresent(), is(false));
        assertThat(ssl.keystoreAlgorithm.get(settings), is(KeyManagerFactory.getDefaultAlgorithm()));
        assertThat(ssl.keystoreKeyPassword.get(settings).isPresent(), is(false));
        assertThat(ssl.keystorePassword.get(settings).isPresent(), is(false));
        assertThat(ssl.keystorePath.get(settings).isPresent(), is(false));
        assertThat(ssl.supportedProtocols.get(settings).size(), is(0));
        assertThat(ssl.truststoreAlgorithm.get(settings), is(TrustManagerFactory.getDefaultAlgorithm()));
        assertThat(ssl.truststorePassword.get(settings).isPresent(), is(false));
        assertThat(ssl.truststorePath.get(settings).isPresent(), is(false));
        assertThat(ssl.verificationMode.get(settings).isPresent(), is(false));
    }

}
