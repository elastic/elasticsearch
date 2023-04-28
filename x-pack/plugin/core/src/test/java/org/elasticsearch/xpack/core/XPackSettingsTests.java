/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterPortSettings;
import org.elasticsearch.transport.TcpTransport;

import java.security.NoSuchAlgorithmException;
import java.util.List;

import javax.crypto.SecretKeyFactory;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class XPackSettingsTests extends ESTestCase {

    public void testDefaultSSLCiphers() {
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_RSA_WITH_AES_128_CBC_SHA"));
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_RSA_WITH_AES_256_CBC_SHA"));
    }

    public void testChaCha20InCiphersOnJdk12Plus() {
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_CHACHA20_POLY1305_SHA256"));
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256"));
        assertThat(XPackSettings.DEFAULT_CIPHERS, hasItem("TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256"));
    }

    public void testPasswordHashingAlgorithmSettingValidation() {
        final boolean isPBKDF2Available = isSecretkeyFactoryAlgoAvailable("PBKDF2WithHMACSHA512");
        final String pbkdf2Algo = randomFrom("PBKDF2_10000", "PBKDF2");
        final Settings settings = Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), pbkdf2Algo).build();
        if (isPBKDF2Available) {
            assertEquals(pbkdf2Algo, XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings));
        } else {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> XPackSettings.PASSWORD_HASHING_ALGORITHM.get(settings)
            );
            assertThat(e.getMessage(), containsString("Support for PBKDF2WithHMACSHA512 must be available"));
        }

        final String bcryptAlgo = randomFrom("BCRYPT", "BCRYPT11");
        assertEquals(
            bcryptAlgo,
            XPackSettings.PASSWORD_HASHING_ALGORITHM.get(
                Settings.builder().put(XPackSettings.PASSWORD_HASHING_ALGORITHM.getKey(), bcryptAlgo).build()
            )
        );
    }

    public void testDefaultPasswordHashingAlgorithmInFips() {
        final Settings.Builder builder = Settings.builder();
        if (inFipsJvm()) {
            builder.put(XPackSettings.FIPS_MODE_ENABLED.getKey(), true);
            assertThat(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(builder.build()), equalTo("PBKDF2_STRETCH"));
        } else {
            assertThat(XPackSettings.PASSWORD_HASHING_ALGORITHM.get(builder.build()), equalTo("BCRYPT"));
        }
    }

    public void testDefaultSupportedProtocols() {
        if (inFipsJvm()) {
            assertThat(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS, contains("TLSv1.2", "TLSv1.1"));
        } else {
            assertThat(XPackSettings.DEFAULT_SUPPORTED_PROTOCOLS, contains("TLSv1.3", "TLSv1.2", "TLSv1.1"));

        }
    }

    public void testServiceTokenHashingAlgorithmSettingValidation() {
        final boolean isPBKDF2Available = isSecretkeyFactoryAlgoAvailable("PBKDF2WithHMACSHA512");
        final String pbkdf2Algo = randomFrom("PBKDF2_10000", "PBKDF2", "PBKDF2_STRETCH");
        final Settings settings = Settings.builder().put(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.getKey(), pbkdf2Algo).build();
        if (isPBKDF2Available) {
            assertEquals(pbkdf2Algo, XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(settings));
        } else {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(settings)
            );
            assertThat(e.getMessage(), containsString("Support for PBKDF2WithHMACSHA512 must be available"));
        }

        final String bcryptAlgo = randomFrom("BCRYPT", "BCRYPT11");
        assertEquals(
            bcryptAlgo,
            XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(
                Settings.builder().put(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.getKey(), bcryptAlgo).build()
            )
        );
    }

    public void testDefaultServiceTokenHashingAlgorithm() {
        assertThat(XPackSettings.SERVICE_TOKEN_HASHING_ALGORITHM.get(Settings.EMPTY), equalTo("PBKDF2_STRETCH"));
    }

    public void testRemoteClusterSslSettings() {
        assumeTrue("tests Remote Cluster Security 2.0 functionality", TcpTransport.isUntrustedRemoteClusterEnabled());
        final List<Setting<?>> allSettings = XPackSettings.getAllSettings();

        final List<String> remoteClusterSslSettingKeys = allSettings.stream()
            .map(Setting::getKey)
            .filter(key -> key.startsWith("xpack.security.remote_cluster_"))
            .toList();

        // Ensure client_authentication is only available for server and verification_mode is only available for client
        assertThat(remoteClusterSslSettingKeys, not(hasItem("xpack.security.remote_cluster_server.ssl.verification_mode")));
        assertThat(remoteClusterSslSettingKeys, hasItem("xpack.security.remote_cluster_client.ssl.verification_mode"));

        assertThat(remoteClusterSslSettingKeys, hasItem("xpack.security.remote_cluster_server.ssl.client_authentication"));
        assertThat(remoteClusterSslSettingKeys, not(hasItem("xpack.security.remote_cluster_client.ssl.client_authentication")));

        // None of them allow insecure password
        List.of(
            "xpack.security.remote_cluster_server.ssl.keystore.password",
            "xpack.security.remote_cluster_server.ssl.keystore.key_password",
            "xpack.security.remote_cluster_server.ssl.key_passphrase",
            "xpack.security.remote_cluster_server.ssl.truststore.password",
            "xpack.security.remote_cluster_client.ssl.keystore.password",
            "xpack.security.remote_cluster_client.ssl.keystore.key_password",
            "xpack.security.remote_cluster_client.ssl.key_passphrase",
            "xpack.security.remote_cluster_client.ssl.truststore.password"
        ).forEach(key -> assertThat(remoteClusterSslSettingKeys, not(hasItem(key))));
    }

    public void testSecurityMustBeEnableForRemoteClusterServer() {
        // Security on, remote cluster server off
        final Settings.Builder builder1 = Settings.builder();
        if (randomBoolean()) {
            builder1.put("xpack.security.enabled", "true");
        }
        if (randomBoolean()) {
            builder1.put("remote_cluster_server.enabled", "false");
        }
        assertThat(XPackSettings.SECURITY_ENABLED.get(builder1.build()), is(true));

        // Security off, remote cluster server off
        final Settings.Builder builder2 = Settings.builder().put("xpack.security.enabled", "false");
        if (randomBoolean()) {
            builder2.put("remote_cluster_server.enabled", "false");
        }
        assertThat(XPackSettings.SECURITY_ENABLED.get(builder2.build()), is(false));

        // Security on, remote cluster server on
        final Settings.Builder builder3 = Settings.builder().put("remote_cluster_server.enabled", "true");
        if (randomBoolean()) {
            builder3.put("xpack.security.enabled", "true");
        }
        final Settings settings3 = builder3.build();
        assertThat(XPackSettings.SECURITY_ENABLED.get(settings3), is(true));
        assertThat(RemoteClusterPortSettings.REMOTE_CLUSTER_SERVER_ENABLED.get(settings3), is(true));

        // Security off, remote cluster server on
        final Settings settings4 = Settings.builder()
            .put("remote_cluster_server.enabled", "true")
            .put("xpack.security.enabled", "false")
            .build();
        final IllegalArgumentException e4 = expectThrows(
            IllegalArgumentException.class,
            () -> XPackSettings.SECURITY_ENABLED.get(settings4)
        );
        assertThat(
            e4.getMessage(),
            containsString("Security [xpack.security.enabled] must be enabled to use the remote cluster server feature")
        );
    }

    private boolean isSecretkeyFactoryAlgoAvailable(String algorithmId) {
        try {
            SecretKeyFactory.getInstance(algorithmId);
            return true;
        } catch (NoSuchAlgorithmException e) {
            return false;
        }
    }
}
