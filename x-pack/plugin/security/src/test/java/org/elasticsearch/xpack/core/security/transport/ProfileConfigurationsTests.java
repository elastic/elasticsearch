/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslClientAuthenticationMode;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.hamcrest.Matchers;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.is;

public class ProfileConfigurationsTests extends ESTestCase {

    public void testGetSecureTransportProfileConfigurations() {
        assumeFalse("Can't run in a FIPS JVM, uses JKS/PKCS12 keystores", inFipsJvm());
        final Settings settings = getBaseSettings().put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .put("transport.profiles.full.xpack.security.ssl.verification_mode", SslVerificationMode.FULL.name())
            .put("transport.profiles.cert.xpack.security.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(env);
        final SslConfiguration defaultConfig = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, true);
        assertThat(profileConfigurations.size(), Matchers.equalTo(3));
        assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("full", "cert", "default"));
        assertThat(profileConfigurations.get("full").verificationMode(), Matchers.equalTo(SslVerificationMode.FULL));
        assertThat(profileConfigurations.get("cert").verificationMode(), Matchers.equalTo(SslVerificationMode.CERTIFICATE));
        assertThat(profileConfigurations.get("default"), Matchers.sameInstance(defaultConfig));
    }

    public void testGetInsecureTransportProfileConfigurations() {
        assumeFalse("Can't run in a FIPS JVM with verification mode None", inFipsJvm());
        final Settings settings = getBaseSettings().put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .put("transport.profiles.none.xpack.security.ssl.verification_mode", SslVerificationMode.NONE.name())
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(env);
        final SslConfiguration defaultConfig = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, true);
        assertThat(profileConfigurations.size(), Matchers.equalTo(2));
        assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("none", "default"));
        assertThat(profileConfigurations.get("none").verificationMode(), Matchers.equalTo(SslVerificationMode.NONE));
        assertThat(profileConfigurations.get("default"), Matchers.sameInstance(defaultConfig));
    }

    public void testTransportAndRemoteClusterSslCanBeEnabledIndependently() {
        assumeFalse("Can't run in a FIPS JVM with JKS/PKCS12 keystore or verification mode None", inFipsJvm());
        final boolean transportSslEnabled = randomBoolean();
        final boolean remoteClusterServerSslEnabled = randomBoolean();
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.enabled", transportSslEnabled)
            .put("xpack.security.transport.ssl.keystore.path", getKeystorePath().toString())
            .put("xpack.security.transport.ssl.verification_mode", SslVerificationMode.NONE.name())
            .put("remote_cluster_server.enabled", true)
            .put("xpack.security.remote_cluster_server.ssl.enabled", remoteClusterServerSslEnabled)
            .put("xpack.security.remote_cluster_server.ssl.keystore.path", getKeystorePath().toString())
            .put("xpack.security.remote_cluster_server.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .setSecureSettings(getKeystoreSecureSettings("transport", "remote_cluster_server"))
            .put("xpack.security.remote_cluster_client.ssl.enabled", randomBoolean())  // client SSL won't be processed
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(env);
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, true);

        if (transportSslEnabled && remoteClusterServerSslEnabled) {
            assertThat(profileConfigurations.size(), Matchers.equalTo(2));
            assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("default", "_remote_cluster"));
            assertThat(profileConfigurations.get("_remote_cluster").verificationMode(), Matchers.equalTo(SslVerificationMode.CERTIFICATE));
            assertThat(
                profileConfigurations.get("default"),
                Matchers.sameInstance(sslService.getSSLConfiguration("xpack.security.transport.ssl"))
            );
        } else if (transportSslEnabled) {
            assertThat(profileConfigurations.size(), Matchers.equalTo(1));
            assertThat(profileConfigurations.keySet(), contains("default"));
            assertThat(
                profileConfigurations.get("default"),
                Matchers.sameInstance(sslService.getSSLConfiguration("xpack.security.transport.ssl"))
            );
        } else if (remoteClusterServerSslEnabled) {
            assertThat(profileConfigurations.size(), Matchers.equalTo(1));
            assertThat(profileConfigurations.keySet(), contains("_remote_cluster"));
            assertThat(profileConfigurations.get("_remote_cluster").verificationMode(), Matchers.equalTo(SslVerificationMode.CERTIFICATE));
        } else {
            assertThat(profileConfigurations, anEmptyMap());
        }
    }

    public void testNoProfileConfigurationForRemoteClusterIfFeatureIsDisabled() {
        assumeFalse("Can't run in a FIPS JVM with JKS/PKCS12 keystore or verification mode None", inFipsJvm());
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.keystore.path", getKeystorePath().toString())
            .put("xpack.security.transport.ssl.verification_mode", SslVerificationMode.NONE.name())
            .put("remote_cluster_server.enabled", false)
            .put("xpack.security.remote_cluster_server.ssl.enabled", true)
            .put("xpack.security.remote_cluster_server.ssl.keystore.path", getKeystorePath().toString())
            .put("xpack.security.remote_cluster_server.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .setSecureSettings(getKeystoreSecureSettings("transport", "remote_cluster_server"))
            .put("xpack.security.remote_cluster_client.ssl.enabled", randomBoolean())  // client SSL won't be processed
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(env);
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, true);
        assertThat(profileConfigurations.size(), Matchers.equalTo(1));
        assertThat(profileConfigurations.keySet(), contains("default"));
        assertThat(
            profileConfigurations.get("default"),
            Matchers.sameInstance(sslService.getSSLConfiguration("xpack.security.transport.ssl"))
        );
    }

    public void testGetProfileConfigurationsIrrespectiveToSslEnabled() {
        assumeFalse("Can't run in a FIPS JVM with JKS/PKCS12 keystore or verification mode None", inFipsJvm());
        final boolean remoteClusterPortEnabled = randomBoolean();
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.enabled", false)
            .put("xpack.security.transport.ssl.keystore.path", getKeystorePath().toString())
            .put("transport.profiles.client.xpack.security.ssl.client_authentication", SslClientAuthenticationMode.NONE)
            .put("remote_cluster_server.enabled", remoteClusterPortEnabled)
            .put("xpack.security.transport.ssl.verification_mode", SslVerificationMode.NONE.name())
            .put("xpack.security.remote_cluster_server.ssl.enabled", false)
            .put("xpack.security.remote_cluster_server.ssl.keystore.path", getKeystorePath().toString())
            .put("xpack.security.remote_cluster_server.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .setSecureSettings(getKeystoreSecureSettings("transport", "remote_cluster_server"))
            .put("xpack.security.remote_cluster_client.ssl.enabled", randomBoolean())  // client SSL won't be processed
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(env);
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, false);
        if (remoteClusterPortEnabled) {
            assertThat(profileConfigurations.size(), Matchers.equalTo(3));
            assertThat(profileConfigurations.keySet(), containsInAnyOrder("default", "client", "_remote_cluster"));
            assertThat(profileConfigurations.get("_remote_cluster").verificationMode(), is(SslVerificationMode.CERTIFICATE));
        } else {
            assertThat(profileConfigurations.size(), Matchers.equalTo(2));
            assertThat(profileConfigurations.keySet(), containsInAnyOrder("default", "client"));
        }
        assertThat(
            profileConfigurations.get("default"),
            Matchers.sameInstance(sslService.getSSLConfiguration("xpack.security.transport.ssl"))
        );
        assertThat(profileConfigurations.get("client").clientAuth(), is(SslClientAuthenticationMode.NONE));
    }

    private Settings.Builder getBaseSettings() {
        return Settings.builder()
            .setSecureSettings(getKeystoreSecureSettings("transport"))
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.keystore.path", getKeystorePath().toString());
    }

    private Path getKeystorePath() {
        final Path keystore = randomBoolean()
            ? getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks")
            : getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.p12");
        return keystore;
    }

    private static MockSecureSettings getKeystoreSecureSettings(String... sslContexts) {
        final MockSecureSettings secureSettings = new MockSecureSettings();
        Arrays.stream(sslContexts).forEach(sslContext -> {
            secureSettings.setString("xpack.security." + sslContext + ".ssl.keystore.secure_password", "testnode");
        });
        return secureSettings;
    }

}
