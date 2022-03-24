/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.SslConfiguration;
import org.elasticsearch.common.ssl.SslVerificationMode;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.hamcrest.Matchers;

import java.nio.file.Path;
import java.util.Map;

public class ProfileConfigurationsTests extends ESTestCase {

    public void testGetSecureTransportProfileConfigurations() {
        assumeFalse("Can't run in a FIPS JVM, uses JKS/PKCS12 keystores", inFipsJvm());
        final Settings settings = getBaseSettings().put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .put("xpack.security.transport.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .put("transport.profiles.full.xpack.security.ssl.verification_mode", SslVerificationMode.FULL.name())
            .put("transport.profiles.cert.xpack.security.ssl.verification_mode", SslVerificationMode.CERTIFICATE.name())
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(env);
        final SslConfiguration defaultConfig = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, defaultConfig);
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
        final Map<String, SslConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, defaultConfig);
        assertThat(profileConfigurations.size(), Matchers.equalTo(2));
        assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("none", "default"));
        assertThat(profileConfigurations.get("none").verificationMode(), Matchers.equalTo(SslVerificationMode.NONE));
        assertThat(profileConfigurations.get("default"), Matchers.sameInstance(defaultConfig));
    }

    private Settings.Builder getBaseSettings() {
        final Path keystore = randomBoolean()
            ? getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.jks")
            : getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.p12");

        MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.transport.ssl.keystore.secure_password", "testnode");

        return Settings.builder()
            .setSecureSettings(secureSettings)
            .put("xpack.security.transport.ssl.enabled", true)
            .put("xpack.security.transport.ssl.keystore.path", keystore.toString());
    }

}
