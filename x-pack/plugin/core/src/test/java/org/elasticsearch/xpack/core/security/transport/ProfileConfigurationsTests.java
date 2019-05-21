/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.transport;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ssl.SSLConfiguration;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.core.ssl.VerificationMode;
import org.hamcrest.Matchers;

import java.util.Map;

public class ProfileConfigurationsTests extends ESTestCase {

    public void testGetSecureTransportProfileConfigurations() {
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.verification_mode", VerificationMode.CERTIFICATE.name())
            .put("transport.profiles.full.xpack.security.ssl.verification_mode", VerificationMode.FULL.name())
            .put("transport.profiles.cert.xpack.security.ssl.verification_mode", VerificationMode.CERTIFICATE.name())
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration defaultConfig = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Map<String, SSLConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, defaultConfig);
        assertThat(profileConfigurations.size(), Matchers.equalTo(3));
        assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("full", "cert", "default"));
        assertThat(profileConfigurations.get("full").verificationMode(), Matchers.equalTo(VerificationMode.FULL));
        assertThat(profileConfigurations.get("cert").verificationMode(), Matchers.equalTo(VerificationMode.CERTIFICATE));
        assertThat(profileConfigurations.get("default"), Matchers.sameInstance(defaultConfig));
    }

    public void testGetInsecureTransportProfileConfigurations() {
        assumeFalse("Can't run in a FIPS JVM with verification mode None", inFipsJvm());
        final Settings settings = Settings.builder()
            .put("path.home", createTempDir())
            .put("xpack.security.transport.ssl.verification_mode", VerificationMode.CERTIFICATE.name())
            .put("transport.profiles.none.xpack.security.ssl.verification_mode", VerificationMode.NONE.name())
            .build();
        final Environment env = TestEnvironment.newEnvironment(settings);
        SSLService sslService = new SSLService(settings, env);
        final SSLConfiguration defaultConfig = sslService.getSSLConfiguration("xpack.security.transport.ssl");
        final Map<String, SSLConfiguration> profileConfigurations = ProfileConfigurations.get(settings, sslService, defaultConfig);
        assertThat(profileConfigurations.size(), Matchers.equalTo(2));
        assertThat(profileConfigurations.keySet(), Matchers.containsInAnyOrder("none", "default"));
        assertThat(profileConfigurations.get("none").verificationMode(), Matchers.equalTo(VerificationMode.NONE));
        assertThat(profileConfigurations.get("default"), Matchers.sameInstance(defaultConfig));
    }
}
