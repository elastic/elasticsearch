/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapCheck;
import org.elasticsearch.common.settings.MockSecureSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.hamcrest.Matchers;

import java.nio.file.Path;

public class PkiRealmBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testPkiRealmBootstrapDefault() throws Exception {
        final Settings settings = Settings.builder().put("path.home", createTempDir()).build();
        assertFalse(runCheck(settings).isFailure());
    }

    public void testBootstrapCheckWithPkiRealm() throws Exception {
        final Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");

        MockSecureSettings secureSettings = new MockSecureSettings();
        Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.pki.test_pki.order", 0)
                .put("path.home", createTempDir())
                .setSecureSettings(secureSettings)
                .build();
        assertTrue(runCheck(settings).isFailure());

        // enable transport tls
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
        settings = Settings.builder().put(settings)
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.security.transport.ssl.certificate", certPath)
                .put("xpack.security.transport.ssl.key", keyPath)
                .build();
        assertFalse(runCheck(settings).isFailure());

        // enable ssl for http
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        settings = Settings.builder().put(settings)
                .put("xpack.security.transport.ssl.enabled", false)
                .put("xpack.security.http.ssl.enabled", true)
                .put("xpack.security.http.ssl.certificate", certPath)
                .put("xpack.security.http.ssl.key", keyPath)
                .build();
        assertTrue(runCheck(settings).isFailure());

        // enable client auth for http
        settings = Settings.builder().put(settings)
                .put("xpack.security.http.ssl.client_authentication", randomFrom("required", "optional"))
                .build();
        assertFalse(runCheck(settings).isFailure());

        // disable http ssl
        settings = Settings.builder().put(settings)
                .put("xpack.security.http.ssl.enabled", false)
                .build();
        assertTrue(runCheck(settings).isFailure());

        // set transport auth
        settings = Settings.builder().put(settings)
                .put("xpack.security.transport.client_authentication", randomFrom("required", "optional"))
                .build();
        assertTrue(runCheck(settings).isFailure());

        // test with transport profile
        settings = Settings.builder().put(settings)
                .put("xpack.security.transport.ssl.enabled", true)
                .put("xpack.security.transport.client_authentication", "none")
                .put("transport.profiles.foo.xpack.security.ssl.client_authentication", randomFrom("required", "optional"))
                .build();
        assertFalse(runCheck(settings).isFailure());
    }

    private BootstrapCheck.BootstrapCheckResult runCheck(Settings settings) throws Exception {
        final SSLService sslService = new SSLService(TestEnvironment.newEnvironment(settings));
        return new PkiRealmBootstrapCheck(sslService).check(createTestContext(settings, null));
    }

    public void testBootstrapCheckWithDisabledRealm() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.pki.test_pki.enabled", false)
                .put("xpack.security.transport.ssl.enabled", false)
                .put("xpack.security.transport.ssl.client_authentication", "none")
                .put("path.home", createTempDir())
                .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        assertFalse(runCheck(settings).isFailure());
    }

    public void testBootstrapCheckWithDelegationEnabled() throws Exception {
        final Path certPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt");
        final Path keyPath = getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem");
        MockSecureSettings secureSettings = new MockSecureSettings();
        // enable transport tls
        secureSettings.setString("xpack.security.transport.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.pki.test_pki.enabled", true)
                .put("xpack.security.authc.realms.pki.test_pki.delegation.enabled", true)
                .put("xpack.security.transport.ssl.enabled", randomBoolean())
                .put("xpack.security.transport.ssl.client_authentication", "none")
                .put("xpack.security.transport.ssl.certificate", certPath.toString())
                .put("xpack.security.transport.ssl.key", keyPath.toString())
                .put("path.home", createTempDir())
                .setSecureSettings(secureSettings)
                .build();
        assertFalse(runCheck(settings).isFailure());
    }

    public void testBootstrapCheckWithClosedSecuredSetting() throws Exception {
        final boolean expectFail = randomBoolean();
        final MockSecureSettings secureSettings = new MockSecureSettings();
        secureSettings.setString("xpack.security.http.ssl.secure_key_passphrase", "testnode");
        Settings settings = Settings.builder()
            .put("xpack.security.authc.realms.pki.test_pki.order", 0)
            .put("xpack.security.http.ssl.enabled", true)
            .put("xpack.security.http.ssl.client_authentication", expectFail ? "none" : "optional")
            .put("xpack.security.http.ssl.key",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.pem"))
            .put("xpack.security.http.ssl.certificate",
                getDataPath("/org/elasticsearch/xpack/security/transport/ssl/certs/simple/testnode.crt"))
            .put("path.home", createTempDir())
            .setSecureSettings(secureSettings)
            .build();

        Environment env = TestEnvironment.newEnvironment(settings);
        final PkiRealmBootstrapCheck check = new PkiRealmBootstrapCheck(new SSLService(env));
        secureSettings.close();
        assertThat(check.check(createTestContext(settings, null)).isFailure(), Matchers.equalTo(expectFail));
    }
}
