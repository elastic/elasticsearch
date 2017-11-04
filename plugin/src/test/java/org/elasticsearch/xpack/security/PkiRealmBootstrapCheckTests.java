/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.bootstrap.BootstrapContext;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.security.authc.pki.PkiRealm;
import org.elasticsearch.xpack.ssl.SSLService;

public class PkiRealmBootstrapCheckTests extends ESTestCase {

    public void testPkiRealmBootstrapDefault() throws Exception {
        assertFalse(new PkiRealmBootstrapCheck(new SSLService(Settings.EMPTY,
                TestEnvironment.newEnvironment(Settings.builder().put("path.home", createTempDir()).build()))).check(
                new BootstrapContext(Settings.EMPTY, null)).isFailure());
    }

    public void testBootstrapCheckWithPkiRealm() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.test_pki.type", PkiRealm.TYPE)
                .put("path.home", createTempDir())
                .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        assertTrue(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());

        // enable transport tls
        settings = Settings.builder().put(settings)
                .put("xpack.security.transport.ssl.enabled", true)
                .build();
        assertFalse(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());

        // disable client auth default
        settings = Settings.builder().put(settings)
                .put("xpack.ssl.client_authentication", "none")
                .build();
        env = TestEnvironment.newEnvironment(settings);
        assertTrue(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());

        // enable ssl for http
        settings = Settings.builder().put(settings)
                .put("xpack.security.http.ssl.enabled", true)
                .build();
        env = TestEnvironment.newEnvironment(settings);
        assertTrue(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());

        // enable client auth for http
        settings = Settings.builder().put(settings)
                .put("xpack.security.http.ssl.client_authentication", randomFrom("required", "optional"))
                .build();
        env = TestEnvironment.newEnvironment(settings);
        assertFalse(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());

        // disable http ssl
        settings = Settings.builder().put(settings)
                .put("xpack.security.http.ssl.enabled", false)
                .build();
        env = TestEnvironment.newEnvironment(settings);
        assertTrue(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());

        // set transport client auth
        settings = Settings.builder().put(settings)
                .put("xpack.security.transport.client_authentication", randomFrom("required", "optional"))
                .build();
        env = TestEnvironment.newEnvironment(settings);
        assertTrue(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());

        // test with transport profile
        settings = Settings.builder().put(settings)
                .put("xpack.security.transport.client_authentication", "none")
                .put("transport.profiles.foo.xpack.security.ssl.client_authentication", randomFrom("required", "optional"))
                .build();
        env = TestEnvironment.newEnvironment(settings);
        assertFalse(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());
    }

    public void testBootstrapCheckWithDisabledRealm() throws Exception {
        Settings settings = Settings.builder()
                .put("xpack.security.authc.realms.test_pki.type", PkiRealm.TYPE)
                .put("xpack.security.authc.realms.test_pki.enabled", false)
                .put("xpack.ssl.client_authentication", "none")
                .put("path.home", createTempDir())
                .build();
        Environment env = TestEnvironment.newEnvironment(settings);
        assertFalse(new PkiRealmBootstrapCheck(new SSLService(settings, env)).check(new BootstrapContext(settings, null)).isFailure());
    }
}
