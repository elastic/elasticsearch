/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.AbstractBootstrapCheckTestCase;

public class FIPS140JKSKeystoreBootstrapCheckTests extends AbstractBootstrapCheckTestCase {

    public void testNoKeystoreIsAllowed() {
        final Settings.Builder settings = Settings.builder()
            .put("xpack.security.fips_mode.enabled", "true");
        assertFalse(new FIPS140JKSKeystoreBootstrapCheck().check(createTestContext(settings.build(), null)).isFailure());
    }

    public void testTransportSSLKeystoreTypeIsNotAllowed() {
        final Settings.Builder settings = Settings.builder()
            .put("xpack.security.fips_mode.enabled", "true")
            .put("xpack.security.transport.ssl.keystore.path", "/this/is/the/path")
            .put("xpack.security.transport.ssl.keystore.type", "JKS");
        assertTrue(new FIPS140JKSKeystoreBootstrapCheck().check(createTestContext(settings.build(), null)).isFailure());
    }

    public void testHttpSSLKeystoreTypeIsNotAllowed() {
        final Settings.Builder settings = Settings.builder()
            .put("xpack.security.fips_mode.enabled", "true")
            .put("xpack.security.http.ssl.keystore.path", "/this/is/the/path")
            .put("xpack.security.http.ssl.keystore.type", "JKS");
        assertTrue(new FIPS140JKSKeystoreBootstrapCheck().check(createTestContext(settings.build(), null)).isFailure());
    }

    public void testRealmKeystoreTypeIsNotAllowed() {
        final Settings.Builder settings = Settings.builder()
            .put("xpack.security.fips_mode.enabled", "true")
            .put("xpack.security.authc.realms.ldap.ssl.keystore.path", "/this/is/the/path")
            .put("xpack.security.authc.realms.ldap.ssl.keystore.type", "JKS");
        assertTrue(new FIPS140JKSKeystoreBootstrapCheck().check(createTestContext(settings.build(), null)).isFailure());
    }

    public void testImplicitRealmKeystoreTypeIsNotAllowed() {
        final Settings.Builder settings = Settings.builder()
            .put("xpack.security.fips_mode.enabled", "true")
            .put("xpack.security.authc.realms.ldap.ssl.keystore.path", "/this/is/the/path");
        assertTrue(new FIPS140JKSKeystoreBootstrapCheck().check(createTestContext(settings.build(), null)).isFailure());
    }
}
