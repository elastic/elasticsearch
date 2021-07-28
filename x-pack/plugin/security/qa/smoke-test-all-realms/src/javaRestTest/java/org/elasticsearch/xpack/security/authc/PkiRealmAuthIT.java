/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;

import java.io.IOException;
import java.util.Map;

/**
 * Integration Rest Test for testing authentication when all possible realms are configured
 */
public class PkiRealmAuthIT extends SecurityRealmSmokeTestCase {

    // Derived from certificate attributes (pki-auth.crt)
    private static final String USERNAME = "pki-auth";

    @Override
    protected Settings restClientSettings() {
        Settings.Builder builder = Settings.builder()
            .put(super.restClientSettings())
            .put(CLIENT_CERT_PATH, getDataPath("/ssl/pki-auth.crt"))
            .put(CLIENT_KEY_PATH, getDataPath("/ssl/pki-auth.key"))
            .put(CLIENT_KEY_PASSWORD, "http-password");
        builder.remove(ThreadContext.PREFIX + ".Authorization");
        return builder.build();
    }

    public void testAuthenticationUsingFileRealm() throws IOException {
        Map<String, Object> authenticate = super.authenticate(RequestOptions.DEFAULT.toBuilder());

        assertUsername(authenticate, USERNAME);
        assertRealm(authenticate, "pki", "pki4");
        assertRoles(authenticate, new String[0]);
    }

}
