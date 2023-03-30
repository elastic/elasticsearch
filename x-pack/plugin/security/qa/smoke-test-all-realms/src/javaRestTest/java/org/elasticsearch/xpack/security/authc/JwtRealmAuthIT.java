/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.jwt.JwtRealm;

import java.io.IOException;
import java.util.Map;

/**
 * Integration Rest Test for testing authentication when all possible realms are configured
 */
public class JwtRealmAuthIT extends SecurityRealmSmokeTestCase {
    private static final Logger LOGGER = LogManager.getLogger(JwtRealmAuthIT.class);

    // Declared in build.gradle
    private static final String USERNAME = "security_test_user";
    private static final String HEADER_CLIENT_SECRET = "client-shared-secret-string";
    private static final String HEADER_JWT = "eyJhbGciOiJIUzI1NiJ9."
        + "eyJhdWQiOiJhdWQ4Iiwic3ViIjoic2VjdXJpdHlfdGVzdF91c2VyIiwicm9sZXMiOiJbc2VjdXJpdHl"
        + "fdGVzdF9yb2xlXSIsImlzcyI6ImlzczgiLCJleHAiOjQwNzA5MDg4MDAsImlhdCI6OTQ2Njg0ODAwfQ"
        + ".YbMbSEY8j3BdE_M71np-5Q9DFHGcjZcu7D4Kk1Ji0wE";

    public void testAuthenticationUsingJwtRealm() throws IOException {
        final RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder()
            .addHeader(
                JwtRealm.HEADER_CLIENT_AUTHENTICATION,
                JwtRealm.HEADER_SHARED_SECRET_AUTHENTICATION_SCHEME + " " + HEADER_CLIENT_SECRET
            )
            .addHeader(JwtRealm.HEADER_END_USER_AUTHENTICATION, JwtRealm.HEADER_END_USER_AUTHENTICATION_SCHEME + " " + HEADER_JWT);

        final Map<String, Object> authenticate = super.authenticate(options);
        assertUsername(authenticate, USERNAME);
        assertRealm(authenticate, "jwt", "jwt8");
        assertRoles(authenticate); // empty list
        assertNoApiKeyInfo(authenticate, Authentication.AuthenticationType.REALM);
    }

}
