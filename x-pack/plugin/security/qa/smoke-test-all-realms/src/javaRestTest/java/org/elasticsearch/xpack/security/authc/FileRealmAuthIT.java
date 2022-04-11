/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;

import java.io.IOException;
import java.util.Map;

/**
 * Integration Rest Test for testing authentication when all possible realms are configured
 */
public class FileRealmAuthIT extends SecurityRealmSmokeTestCase {

    // Declared in build.gradle
    private static final String USERNAME = "security_test_user";
    private static final SecureString PASSWORD = new SecureString("security-test-password".toCharArray());
    private static final String ROLE_NAME = "security_test_role";

    public void testAuthenticationUsingFileRealm() throws IOException {
        Map<String, Object> authenticate = super.authenticate(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(USERNAME, PASSWORD))
        );

        assertUsername(authenticate, USERNAME);
        assertRealm(authenticate, "file", "file0");
        assertRoles(authenticate, ROLE_NAME);
        assertNoApiKeyInfo(authenticate, Authentication.AuthenticationType.REALM);
    }

}
