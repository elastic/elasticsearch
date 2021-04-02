/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Integration Rest Test for testing authentication when all possible realms are configured
 */
public class NativeRealmAuthIT extends SecurityRealmSmokeTestCase {

    private static final String USERNAME = "test_native_user";
    private static final SecureString PASSWORD = new SecureString("native-user-password".toCharArray());
    private static final String ROLE_NAME = "native_role";

    @Before
    public void createUsersAndRoles() throws IOException {
        createUser(USERNAME, PASSWORD, List.of(ROLE_NAME));
        createRole("native_role", Set.of("monitor"));
    }

    @After
    public void cleanUp() throws IOException {
        deleteUser(USERNAME);
        deleteRole(ROLE_NAME);
    }

    public void testAuthenticationUsingNativeRealm() throws IOException {
        Map<String, Object> authenticate = super.authenticate(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization",
                UsernamePasswordToken.basicAuthHeaderValue(USERNAME, PASSWORD))
        );

        assertUsername(authenticate, USERNAME);
        assertRealm(authenticate, "native", "native1");
        assertRoles(authenticate, ROLE_NAME);
    }

}
