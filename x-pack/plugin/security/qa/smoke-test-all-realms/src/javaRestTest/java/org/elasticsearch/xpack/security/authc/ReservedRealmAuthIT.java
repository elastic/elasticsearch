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
import org.junit.Before;

import java.io.IOException;
import java.util.Map;

/**
 * Integration Rest Test for testing authentication when all possible realms are configured
 */
public class ReservedRealmAuthIT extends SecurityRealmSmokeTestCase {

    private static final String USERNAME = "kibana_system";
    private static final String ROLE_NAME = "kibana_system";

    private SecureString password;

    @Before
    public void setUserPassword() throws IOException {
        this.password = new SecureString(randomAlphaOfLengthBetween(14, 20).toCharArray());
        changePassword(USERNAME, password);
    }

    public void testAuthenticationUsingReservedRealm() throws IOException {
        Map<String, Object> authenticate = super.authenticate(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", UsernamePasswordToken.basicAuthHeaderValue(USERNAME, password))
        );

        assertUsername(authenticate, USERNAME);
        assertRealm(authenticate, "reserved", "reserved");
        assertRoles(authenticate, ROLE_NAME);
    }

}
