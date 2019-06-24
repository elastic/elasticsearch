/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;

public class CreateTokenRequestTests extends ESTestCase {

    public void testRequestValidation() {
        CreateTokenRequest request = new CreateTokenRequest();
        ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0), containsString("[password, _kerberos, refresh_token, client_credentials]"));
        assertThat(ve.validationErrors().get(0), containsString("grant_type"));

        request.setGrantType("password");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("username is missing"));
        assertThat(ve.validationErrors(), hasItem("password is missing"));

        request.setUsername(randomBoolean() ? null : "");
        request.setPassword(randomBoolean() ? null : new SecureString(new char[] {}));

        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("username is missing"));
        assertThat(ve.validationErrors(), hasItem("password is missing"));

        request.setUsername(randomAlphaOfLengthBetween(1, 256));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("password is missing"));

        request.setPassword(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        ve = request.validate();
        assertNull(ve);

        request.setRefreshToken(randomAlphaOfLengthBetween(1, 10));
        request.setKerberosTicket(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("refresh_token is not supported")));

        request.setGrantType("refresh_token");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(3, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("username is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("password is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));

        request.setUsername(null);
        request.setPassword(null);
        request.setKerberosTicket(null);
        ve = request.validate();
        assertNull(ve);

        request.setRefreshToken(null);
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem("refresh_token is missing"));

        request.setGrantType("client_credentials");
        ve = request.validate();
        assertNull(ve);

        request.setUsername(randomAlphaOfLengthBetween(1, 32));
        request.setPassword(new SecureString(randomAlphaOfLengthBetween(1, 32).toCharArray()));
        request.setKerberosTicket(new SecureString(randomAlphaOfLengthBetween(1, 256).toCharArray()));
        request.setRefreshToken(randomAlphaOfLengthBetween(1, 32));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(4, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("username is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("password is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("refresh_token is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("kerberos_ticket is not supported")));

        request.setGrantType("_kerberos");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(3, ve.validationErrors().size());
        assertThat(ve.validationErrors(), hasItem(containsString("username is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("password is not supported")));
        assertThat(ve.validationErrors(), hasItem(containsString("refresh_token is not supported")));

        request.setUsername(null);
        request.setPassword(null);
        request.setRefreshToken(null);
        ve = request.validate();
        assertNull(ve);
    }
}
