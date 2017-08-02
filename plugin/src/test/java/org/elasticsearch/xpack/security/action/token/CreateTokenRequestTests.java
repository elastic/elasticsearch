/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.token;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.hasItem;

public class CreateTokenRequestTests extends ESTestCase {

    public void testRequestValidation() throws Exception {
        CreateTokenRequest request = new CreateTokenRequest();
        ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertEquals(3, ve.validationErrors().size());

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
    }
}
