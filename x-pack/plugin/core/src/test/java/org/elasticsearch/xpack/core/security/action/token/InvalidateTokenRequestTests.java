/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class InvalidateTokenRequestTests extends ESTestCase {

    public void testValidation() {
        InvalidateTokenRequest request = new InvalidateTokenRequest();
        ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0), containsString("token string must be provided when not specifying a realm"));

        request = new InvalidateTokenRequest(randomAlphaOfLength(12), randomFrom("", null));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0), containsString("token type must be provided when a token string is specified"));

        request = new InvalidateTokenRequest(randomFrom("", null), "access_token");
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0), containsString("token string must be provided when not specifying a realm"));

        request = new InvalidateTokenRequest(randomFrom("", null), randomFrom("", null), randomAlphaOfLength(4), randomAlphaOfLength(8));
        ve = request.validate();
        assertNull(ve);

        request =
            new InvalidateTokenRequest(randomAlphaOfLength(4), randomFrom("", null), randomAlphaOfLength(4), randomAlphaOfLength(8));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0),
            containsString("token string must not be provided when realm name or username is specified"));

        request = new InvalidateTokenRequest(randomAlphaOfLength(4), randomFrom("token", "refresh_token"),
            randomAlphaOfLength(4), randomAlphaOfLength(8));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0),
            containsString("token string must not be provided when realm name or username is specified"));
        assertThat(ve.validationErrors().get(1),
            containsString("token type must not be provided when realm name or username is specified"));

        request =
            new InvalidateTokenRequest(randomAlphaOfLength(4), randomFrom("", null), randomAlphaOfLength(4), randomAlphaOfLength(8));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0),
            containsString("token string must not be provided when realm name or username is specified"));

        request =
            new InvalidateTokenRequest(randomAlphaOfLength(4), randomFrom("token", "refresh_token"), randomFrom("", null),
                randomAlphaOfLength(8));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(2, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0),
            containsString("token string must not be provided when realm name or username is specified"));
        assertThat(ve.validationErrors().get(1),
            containsString("token type must not be provided when realm name or username is specified"));

        request = new InvalidateTokenRequest(randomAlphaOfLength(4), randomFrom("", null), randomFrom("", null), randomAlphaOfLength(8));
        ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0),
            containsString("token string must not be provided when realm name or username is specified"));
    }
}
