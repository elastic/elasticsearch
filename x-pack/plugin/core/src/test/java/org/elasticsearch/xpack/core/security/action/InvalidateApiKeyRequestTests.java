/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class InvalidateApiKeyRequestTests extends ESTestCase {

    public void testRequestValidation() {
        InvalidateApiKeyRequest request = new InvalidateApiKeyRequest();
        ActionRequestValidationException ve = request.validate();
        assertNotNull(ve);
        assertEquals(1, ve.validationErrors().size());
        assertThat(ve.validationErrors().get(0),
                containsString("api key id or api key name or username and/or realm name must be specified"));

        request = InvalidateApiKeyRequest.usingApiKeyId(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingApiKeyName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingRealmName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingUserName(randomAlphaOfLength(5));
        ve = request.validate();
        assertNull(ve);
        request = InvalidateApiKeyRequest.usingRealmAndUserName(randomAlphaOfLength(5), randomAlphaOfLength(7));
        ve = request.validate();
        assertNull(ve);
    }
}
