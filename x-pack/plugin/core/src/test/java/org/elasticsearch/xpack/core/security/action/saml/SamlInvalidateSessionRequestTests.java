/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class SamlInvalidateSessionRequestTests extends ESTestCase {

    public void testCannotSetQueryStringTwice() {
        final SamlInvalidateSessionRequest samlInvalidateSessionRequest = new SamlInvalidateSessionRequest();
        samlInvalidateSessionRequest.setQueryString("query_string");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> samlInvalidateSessionRequest.setQueryString("queryString")
        );
        assertThat(e.getMessage(), containsString("Must use either [query_string] or [queryString], not both at the same time"));
    }
}
