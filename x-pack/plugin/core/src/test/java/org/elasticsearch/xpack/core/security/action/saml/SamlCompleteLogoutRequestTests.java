/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.saml;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.containsString;

public class SamlCompleteLogoutRequestTests extends ESTestCase {

    public void testValidateFailsWhenQueryAndBodyBothNotExist() {
        final SamlCompleteLogoutRequest samlCompleteLogoutRequest = new SamlCompleteLogoutRequest();
        samlCompleteLogoutRequest.setRealm("realm");
        final ActionRequestValidationException validationException = samlCompleteLogoutRequest.validate();
        assertThat(validationException.getMessage(), containsString("query_string and content may not both be empty"));
    }

    public void testValidateFailsWhenQueryAndBodyBothSet() {
        final SamlCompleteLogoutRequest samlCompleteLogoutRequest = new SamlCompleteLogoutRequest();
        samlCompleteLogoutRequest.setRealm("realm");
        samlCompleteLogoutRequest.setQueryString("queryString");
        samlCompleteLogoutRequest.setContent("content");
        final ActionRequestValidationException validationException = samlCompleteLogoutRequest.validate();
        assertThat(validationException.getMessage(), containsString("query_string and content may not both present"));
    }

    public void testValidateFailsWhenRealmIsNotSet() {
        final SamlCompleteLogoutRequest samlCompleteLogoutRequest = new SamlCompleteLogoutRequest();
        samlCompleteLogoutRequest.setQueryString("queryString");
        final ActionRequestValidationException validationException = samlCompleteLogoutRequest.validate();
        assertThat(validationException.getMessage(), containsString("realm may not be empty"));
    }

    public void testCannotSetQueryStringTwice() {
        final SamlCompleteLogoutRequest samlCompleteLogoutRequest = new SamlCompleteLogoutRequest();
        samlCompleteLogoutRequest.setQueryString("query_string");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> samlCompleteLogoutRequest.setQueryString("queryString")
        );
        assertThat(e.getMessage(), containsString("Must use either [query_string] or [queryString], not both at the same time"));
    }
}
