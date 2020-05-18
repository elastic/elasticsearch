/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action.saml;

import junit.framework.TestCase;
import org.elasticsearch.action.ActionRequestValidationException;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertThat;

public class SamlCompleteLogoutRequestTests extends TestCase {

    public void testValidateFailsWhenQueryAndBodyBothNotExist() {
        final SamlCompleteLogoutRequest samlCompleteLogoutRequest = new SamlCompleteLogoutRequest();
        final ActionRequestValidationException validationException = samlCompleteLogoutRequest.validate();
        assertThat(validationException.getMessage(), containsString("queryString and content may not both be null"));
    }

    public void testValidateFailsWhenQueryAndBodyBothSet() {
        final SamlCompleteLogoutRequest samlCompleteLogoutRequest = new SamlCompleteLogoutRequest();
        samlCompleteLogoutRequest.setQueryString("queryString");
        samlCompleteLogoutRequest.setContent("content");
        final ActionRequestValidationException validationException = samlCompleteLogoutRequest.validate();
        assertThat(validationException.getMessage(), containsString("queryString and content may not both present"));
    }

}
