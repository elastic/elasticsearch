/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.action.oidc;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateRequest;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class OpenIdConnectAuthenticateRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final OpenIdConnectAuthenticateRequest request = new OpenIdConnectAuthenticateRequest();
        final String nonce = randomAlphaOfLengthBetween(8, 12);
        final String state = randomAlphaOfLengthBetween(8, 12);
        final String redirectUri = "https://rp.com/cb?code=thisisacode&state=" + state;
        request.setRedirectUri(redirectUri);
        request.setState(state);
        request.setNonce(nonce);
        final BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        final OpenIdConnectAuthenticateRequest unserialized = new OpenIdConnectAuthenticateRequest(out.bytes().streamInput());
        assertThat(unserialized.getRedirectUri(), equalTo(redirectUri));
        assertThat(unserialized.getState(), equalTo(state));
        assertThat(unserialized.getNonce(), equalTo(nonce));
    }

    public void testValidation() {
        final OpenIdConnectAuthenticateRequest request = new OpenIdConnectAuthenticateRequest();
        final ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertThat(validation.validationErrors(), hasSize(3));
        assertThat(validation.validationErrors().get(0), containsString("state parameter is missing"));
        assertThat(validation.validationErrors().get(1), containsString("nonce parameter is missing"));
        assertThat(validation.validationErrors().get(2), containsString("redirect_uri parameter is missing"));

        final OpenIdConnectAuthenticateRequest request2 = new OpenIdConnectAuthenticateRequest();
        request2.setRedirectUri("https://rp.company.com/cb?code=abc");
        request2.setState(randomAlphaOfLengthBetween(8, 12));
        final ActionRequestValidationException validation2 = request2.validate();
        assertNotNull(validation2);
        assertThat(validation2.validationErrors(), hasSize(1));
        assertThat(validation2.validationErrors().get(0), containsString("nonce parameter is missing"));
    }
}
