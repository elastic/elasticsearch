/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.oidc;


import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectAuthenticateRequest;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class OpenIdConnectAuthenticateRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final OpenIdConnectAuthenticateRequest request = new OpenIdConnectAuthenticateRequest();
        final String nonce = randomBoolean() ? null : randomAlphaOfLengthBetween(8, 12);
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
}
