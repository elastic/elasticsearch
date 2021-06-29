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
import org.elasticsearch.xpack.core.security.action.oidc.OpenIdConnectPrepareAuthenticationRequest;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OpenIdConnectPrepareAuthenticationRequestTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final OpenIdConnectPrepareAuthenticationRequest request = new OpenIdConnectPrepareAuthenticationRequest();
        request.setRealmName("oidc-realm1");
        final BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        final OpenIdConnectPrepareAuthenticationRequest deserialized =
            new OpenIdConnectPrepareAuthenticationRequest(out.bytes().streamInput());
        assertThat(deserialized.getRealmName(), equalTo("oidc-realm1"));

        final OpenIdConnectPrepareAuthenticationRequest request2 = new OpenIdConnectPrepareAuthenticationRequest();
        request2.setIssuer("https://op.company.org/");
        final BytesStreamOutput out2 = new BytesStreamOutput();
        request2.writeTo(out2);

        final OpenIdConnectPrepareAuthenticationRequest deserialized2 =
            new OpenIdConnectPrepareAuthenticationRequest(out2.bytes().streamInput());
        assertThat(deserialized2.getIssuer(), equalTo("https://op.company.org/"));
    }

    public void testSerializationWithStateAndNonce() throws IOException {
        final OpenIdConnectPrepareAuthenticationRequest request = new OpenIdConnectPrepareAuthenticationRequest();
        final String nonce = randomAlphaOfLengthBetween(8, 12);
        final String state = randomAlphaOfLengthBetween(8, 12);
        request.setRealmName("oidc-realm1");
        request.setNonce(nonce);
        request.setState(state);
        final BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        final OpenIdConnectPrepareAuthenticationRequest deserialized =
            new OpenIdConnectPrepareAuthenticationRequest(out.bytes().streamInput());
        assertThat(deserialized.getRealmName(), equalTo("oidc-realm1"));
        assertThat(deserialized.getState(), equalTo(state));
        assertThat(deserialized.getNonce(), equalTo(nonce));
    }

    public void testValidation() {
        final OpenIdConnectPrepareAuthenticationRequest request = new OpenIdConnectPrepareAuthenticationRequest();
        final ActionRequestValidationException validation = request.validate();
        assertNotNull(validation);
        assertThat(validation.validationErrors().size(), equalTo(1));
        assertThat(validation.validationErrors().get(0), containsString("one of [realm, issuer] must be provided"));

        final OpenIdConnectPrepareAuthenticationRequest request2 = new OpenIdConnectPrepareAuthenticationRequest();
        request2.setRealmName("oidc-realm1");
        request2.setIssuer("https://op.company.org/");
        final ActionRequestValidationException validation2 = request2.validate();
        assertNotNull(validation2);
        assertThat(validation2.validationErrors().size(), equalTo(1));
        assertThat(validation2.validationErrors().get(0),
            containsString("only one of [realm, issuer] can be provided in the same request"));
    }
}
