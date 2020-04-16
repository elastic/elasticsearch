/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.action.saml;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.core.security.action.saml.SamlPrepareAuthenticationRequest;
import org.elasticsearch.xpack.security.authc.saml.SamlTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

public class SamlPrepareAuthenticationRequestTests extends SamlTestCase {

    public void testSerialiseNonNullCriteria() throws IOException {
        final SamlPrepareAuthenticationRequest req = new SamlPrepareAuthenticationRequest();
        req.setRealmName("saml1");
        req.setAssertionConsumerServiceURL("https://sp.example.com/sso/saml2/post");
        req.setRelayState("the_relay_state");
        serialiseAndValidate(req);
    }

    public void testSerialiseNullCriteria() throws IOException {
        final SamlPrepareAuthenticationRequest req = new SamlPrepareAuthenticationRequest();
        req.setRealmName(null);
        req.setAssertionConsumerServiceURL(null);
        req.setRelayState(null);
        serialiseAndValidate(req);
    }

    private void serialiseAndValidate(SamlPrepareAuthenticationRequest req1) throws IOException {
        final BytesStreamOutput out = new BytesStreamOutput();
        req1.writeTo(out);

        final SamlPrepareAuthenticationRequest req2 = new SamlPrepareAuthenticationRequest(out.bytes().streamInput());

        assertThat(req2.getRealmName(), Matchers.equalTo(req1.getRealmName()));
        assertThat(req2.getAssertionConsumerServiceURL(), Matchers.equalTo(req1.getAssertionConsumerServiceURL()));
        assertThat(req2.getRelayState(), Matchers.equalTo(req1.getRelayState()));
        assertThat(req2.getParentTask(), Matchers.equalTo(req1.getParentTask()));
    }

}
