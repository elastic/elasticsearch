/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

public class SamlValidateAuthnRequestRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final SamlValidateAuthnRequestRequest request = new SamlValidateAuthnRequestRequest();
        request.setQueryString("?SAMLRequest=x&RelayState=y&SigAlg=z&Signature=sig");
        final BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        final SamlValidateAuthnRequestRequest request1 = new SamlValidateAuthnRequestRequest(out.bytes().streamInput());
        assertThat(request.getQueryString(), equalTo(request1.getQueryString()));
        final ActionRequestValidationException exception = request1.validate();
        assertNull(exception);
    }

    public void testValidation() {
        final SamlValidateAuthnRequestRequest request = new SamlValidateAuthnRequestRequest();
        final ActionRequestValidationException exception = request.validate();
        assertNotNull(exception);
        assertThat(exception.validationErrors().size(), equalTo(1));
        assertThat(exception.validationErrors().get(0), containsString("Authentication request query string must be provided"));
    }
}
