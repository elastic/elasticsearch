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

public class SamlInitiateSingleSignOnRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final SamlInitiateSingleSignOnRequest request = new SamlInitiateSingleSignOnRequest();
        request.setSpEntityId("https://kibana_url");
        final BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);

        final SamlInitiateSingleSignOnRequest request1 = new SamlInitiateSingleSignOnRequest(out.bytes().streamInput());
        assertThat(request1.getSpEntityId(), equalTo(request.getSpEntityId()));
        final ActionRequestValidationException validationException = request1.validate();
        assertNull(validationException);
    }

    public void testValidation() {

        final SamlInitiateSingleSignOnRequest request1 = new SamlInitiateSingleSignOnRequest();
        final ActionRequestValidationException validationException = request1.validate();
        assertNotNull(validationException);
        assertThat(validationException.validationErrors().size(), equalTo(1));
        assertThat(validationException.validationErrors().get(0), containsString("sp_entity_id is missing"));
    }
}
