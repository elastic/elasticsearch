/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ClientEnrollmentRequestTests extends ESTestCase {

    public void testValidation() {
        expectThrows(NullPointerException.class, () -> new ClientEnrollmentRequest(null, null));

        assertNull(new ClientEnrollmentRequest("kibana", null).validate());
        assertNull(new ClientEnrollmentRequest("generic_client", null).validate());
        assertNull(new ClientEnrollmentRequest("kibana", new SecureString(randomAlphaOfLengthBetween(8, 19).toCharArray())).validate());

        ClientEnrollmentRequest r =
            new ClientEnrollmentRequest("generic_client", new SecureString(randomAlphaOfLengthBetween(8, 19).toCharArray()));
        ActionRequestValidationException e = r.validate();
        assertNotNull(e);
        assertThat(e.getMessage(), containsString("client_password cannot be set when client_type is generic_client"));
    }

    public void testSerialization() throws IOException {
        ClientEnrollmentRequest request = new ClientEnrollmentRequest(
            "kibana", randomBoolean() ? null : new SecureString(randomAlphaOfLengthBetween(8, 19).toCharArray()));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ClientEnrollmentRequest serialized = new ClientEnrollmentRequest(in);
                assertThat(request.getClientType(), equalTo(serialized.getClientType()));
                assertThat(request.getClientPassword(), equalTo(serialized.getClientPassword()));
            }
        }
    }
}
