/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class ClientEnrollmentResponseTests extends AbstractXContentTestCase<ClientEnrollmentResponse> {

    @Override protected ClientEnrollmentResponse createTestInstance() {
        return new ClientEnrollmentResponse(
            randomAlphaOfLength(50),
            randomList(10, () -> buildNewFakeTransportAddress().toString()));
    }

    @Override protected ClientEnrollmentResponse doParseInstance(XContentParser parser) throws IOException {
        return ClientEnrollmentResponse.fromXContent(parser);
    }

    @Override protected boolean supportsUnknownFields() {
        return false;
    }

    public void testSerialization() throws IOException{
        ClientEnrollmentResponse response = createTestInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                ClientEnrollmentResponse serialized = new ClientEnrollmentResponse(in);
                assertThat(response.getHttpCa(), is(serialized.getHttpCa()));
                assertThat(response.getNodesAddresses(), is(serialized.getNodesAddresses()));
            }
        }
    }
}
