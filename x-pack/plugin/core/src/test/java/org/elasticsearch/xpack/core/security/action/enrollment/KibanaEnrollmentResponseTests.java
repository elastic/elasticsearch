/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.enrollment;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class KibanaEnrollmentResponseTests extends AbstractXContentTestCase<KibanaEnrollmentResponse> {

    @Override protected KibanaEnrollmentResponse createTestInstance() {
        return new KibanaEnrollmentResponse(
            new SecureString(randomAlphaOfLength(14).toCharArray()),
            randomAlphaOfLength(50));
    }

    @Override protected KibanaEnrollmentResponse doParseInstance(XContentParser parser) throws IOException {
        return KibanaEnrollmentResponse.fromXContent(parser);
    }

    @Override protected boolean supportsUnknownFields() {
        return false;
    }

    public void testSerialization() throws IOException{
        KibanaEnrollmentResponse response = createTestInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                KibanaEnrollmentResponse serialized = new KibanaEnrollmentResponse(in);
                assertThat(response.getHttpCa(), is(serialized.getHttpCa()));
                assertThat(response.getPassword(), is(serialized.getPassword()));
            }
        }
    }
}
