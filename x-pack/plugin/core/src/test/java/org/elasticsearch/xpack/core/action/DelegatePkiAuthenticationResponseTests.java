/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.action;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.xpack.core.security.action.DelegatePkiAuthenticationResponse;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class DelegatePkiAuthenticationResponseTests extends AbstractXContentTestCase<DelegatePkiAuthenticationResponse> {

    public void testSerialization() throws Exception {
        DelegatePkiAuthenticationResponse response = createTestInstance();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                DelegatePkiAuthenticationResponse serialized = new DelegatePkiAuthenticationResponse(input);
                assertThat(response.getAccessToken(), is(serialized.getAccessToken()));
                assertThat(response.getExpiresIn(), is(serialized.getExpiresIn()));
                assertThat(response, is(serialized));
            }
        }
    }

    @Override
    protected DelegatePkiAuthenticationResponse createTestInstance() {
        return new DelegatePkiAuthenticationResponse(randomAlphaOfLengthBetween(0, 10),
                TimeValue.parseTimeValue(randomTimeValue(), getClass().getSimpleName() + ".expiresIn"));
    }

    @Override
    protected DelegatePkiAuthenticationResponse doParseInstance(XContentParser parser) throws IOException {
        return DelegatePkiAuthenticationResponse.PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }
}
