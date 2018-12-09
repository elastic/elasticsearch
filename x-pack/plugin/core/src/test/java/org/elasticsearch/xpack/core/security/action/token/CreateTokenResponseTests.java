/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

public class CreateTokenResponseTests extends ESTestCase {

    public void testSerialization() throws Exception {
        CreateTokenResponse response = new CreateTokenResponse(randomAlphaOfLengthBetween(1, 10), TimeValue.timeValueMinutes(20L),
            randomBoolean() ? null : "FULL", randomAlphaOfLengthBetween(1, 10));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                CreateTokenResponse serialized = new CreateTokenResponse();
                serialized.readFrom(input);
                assertEquals(response, serialized);
            }
        }

        response = new CreateTokenResponse(randomAlphaOfLengthBetween(1, 10), TimeValue.timeValueMinutes(20L),
            randomBoolean() ? null : "FULL", null);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                CreateTokenResponse serialized = new CreateTokenResponse();
                serialized.readFrom(input);
                assertEquals(response, serialized);
            }
        }
    }

    public void testSerializationToPre62Version() throws Exception {
        CreateTokenResponse response = new CreateTokenResponse(randomAlphaOfLengthBetween(1, 10), TimeValue.timeValueMinutes(20L),
            randomBoolean() ? null : "FULL", randomBoolean() ? null : randomAlphaOfLengthBetween(1, 10));
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.V_6_1_4);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(version);
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                input.setVersion(version);
                CreateTokenResponse serialized = new CreateTokenResponse();
                serialized.readFrom(input);
                assertNull(serialized.getRefreshToken());
                assertEquals(response.getTokenString(), serialized.getTokenString());
                assertEquals(response.getExpiresIn(), serialized.getExpiresIn());
                assertEquals(response.getScope(), serialized.getScope());
            }
        }
    }

    public void testSerializationToPost62Pre65Version() throws Exception {
        CreateTokenResponse response = new CreateTokenResponse(randomAlphaOfLengthBetween(1, 10), TimeValue.timeValueMinutes(20L),
            randomBoolean() ? null : "FULL", randomAlphaOfLengthBetween(1, 10));
        final Version version = VersionUtils.randomVersionBetween(random(), Version.V_6_2_0, Version.V_6_4_0);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(version);
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                input.setVersion(version);
                CreateTokenResponse serialized = new CreateTokenResponse();
                serialized.readFrom(input);
                assertEquals(response, serialized);
            }
        }

        // no refresh token
        response = new CreateTokenResponse(randomAlphaOfLengthBetween(1, 10), TimeValue.timeValueMinutes(20L),
            randomBoolean() ? null : "FULL", null);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(version);
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                input.setVersion(version);
                CreateTokenResponse serialized = new CreateTokenResponse();
                serialized.readFrom(input);
                assertEquals("", serialized.getRefreshToken());
                assertEquals(response.getTokenString(), serialized.getTokenString());
                assertEquals(response.getExpiresIn(), serialized.getExpiresIn());
                assertEquals(response.getScope(), serialized.getScope());
            }
        }
    }
}
