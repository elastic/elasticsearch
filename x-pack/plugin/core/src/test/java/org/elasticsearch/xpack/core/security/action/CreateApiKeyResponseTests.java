/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

public class CreateApiKeyResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final String name = randomAlphaOfLengthBetween(1, 256);
        final SecureString key = new SecureString(UUIDs.randomBase64UUID().toCharArray());
        final Instant expiration = randomBoolean() ? Instant.now().plus(7L, ChronoUnit.DAYS) : null;

        final CreateApiKeyResponse response = new CreateApiKeyResponse(name, key, expiration);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                CreateApiKeyResponse serialized = new CreateApiKeyResponse();
                serialized.readFrom(in);
                assertEquals(name, serialized.getName());
                assertEquals(key, serialized.getKey());
                assertEquals(expiration, serialized.getExpiration());
            }

            try (StreamInput in = out.bytes().streamInput()) {
                CreateApiKeyResponse serialized = new CreateApiKeyResponse(in);
                assertEquals(name, serialized.getName());
                assertEquals(key, serialized.getKey());
                assertEquals(expiration, serialized.getExpiration());
            }
        }
    }
}
