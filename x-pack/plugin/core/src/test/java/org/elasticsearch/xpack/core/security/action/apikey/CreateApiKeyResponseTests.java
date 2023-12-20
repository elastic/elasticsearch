/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.hamcrest.Matchers.equalTo;

public class CreateApiKeyResponseTests extends AbstractXContentTestCase<CreateApiKeyResponse> {

    @Override
    protected CreateApiKeyResponse doParseInstance(XContentParser parser) throws IOException {
        return CreateApiKeyResponse.fromXContent(parser);
    }

    @Override
    protected CreateApiKeyResponse createTestInstance() {
        final String name = randomAlphaOfLengthBetween(1, 256);
        final SecureString key = new SecureString(UUIDs.randomBase64UUID().toCharArray());
        final Instant expiration = randomBoolean() ? Instant.now().plus(7L, ChronoUnit.DAYS) : null;
        final String id = randomAlphaOfLength(100);
        return new CreateApiKeyResponse(name, id, key, expiration);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testSerialization() throws IOException {
        final CreateApiKeyResponse response = createTestInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            response.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                CreateApiKeyResponse serialized = new CreateApiKeyResponse(in);
                assertThat(serialized, equalTo(response));
            }
        }
    }

    public void testEqualsHashCode() {
        CreateApiKeyResponse createApiKeyResponse = createTestInstance();

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyResponse, (original) -> {
            return new CreateApiKeyResponse(original.getName(), original.getId(), original.getKey(), original.getExpiration());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyResponse, (original) -> {
            return new CreateApiKeyResponse(original.getName(), original.getId(), original.getKey(), original.getExpiration());
        }, CreateApiKeyResponseTests::mutateTestItem);
    }

    private static CreateApiKeyResponse mutateTestItem(CreateApiKeyResponse original) {
        return switch (randomIntBetween(0, 3)) {
            case 0 -> new CreateApiKeyResponse(randomAlphaOfLength(5), original.getId(), original.getKey(), original.getExpiration());
            case 1 -> new CreateApiKeyResponse(original.getName(), randomAlphaOfLength(5), original.getKey(), original.getExpiration());
            case 2 -> new CreateApiKeyResponse(
                original.getName(),
                original.getId(),
                new SecureString(UUIDs.randomBase64UUID().toCharArray()),
                original.getExpiration()
            );
            case 3 -> new CreateApiKeyResponse(original.getName(), original.getId(), original.getKey(), Instant.now());
            default -> new CreateApiKeyResponse(randomAlphaOfLength(5), original.getId(), original.getKey(), original.getExpiration());
        };
    }
}
