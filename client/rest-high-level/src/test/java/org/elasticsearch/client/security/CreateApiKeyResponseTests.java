/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.core.CharArrays;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class CreateApiKeyResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        final String id = randomAlphaOfLengthBetween(4, 8);
        final String name = randomAlphaOfLength(5);
        final SecureString apiKey = UUIDs.randomBase64UUIDSecureString();
        final Instant expiration = randomBoolean() ? null : Instant.ofEpochMilli(10000);

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        builder.startObject().field("id", id).field("name", name);
        if (expiration != null) {
            builder.field("expiration", expiration.toEpochMilli());
        }
        byte[] charBytes = CharArrays.toUtf8Bytes(apiKey.getChars());
        try {
            builder.field("api_key").utf8Value(charBytes, 0, charBytes.length);
        } finally {
            Arrays.fill(charBytes, (byte) 0);
        }
        builder.endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        final CreateApiKeyResponse response = CreateApiKeyResponse.fromXContent(createParser(xContentType.xContent(), xContent));
        assertThat(response.getId(), equalTo(id));
        assertThat(response.getName(), equalTo(name));
        assertThat(response.getKey(), equalTo(apiKey));
        if (expiration != null) {
            assertThat(response.getExpiration(), equalTo(expiration));
        }
    }

    public void testEqualsHashCode() {
        final String id = randomAlphaOfLengthBetween(4, 8);
        final String name = randomAlphaOfLength(5);
        final SecureString apiKey = UUIDs.randomBase64UUIDSecureString();
        final Instant expiration = Instant.ofEpochMilli(10000);
        CreateApiKeyResponse createApiKeyResponse = new CreateApiKeyResponse(name, id, apiKey, expiration);

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyResponse, (original) -> {
            return new CreateApiKeyResponse(original.getName(), original.getId(), original.getKey(), original.getExpiration());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createApiKeyResponse, (original) -> {
            return new CreateApiKeyResponse(original.getName(), original.getId(), original.getKey(), original.getExpiration());
        }, CreateApiKeyResponseTests::mutateTestItem);
    }

    private static CreateApiKeyResponse mutateTestItem(CreateApiKeyResponse original) {
        switch (randomIntBetween(0, 3)) {
        case 0:
            return new CreateApiKeyResponse(randomAlphaOfLength(7), original.getId(), original.getKey(), original.getExpiration());
        case 1:
            return new CreateApiKeyResponse(original.getName(), randomAlphaOfLengthBetween(4, 8), original.getKey(),
                    original.getExpiration());
        case 2:
            return new CreateApiKeyResponse(original.getName(), original.getId(), UUIDs.randomBase64UUIDSecureString(),
                    original.getExpiration());
        case 3:
            return new CreateApiKeyResponse(original.getName(), original.getId(), original.getKey(), Instant.ofEpochMilli(150000));
        default:
            return new CreateApiKeyResponse(randomAlphaOfLength(7), original.getId(), original.getKey(), original.getExpiration());
        }
    }
}
