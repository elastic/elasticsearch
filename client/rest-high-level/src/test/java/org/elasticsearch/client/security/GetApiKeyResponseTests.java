/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.security;

import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class GetApiKeyResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {
        ApiKey apiKeyInfo1 = createApiKeyInfo("name1", "id-1", Instant.ofEpochMilli(100000L), Instant.ofEpochMilli(10000000L), false,
                "user-a", "realm-x");
        ApiKey apiKeyInfo2 = createApiKeyInfo("name2", "id-2", Instant.ofEpochMilli(100000L), Instant.ofEpochMilli(10000000L), true,
                "user-b", "realm-y");
        ApiKey apiKeyInfo3 = createApiKeyInfo(null, "id-3", Instant.ofEpochMilli(100000L), null, true,
            "user-c", "realm-z");
        GetApiKeyResponse response = new GetApiKeyResponse(Arrays.asList(apiKeyInfo1, apiKeyInfo2, apiKeyInfo3));
        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        toXContent(response, builder);
        BytesReference xContent = BytesReference.bytes(builder);
        GetApiKeyResponse responseParsed = GetApiKeyResponse.fromXContent(createParser(xContentType.xContent(), xContent));
        assertThat(responseParsed, equalTo(response));
    }

    private void toXContent(GetApiKeyResponse response, final XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startArray("api_keys");
        for (ApiKey apiKey : response.getApiKeyInfos()) {
        builder.startObject()
        .field("id", apiKey.getId())
        .field("name", apiKey.getName())
        .field("creation", apiKey.getCreation().toEpochMilli());
        if (apiKey.getExpiration() != null) {
            builder.field("expiration", apiKey.getExpiration().toEpochMilli());
        }
        builder.field("invalidated", apiKey.isInvalidated())
        .field("username", apiKey.getUsername())
        .field("realm", apiKey.getRealm());
        builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    public void testEqualsHashCode() {
        ApiKey apiKeyInfo1 = createApiKeyInfo("name1", "id-1", Instant.ofEpochMilli(100000L), Instant.ofEpochMilli(10000000L), false,
                "user-a", "realm-x");
        GetApiKeyResponse response = new GetApiKeyResponse(Arrays.asList(apiKeyInfo1));

        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, (original) -> {
            return new GetApiKeyResponse(original.getApiKeyInfos());
        });
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(response, (original) -> {
            return new GetApiKeyResponse(original.getApiKeyInfos());
        }, GetApiKeyResponseTests::mutateTestItem);
    }

    private static GetApiKeyResponse mutateTestItem(GetApiKeyResponse original) {
        ApiKey apiKeyInfo = createApiKeyInfo("name2", "id-2", Instant.ofEpochMilli(100000L), Instant.ofEpochMilli(10000000L), true,
                "user-b", "realm-y");
        switch (randomIntBetween(0, 2)) {
        case 0:
            return new GetApiKeyResponse(Arrays.asList(apiKeyInfo));
        default:
            return new GetApiKeyResponse(Arrays.asList(apiKeyInfo));
        }
    }

    private static ApiKey createApiKeyInfo(String name, String id, Instant creation, Instant expiration, boolean invalidated,
                                           String username, String realm) {
        return new ApiKey(name, id, creation, expiration, invalidated, username, realm, null);
    }
}
