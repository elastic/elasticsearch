/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class GetApiKeyResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        boolean withExpiration = randomBoolean();
        ApiKeyInfo apiKeyInfo = createApiKeyInfo(randomAlphaOfLength(4), randomAlphaOfLength(5), Instant.now(),
                (withExpiration) ? Instant.now() : null, false, randomAlphaOfLength(4), randomAlphaOfLength(5));
        GetApiKeyResponse response = new GetApiKeyResponse(Collections.singletonList(apiKeyInfo));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                GetApiKeyResponse serialized = new GetApiKeyResponse(input);
                assertThat(serialized.getApiKeyInfos(), equalTo(response.getApiKeyInfos()));
            }
        }
    }

    public void testToXContent() throws IOException {
        ApiKeyInfo apiKeyInfo1 = createApiKeyInfo("name1", "id-1", Instant.ofEpochMilli(100000L), Instant.ofEpochMilli(10000000L), false,
                "user-a", "realm-x");
        ApiKeyInfo apiKeyInfo2 = createApiKeyInfo("name2", "id-2", Instant.ofEpochMilli(100000L), Instant.ofEpochMilli(10000000L), true,
                "user-b", "realm-y");
        GetApiKeyResponse response = new GetApiKeyResponse(Arrays.asList(apiKeyInfo1, apiKeyInfo2));
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(
                "{"
                + "\"api_key_infos\":["
                + "{\"id\":\"id-1\",\"name\":\"name1\",\"creation\":100000,\"invalidated\":false,"
                + "\"username\":\"user-a\",\"realm\":\"realm-x\",\"expiration\":10000000},"
                + "{\"id\":\"id-2\",\"name\":\"name2\",\"creation\":100000,\"invalidated\":true,"
                + "\"username\":\"user-b\",\"realm\":\"realm-y\",\"expiration\":10000000}"
                + "]"
                + "}"));
    }

    private ApiKeyInfo createApiKeyInfo(String name, String id, Instant creation, Instant expiration, boolean invalidated, String username,
                                        String realm) {
        return new ApiKeyInfo(name, id, creation, expiration, invalidated, username, realm);
    }
}

