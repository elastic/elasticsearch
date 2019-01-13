/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.security.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authc.support.ApiKeysInvalidationResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InvalidateApiKeyResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        ApiKeysInvalidationResult result = new ApiKeysInvalidationResult(Arrays.asList("api-key-id-1"),
                Arrays.asList("api-key-id-2", "api-key-id-3"),
                Arrays.asList(new ElasticsearchException("error1"),
                        new ElasticsearchException("error2")),
                randomIntBetween(0, 5));
        InvalidateApiKeyResponse response = new InvalidateApiKeyResponse(result);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                InvalidateApiKeyResponse serialized = new InvalidateApiKeyResponse();
                serialized.readFrom(input);
                assertThat(serialized.getResult().getInvalidatedApiKeys(), equalTo(response.getResult().getInvalidatedApiKeys()));
                assertThat(serialized.getResult().getPreviouslyInvalidatedApiKeys(),
                    equalTo(response.getResult().getPreviouslyInvalidatedApiKeys()));
                assertThat(serialized.getResult().getErrors().size(), equalTo(response.getResult().getErrors().size()));
                assertThat(serialized.getResult().getErrors().get(0).toString(), containsString("error1"));
                assertThat(serialized.getResult().getErrors().get(1).toString(), containsString("error2"));
            }
        }

        result = new ApiKeysInvalidationResult(Arrays.asList(generateRandomStringArray(20, 15, false)),
            Arrays.asList(generateRandomStringArray(20, 15, false)),
            Collections.emptyList(), randomIntBetween(0, 5));
        response = new InvalidateApiKeyResponse(result);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                InvalidateApiKeyResponse serialized = new InvalidateApiKeyResponse();
                serialized.readFrom(input);
                assertThat(serialized.getResult().getInvalidatedApiKeys(), equalTo(response.getResult().getInvalidatedApiKeys()));
                assertThat(serialized.getResult().getPreviouslyInvalidatedApiKeys(),
                    equalTo(response.getResult().getPreviouslyInvalidatedApiKeys()));
                assertThat(serialized.getResult().getErrors().size(), equalTo(response.getResult().getErrors().size()));
            }
        }
    }

    public void testToXContent() throws IOException {
        ApiKeysInvalidationResult result = new ApiKeysInvalidationResult(Arrays.asList("api-key-id-1"),
                Arrays.asList("api-key-id-2", "api-key-id-3"),
                Arrays.asList(new ElasticsearchException("error1", new IllegalArgumentException("msg - 1")),
                        new ElasticsearchException("error2", new IllegalArgumentException("msg - 2"))),
                randomIntBetween(0, 5));
        InvalidateApiKeyResponse response = new InvalidateApiKeyResponse(result);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder),
            equalTo("{" +
                "\"invalidated_api_keys\":[\"api-key-id-1\"]," +
                "\"previously_invalidated_api_keys\":[\"api-key-id-2\",\"api-key-id-3\"]," +
                "\"error_count\":2," +
                "\"error_details\":[" +
                "{\"type\":\"exception\"," +
                "\"reason\":\"error1\"," +
                "\"caused_by\":{" +
                "\"type\":\"illegal_argument_exception\"," +
                "\"reason\":\"msg - 1\"}" +
                "}," +
                "{\"type\":\"exception\"," +
                "\"reason\":\"error2\"," +
                "\"caused_by\":" +
                "{\"type\":\"illegal_argument_exception\"," +
                "\"reason\":\"msg - 2\"}" +
                "}" +
                "]" +
                "}"));
    }
}
