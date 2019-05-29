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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InvalidateApiKeyResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        InvalidateApiKeyResponse response = new InvalidateApiKeyResponse(Arrays.asList("api-key-id-1"),
                Arrays.asList("api-key-id-2", "api-key-id-3"),
                Arrays.asList(new ElasticsearchException("error1"),
                        new ElasticsearchException("error2")));
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                InvalidateApiKeyResponse serialized = new InvalidateApiKeyResponse(input);
                assertThat(serialized.getInvalidatedApiKeys(), equalTo(response.getInvalidatedApiKeys()));
                assertThat(serialized.getPreviouslyInvalidatedApiKeys(),
                    equalTo(response.getPreviouslyInvalidatedApiKeys()));
                assertThat(serialized.getErrors().size(), equalTo(response.getErrors().size()));
                assertThat(serialized.getErrors().get(0).toString(), containsString("error1"));
                assertThat(serialized.getErrors().get(1).toString(), containsString("error2"));
            }
        }

        response = new InvalidateApiKeyResponse(Arrays.asList(generateRandomStringArray(20, 15, false)),
            Arrays.asList(generateRandomStringArray(20, 15, false)),
            Collections.emptyList());
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                InvalidateApiKeyResponse serialized = new InvalidateApiKeyResponse(input);
                assertThat(serialized.getInvalidatedApiKeys(), equalTo(response.getInvalidatedApiKeys()));
                assertThat(serialized.getPreviouslyInvalidatedApiKeys(),
                    equalTo(response.getPreviouslyInvalidatedApiKeys()));
                assertThat(serialized.getErrors().size(), equalTo(response.getErrors().size()));
            }
        }
    }

    public void testToXContent() throws IOException {
        InvalidateApiKeyResponse response = new InvalidateApiKeyResponse(Arrays.asList("api-key-id-1"),
                Arrays.asList("api-key-id-2", "api-key-id-3"),
                Arrays.asList(new ElasticsearchException("error1", new IllegalArgumentException("msg - 1")),
                        new ElasticsearchException("error2", new IllegalArgumentException("msg - 2"))));
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
