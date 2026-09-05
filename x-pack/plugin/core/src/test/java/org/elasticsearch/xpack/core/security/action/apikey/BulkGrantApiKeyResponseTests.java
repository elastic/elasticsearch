/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.apikey;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class BulkGrantApiKeyResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        final CreateApiKeyResponse created = new CreateApiKeyResponse(
            "key-1",
            "id-1",
            new SecureString("secret".toCharArray()),
            Instant.ofEpochMilli(1_700_000_000_000L)
        );
        final var response = new BulkGrantApiKeyResponse(
            List.of(created),
            Map.of("id-2", new IllegalArgumentException("bad role descriptors"))
        );
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                final var serialized = new BulkGrantApiKeyResponse(input);
                assertThat(serialized.getCreated().size(), equalTo(1));
                assertThat(serialized.getCreated().get(0).getId(), equalTo("id-1"));
                assertThat(serialized.getCreated().get(0).getName(), equalTo("key-1"));
                assertThat(serialized.getCreated().get(0).getKey().toString(), equalTo("secret"));
                assertThat(serialized.getErrorDetails().size(), equalTo(1));
                assertThat(serialized.getErrorDetails().get("id-2").toString(), containsString("bad role descriptors"));
            }
        }
    }

    public void testToXContent() throws IOException {
        final CreateApiKeyResponse created = new CreateApiKeyResponse(
            "key-1",
            "id-1",
            new SecureString("secret".toCharArray()),
            Instant.ofEpochMilli(1_700_000_000_000L)
        );
        final SortedMap<String, Exception> errorDetails = new TreeMap<>();
        errorDetails.put("id-2", new ElasticsearchException("failed to create"));
        final var response = new BulkGrantApiKeyResponse(List.of(created), errorDetails);
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        final String json = Strings.toString(builder);
        assertThat(json, containsString("\"created\""));
        assertThat(json, containsString("\"id\":\"id-1\""));
        assertThat(json, containsString("\"name\":\"key-1\""));
        assertThat(json, containsString("\"api_key\":\"secret\""));
        assertThat(json, containsString("\"errors\""));
        assertThat(json, containsString("\"count\":1"));
        assertThat(json, containsString("\"id-2\""));
    }

    public void testToXContentOmitsErrorsSectionIfNoErrors() throws IOException {
        final CreateApiKeyResponse created = new CreateApiKeyResponse("key-1", "id-1", new SecureString("secret".toCharArray()), null);
        final var response = new BulkGrantApiKeyResponse(List.of(created), Map.of());
        final XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(XContentHelper.stripWhitespace("""
            {
              "created": [
                {
                  "id": "id-1",
                  "name": "key-1",
                  "api_key": "secret",
                  "encoded": "aWQtMTpzZWNyZXQ="
                }
              ]
            }""")));
    }
}
