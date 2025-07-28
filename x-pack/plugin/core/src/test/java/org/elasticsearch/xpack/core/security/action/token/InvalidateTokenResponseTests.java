/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.action.token;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authc.support.TokensInvalidationResult;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class InvalidateTokenResponseTests extends ESTestCase {

    public void testSerialization() throws IOException {
        TokensInvalidationResult result = new TokensInvalidationResult(
            Arrays.asList(generateRandomStringArray(20, 15, false)),
            Arrays.asList(generateRandomStringArray(20, 15, false)),
            Arrays.asList(
                new ElasticsearchException("foo", new IllegalArgumentException("this is an error message")),
                new ElasticsearchException("bar", new IllegalArgumentException("this is an error message2"))
            ),
            RestStatus.OK
        );
        InvalidateTokenResponse response = new InvalidateTokenResponse(result);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                InvalidateTokenResponse serialized = new InvalidateTokenResponse(input);
                assertThat(serialized.getResult().getInvalidatedTokens(), equalTo(response.getResult().getInvalidatedTokens()));
                assertThat(
                    serialized.getResult().getPreviouslyInvalidatedTokens(),
                    equalTo(response.getResult().getPreviouslyInvalidatedTokens())
                );
                assertThat(serialized.getResult().getErrors().size(), equalTo(response.getResult().getErrors().size()));
                assertThat(serialized.getResult().getErrors().get(0).getCause().getMessage(), containsString("this is an error message"));
                assertThat(serialized.getResult().getErrors().get(1).getCause().getMessage(), containsString("this is an error message2"));
            }
        }

        result = new TokensInvalidationResult(
            Arrays.asList(generateRandomStringArray(20, 15, false)),
            Arrays.asList(generateRandomStringArray(20, 15, false)),
            Collections.emptyList(),
            RestStatus.OK
        );
        response = new InvalidateTokenResponse(result);
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            response.writeTo(output);
            try (StreamInput input = output.bytes().streamInput()) {
                InvalidateTokenResponse serialized = new InvalidateTokenResponse(input);
                assertThat(serialized.getResult().getInvalidatedTokens(), equalTo(response.getResult().getInvalidatedTokens()));
                assertThat(
                    serialized.getResult().getPreviouslyInvalidatedTokens(),
                    equalTo(response.getResult().getPreviouslyInvalidatedTokens())
                );
                assertThat(serialized.getResult().getErrors().size(), equalTo(response.getResult().getErrors().size()));
            }
        }
    }

    public void testToXContent() throws IOException {
        List<String> invalidatedTokens = Arrays.asList(generateRandomStringArray(20, 15, false));
        List<String> previouslyInvalidatedTokens = Arrays.asList(generateRandomStringArray(20, 15, false));
        TokensInvalidationResult result = new TokensInvalidationResult(
            invalidatedTokens,
            previouslyInvalidatedTokens,
            Arrays.asList(
                new ElasticsearchException("foo", new IllegalArgumentException("this is an error message")),
                new ElasticsearchException("bar", new IllegalArgumentException("this is an error message2"))
            ),
            RestStatus.OK
        );
        InvalidateTokenResponse response = new InvalidateTokenResponse(result);
        XContentBuilder builder = XContentFactory.jsonBuilder();
        response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertThat(Strings.toString(builder), equalTo(XContentHelper.stripWhitespace(Strings.format("""
            {
              "invalidated_tokens": %s,
              "previously_invalidated_tokens": %s,
              "error_count": 2,
              "error_details": [
                {
                  "type": "exception",
                  "reason": "foo",
                  "caused_by": {
                    "type": "illegal_argument_exception",
                    "reason": "this is an error message"
                  }
                },
                {
                  "type": "exception",
                  "reason": "bar",
                  "caused_by": {
                    "type": "illegal_argument_exception",
                    "reason": "this is an error message2"
                  }
                }
              ]
            }
            """, invalidatedTokens.size(), previouslyInvalidatedTokens.size()))));
    }
}
