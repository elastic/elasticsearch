/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.security;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class InvalidateTokenResponseTests extends ESTestCase {

    public void testFromXContent() throws IOException {

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        final int invalidatedTokens = randomInt(32);
        final int previouslyInvalidatedTokens = randomInt(32);
        builder.startObject()
            .field("invalidated_tokens", invalidatedTokens)
            .field("previously_invalidated_tokens", previouslyInvalidatedTokens)
            .field("error_count", 0)
            .endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        try (XContentParser parser = createParser(xContentType.xContent(), xContent)) {
            final InvalidateTokenResponse response = InvalidateTokenResponse.fromXContent(parser);
            assertThat(response.getInvalidatedTokens(), Matchers.equalTo(invalidatedTokens));
            assertThat(response.getPreviouslyInvalidatedTokens(), Matchers.equalTo(previouslyInvalidatedTokens));
            assertThat(response.getErrorsCount(), Matchers.equalTo(0));
        }
    }

    public void testFromXContentWithErrors() throws IOException {

        final XContentType xContentType = randomFrom(XContentType.values());
        final XContentBuilder builder = XContentFactory.contentBuilder(xContentType);
        final int invalidatedTokens = randomInt(32);
        final int previouslyInvalidatedTokens = randomInt(32);
        builder.startObject()
            .field("invalidated_tokens", invalidatedTokens)
            .field("previously_invalidated_tokens", previouslyInvalidatedTokens)
            .field("error_count", 0)
            .startArray("error_details")
            .startObject();
        ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, new ElasticsearchException("foo",
            new IllegalArgumentException("bar")));
        builder.endObject().startObject();
        ElasticsearchException.generateThrowableXContent(builder, ToXContent.EMPTY_PARAMS, new ElasticsearchException("boo",
            new IllegalArgumentException("far")));
        builder.endObject()
            .endArray()
            .endObject();
        BytesReference xContent = BytesReference.bytes(builder);

        try (XContentParser parser = createParser(xContentType.xContent(), xContent)) {
            final InvalidateTokenResponse response = InvalidateTokenResponse.fromXContent(parser);
            assertThat(response.getInvalidatedTokens(), Matchers.equalTo(invalidatedTokens));
            assertThat(response.getPreviouslyInvalidatedTokens(), Matchers.equalTo(previouslyInvalidatedTokens));
            assertThat(response.getErrorsCount(), Matchers.equalTo(2));
            assertThat(response.getErrors().get(0).toString(), containsString("type=exception, reason=foo"));
            assertThat(response.getErrors().get(0).getCause().getMessage(), containsString("type=illegal_argument_exception, reason=bar"));
            assertThat(response.getErrors().get(1).toString(), containsString("type=exception, reason=boo"));
            assertThat(response.getErrors().get(1).getCause().getMessage(), containsString("type=illegal_argument_exception, reason=far"));
        }
    }
}
