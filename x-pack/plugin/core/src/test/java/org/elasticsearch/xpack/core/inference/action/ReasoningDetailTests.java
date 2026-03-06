/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class ReasoningDetailTests extends ESTestCase {

    public void testParsingReasoningDetail_NoType_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "format": "some encrypted reasoning detail format",
                "id": "some id 0",
                "index": 0,
                "data": "some encrypted data"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> ReasoningDetail.PARSER.apply(parser, null));
            assertThat(exception.getMessage(), is("Required [type]"));
        }
    }

    public void testParsingReasoningDetail_UnsupportedTypeValue_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "unknown"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> ReasoningDetail.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(
                rootCause.getMessage(),
                is(
                    """
                        Unrecognized type [unknown] in object [reasoning_detail_type], \
                        must be one of [reasoning.encrypted, reasoning.summary, reasoning.text]"""
                )
            );
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testParsingReasoningDetail_NegativeIndex_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "index": -1
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> ReasoningDetail.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Field [index] must be non-negative, but was [-1]"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public static ReasoningDetail randomReasoningDetail() {
        var type = randomFrom(ReasoningDetail.ReasoningDetailType.values());
        return switch (type) {
            case ENCRYPTED -> EncryptedReasoningDetailTests.randomEncryptedReasoningDetail();
            case SUMMARY -> SummaryReasoningDetailTests.randomSummaryReasoningDetail();
            case TEXT -> TextReasoningDetailTests.randomTextReasoningDetail();
        };
    }
}
