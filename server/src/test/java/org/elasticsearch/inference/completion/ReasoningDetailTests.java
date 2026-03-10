/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.inference.completion;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class ReasoningDetailTests extends ESTestCase {

    private static final String DATA_VALUE = "some encrypted data";

    public void testParsingReasoningDetail_NoType_ThrowsException() throws IOException {
        String reasoningDetailJson = "{}";

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> ReasoningDetail.REQUEST_PARSER.apply(parser, null));
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
            var exception = assertThrows(IllegalArgumentException.class, () -> ReasoningDetail.REQUEST_PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("""
                Unrecognized type [unknown] in object [reasoning_detail_type], \
                must be one of [reasoning.encrypted, reasoning.summary, reasoning.text]"""));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testParsingRequestReasoningDetail_NegativeIndex_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "data": "some encrypted data",
                "index": -1
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(IllegalArgumentException.class, () -> ReasoningDetail.REQUEST_PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Field [index] must be non-negative, but was [-1]"));
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    public void testParsingResponseReasoningDetail_NegativeIndex_Ignored() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "data": "some encrypted data",
                "index": -1
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.RESPONSE_PARSER.apply(parser, null);
            var expected = new ReasoningDetail.EncryptedReasoningDetail(null, null, -1L, DATA_VALUE);

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingRequestReasoningDetail_UnknownField_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "data": "some encrypted data",
                "unknown_field": "some value"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(XContentParseException.class, () -> ReasoningDetail.REQUEST_PARSER.apply(parser, null));
            assertThat(exception.getMessage(), is("[4:5] [ReasoningDetail] unknown field [unknown_field]"));
        }
    }

    public void testParsingResponseReasoningDetail_UnknownField_Ignored() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "data": "some encrypted data",
                "unknown_field": "some value"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.RESPONSE_PARSER.apply(parser, null);
            var expected = new ReasoningDetail.EncryptedReasoningDetail(null, null, null, DATA_VALUE);

            assertThat(reasoningDetail, is(expected));
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
