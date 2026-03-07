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
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class TextReasoningDetailTests extends AbstractBWCSerializationTestCase<ReasoningDetail.TextReasoningDetail> {

    public void testParsingTextReasoningDetails_AllFields() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.text",
                "format": "some text reasoning detail format",
                "id": "some id 2",
                "index": 2,
                "text": "some text",
                "signature": "some signature"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.TextReasoningDetail(
                "some text reasoning detail format",
                "some id 2",
                2L,
                "some text",
                "some signature"
            );

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingTextReasoningDetail_OnlyTypeAndText() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.text",
                "text": "some text"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.TextReasoningDetail(null, null, null, "some text", null);

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingTextReasoningDetail_UnknownField() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.text",
                "text": "some text",
                "unknown_field": "some value"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.TextReasoningDetail(null, null, null, "some text", null);

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingTextReasoningDetail_OnlyTypeAndSignature() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.text",
                "signature": "some signature"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.TextReasoningDetail(null, null, null, null, "some signature");

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingTextReasoningDetail_OnlyType_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.text"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(XContentParseException.class, () -> ReasoningDetail.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(
                rootCause.getMessage(),
                is("At least one of [text, signature] must be provided for reasoning details of type [reasoning.text]")
            );
            assertThat(rootCause.status(), is(RestStatus.BAD_REQUEST));
        }
    }

    @Override
    protected ReasoningDetail.TextReasoningDetail mutateInstanceForVersion(
        ReasoningDetail.TextReasoningDetail instance,
        TransportVersion version
    ) {
        // checks for version compatibility are done outside tested class, so we can return the instance as is without mutation
        return instance;
    }

    @Override
    protected Writeable.Reader<ReasoningDetail.TextReasoningDetail> instanceReader() {
        return ReasoningDetail.TextReasoningDetail::new;
    }

    @Override
    protected ReasoningDetail.TextReasoningDetail createTestInstance() {
        return randomTextReasoningDetail();
    }

    static ReasoningDetail.TextReasoningDetail randomTextReasoningDetail() {
        var text = randomAlphaOfLengthOrNull(10);
        var signature = text == null ? randomAlphaOfLength(10) : randomAlphaOfLengthOrNull(10);
        return new ReasoningDetail.TextReasoningDetail(
            randomAlphaOfLengthOrNull(10),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            text,
            signature
        );
    }

    @Override
    protected ReasoningDetail.TextReasoningDetail mutateInstance(ReasoningDetail.TextReasoningDetail instance) throws IOException {
        var format = instance.format();
        var id = instance.id();
        var index = instance.index();
        var text = instance.text();
        var signature = instance.signature();

        switch (between(0, 4)) {
            case 0 -> format = randomValueOtherThan(format, () -> randomAlphaOfLengthOrNull(10));
            case 1 -> id = randomValueOtherThan(id, () -> randomAlphaOfLengthOrNull(10));
            case 2 -> index = randomValueOtherThan(index, ESTestCase::randomNonNegativeLongOrNull);
            case 3 -> {
                // Only mutate to non-null if signature is null
                if (signature == null) {
                    text = randomValueOtherThan(text, () -> randomAlphaOfLength(10));
                } else {
                    text = randomValueOtherThan(text, () -> randomAlphaOfLengthOrNull(10));
                }
            }
            case 4 -> {
                // Only mutate to non-null if text is null
                if (text == null) {
                    signature = randomValueOtherThan(signature, () -> randomAlphaOfLength(10));
                } else {
                    signature = randomValueOtherThan(signature, () -> randomAlphaOfLengthOrNull(10));
                }
            }
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new ReasoningDetail.TextReasoningDetail(format, id, index, text, signature);
    }

    @Override
    protected ReasoningDetail.TextReasoningDetail doParseInstance(XContentParser parser) throws IOException {
        return (ReasoningDetail.TextReasoningDetail) ReasoningDetail.PARSER.apply(parser, null);
    }
}
