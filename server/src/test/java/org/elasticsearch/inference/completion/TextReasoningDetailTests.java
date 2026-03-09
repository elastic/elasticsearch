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
import org.elasticsearch.core.Strings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractBWCSerializationTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matchers;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class TextReasoningDetailTests extends AbstractBWCSerializationTestCase<ReasoningDetail.TextReasoningDetail> {

    private static final String FORMAT_VALUE = "some encrypted reasoning detail format";
    private static final String ID_VALUE = "some id 0";
    private static final long INDEX_VALUE = 0L;
    private static final String DATA_VALUE = "some encrypted data";
    private static final String SUMMARY_VALUE = "some summary";
    private static final String TEXT_VALUE = "some text";
    private static final String SIGNATURE_VALUE = "some signature";

    public void testParsingRequestTextReasoningDetail_AllFields() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.text",
                    "format": "%s",
                    "id": "%s",
                    "index": %d,
                    "text": "%s",
                    "signature": "%s"
                }
                """, FORMAT_VALUE, ID_VALUE, INDEX_VALUE, TEXT_VALUE, SIGNATURE_VALUE),
            ReasoningDetail.REQUEST_PARSER,
            new ReasoningDetail.TextReasoningDetail(FORMAT_VALUE, ID_VALUE, INDEX_VALUE, TEXT_VALUE, SIGNATURE_VALUE)
        );
    }

    public void testParsingResponseTextReasoningDetail_AllFields() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.text",
                    "format": "%s",
                    "id": "%s",
                    "index": %d,
                    "text": "%s",
                    "signature": "%s"
                }
                """, FORMAT_VALUE, ID_VALUE, INDEX_VALUE, TEXT_VALUE, SIGNATURE_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.TextReasoningDetail(FORMAT_VALUE, ID_VALUE, INDEX_VALUE, TEXT_VALUE, SIGNATURE_VALUE)
        );
    }

    public void testParsingRequestTextReasoningDetail_OnlyText() throws IOException {
        testSuccessfulParsing(Strings.format("""
            {
                "type": "reasoning.text",
                "text": "%s"
            }
            """, TEXT_VALUE), ReasoningDetail.REQUEST_PARSER, new ReasoningDetail.TextReasoningDetail(null, null, null, TEXT_VALUE, null));
    }

    public void testParsingRequestTextReasoningDetail_OnlySignature() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.text",
                    "signature": "%s"
                }
                """, SIGNATURE_VALUE),
            ReasoningDetail.REQUEST_PARSER,
            new ReasoningDetail.TextReasoningDetail(null, null, null, null, SIGNATURE_VALUE)
        );
    }

    public void testParsingRequestTextReasoningDetail_NoTextNoSignature_ThrowsException() throws IOException {
        testFailedParsing("""
            {
                "type": "reasoning.text"
            }
            """, "At least one of [text, signature] must be provided for reasoning details of type [reasoning.text]");
    }

    public void testParsingResponseTextReasoningDetail_NoTextNoSignature_Ignored() throws IOException {
        testSuccessfulParsing("""
            {
                "type": "reasoning.text"
            }
            """, ReasoningDetail.RESPONSE_PARSER, new ReasoningDetail.TextReasoningDetail(null, null, null, null, null));
    }

    public void testParsingRequestTextReasoningDetail_DataFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.text",
                    "data": "%s",
                    "text": "%s"
                }
                """, DATA_VALUE, TEXT_VALUE),
            Strings.format("Field [data] is not expected for reasoning details of type [reasoning.text], but found [%s]", DATA_VALUE)
        );
    }

    public void testParsingResponseTextReasoningDetail_DataFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.text",
                    "data": "%s",
                    "text": "%s"
                }
                """, DATA_VALUE, TEXT_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.TextReasoningDetail(null, null, null, TEXT_VALUE, null)
        );
    }

    public void testParsingRequestTextReasoningDetail_SummaryFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.text",
                    "text": "%s",
                    "summary": "%s"
                }
                """, TEXT_VALUE, SUMMARY_VALUE),
            Strings.format("Field [summary] is not expected for reasoning details of type [reasoning.text], but found [%s]", SUMMARY_VALUE)
        );
    }

    public void testParsingResponseTextReasoningDetail_SummaryFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.text",
                    "text": "%s",
                    "summary": "%s"
                }
                """, TEXT_VALUE, SUMMARY_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.TextReasoningDetail(null, null, null, TEXT_VALUE, null)
        );
    }

    private void testSuccessfulParsing(
        String reasoningDetailJson,
        ConstructingObjectParser<ReasoningDetail, Void> responseParser,
        ReasoningDetail.TextReasoningDetail expectedReasoningDetail
    ) throws IOException {
        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = responseParser.apply(parser, null);

            assertThat(reasoningDetail, Matchers.is(expectedReasoningDetail));
        }
    }

    private void testFailedParsing(String reasoningDetailJson, String expectedExceptionMessage) throws IOException {
        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(XContentParseException.class, () -> ReasoningDetail.REQUEST_PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is(expectedExceptionMessage));
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
        return (ReasoningDetail.TextReasoningDetail) ReasoningDetail.REQUEST_PARSER.apply(parser, null);
    }
}
