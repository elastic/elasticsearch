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

public class SummaryReasoningDetailTests extends AbstractBWCSerializationTestCase<ReasoningDetail.SummaryReasoningDetail> {

    private static final String FORMAT_VALUE = "some encrypted reasoning detail format";
    private static final String ID_VALUE = "some id 0";
    private static final long INDEX_VALUE = 0L;
    private static final String DATA_VALUE = "some encrypted data";
    private static final String SUMMARY_VALUE = "some summary";
    private static final String TEXT_VALUE = "some text";
    private static final String SIGNATURE_VALUE = "some signature";

    public void testParsingRequestSummaryReasoningDetail_AllFields() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "format": "%s",
                    "id": "%s",
                    "index": %d,
                    "summary": "%s"
                }
                """, FORMAT_VALUE, ID_VALUE, INDEX_VALUE, SUMMARY_VALUE),
            ReasoningDetail.REQUEST_PARSER,
            new ReasoningDetail.SummaryReasoningDetail(FORMAT_VALUE, ID_VALUE, INDEX_VALUE, SUMMARY_VALUE)
        );
    }

    public void testParsingResponseSummaryReasoningDetail_AllFields() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "format": "%s",
                    "id": "%s",
                    "index": %d,
                    "summary": "%s"
                }
                """, FORMAT_VALUE, ID_VALUE, INDEX_VALUE, SUMMARY_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.SummaryReasoningDetail(FORMAT_VALUE, ID_VALUE, INDEX_VALUE, SUMMARY_VALUE)
        );
    }

    public void testParsingRequestSummaryReasoningDetail_OnlyRequiredFields() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "summary": "%s"
                }
                """, SUMMARY_VALUE),
            ReasoningDetail.REQUEST_PARSER,
            new ReasoningDetail.SummaryReasoningDetail(null, null, null, SUMMARY_VALUE)
        );
    }

    public void testParsingRequestSummaryReasoningDetail_NoSummary_ThrowsException() throws IOException {
        testFailedParsing("""
            {
                "type": "reasoning.summary"
            }
            """, "Required field [summary] is missing for reasoning details of type [reasoning.summary]");
    }

    public void testParsingResponseSummaryReasoningDetail_NoSummary_Ignored() throws IOException {
        testSuccessfulParsing("""
            {
                "type": "reasoning.summary"
            }
            """, ReasoningDetail.RESPONSE_PARSER, new ReasoningDetail.SummaryReasoningDetail(null, null, null, null));
    }

    public void testParsingRequestSummaryReasoningDetail_DataFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "summary": "%s",
                    "data": "%s"
                }
                """, SUMMARY_VALUE, DATA_VALUE),
            Strings.format("Field [data] is not expected for reasoning details of type [reasoning.summary], but found [%s]", DATA_VALUE)
        );
    }

    public void testParsingResponseSummaryReasoningDetail_DataFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "summary": "%s",
                    "data": "%s"
                }
                """, SUMMARY_VALUE, DATA_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.SummaryReasoningDetail(null, null, null, SUMMARY_VALUE)
        );
    }

    public void testParsingRequestSummaryReasoningDetail_TextFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "summary": "%s",
                    "text": "%s"
                }
                """, SUMMARY_VALUE, TEXT_VALUE),
            Strings.format("Field [text] is not expected for reasoning details of type [reasoning.summary], but found [%s]", TEXT_VALUE)
        );
    }

    public void testParsingResponseSummaryReasoningDetail_TextFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "summary": "%s",
                    "text": "%s"
                }
                """, SUMMARY_VALUE, TEXT_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.SummaryReasoningDetail(null, null, null, SUMMARY_VALUE)
        );
    }

    public void testParsingRequestSummaryReasoningDetail_SignatureFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "summary": "%s",
                    "signature": "%s"
                }
                """, SUMMARY_VALUE, SIGNATURE_VALUE),
            Strings.format(
                "Field [signature] is not expected for reasoning details of type [reasoning.summary], but found [%s]",
                SIGNATURE_VALUE
            )
        );
    }

    public void testParsingResponseSummaryReasoningDetail_SignatureFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.summary",
                    "summary": "%s",
                    "signature": "%s"
                }
                """, SUMMARY_VALUE, SIGNATURE_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.SummaryReasoningDetail(null, null, null, SUMMARY_VALUE)
        );
    }

    private void testSuccessfulParsing(
        String reasoningDetailJson,
        ConstructingObjectParser<ReasoningDetail, Void> responseParser,
        ReasoningDetail.SummaryReasoningDetail expectedReasoningDetail
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
    protected ReasoningDetail.SummaryReasoningDetail mutateInstanceForVersion(
        ReasoningDetail.SummaryReasoningDetail instance,
        TransportVersion version
    ) {
        // checks for version compatibility are done outside tested class, so we can return the instance as is without mutation
        return instance;
    }

    @Override
    protected Writeable.Reader<ReasoningDetail.SummaryReasoningDetail> instanceReader() {
        return ReasoningDetail.SummaryReasoningDetail::new;
    }

    @Override
    protected ReasoningDetail.SummaryReasoningDetail createTestInstance() {
        return randomSummaryReasoningDetail();
    }

    static ReasoningDetail.SummaryReasoningDetail randomSummaryReasoningDetail() {
        return new ReasoningDetail.SummaryReasoningDetail(
            randomAlphaOfLengthOrNull(10),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomAlphaOfLength(10)
        );
    }

    @Override
    protected ReasoningDetail.SummaryReasoningDetail mutateInstance(ReasoningDetail.SummaryReasoningDetail instance) throws IOException {
        var format = instance.format();
        var id = instance.id();
        var index = instance.index();
        var summary = instance.summary();

        switch (between(0, 3)) {
            case 0 -> format = randomValueOtherThan(format, () -> randomAlphaOfLengthOrNull(10));
            case 1 -> id = randomValueOtherThan(id, () -> randomAlphaOfLengthOrNull(10));
            case 2 -> index = randomValueOtherThan(index, ESTestCase::randomNonNegativeLongOrNull);
            case 3 -> summary = randomValueOtherThan(summary, () -> randomAlphaOfLength(10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ReasoningDetail.SummaryReasoningDetail(format, id, index, summary);
    }

    @Override
    protected ReasoningDetail.SummaryReasoningDetail doParseInstance(XContentParser parser) throws IOException {
        return (ReasoningDetail.SummaryReasoningDetail) ReasoningDetail.REQUEST_PARSER.apply(parser, null);
    }
}
