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

public class EncryptedReasoningDetailTests extends AbstractBWCSerializationTestCase<ReasoningDetail.EncryptedReasoningDetail> {

    private static final String FORMAT_VALUE = "some encrypted reasoning detail format";
    private static final String ID_VALUE = "some id 0";
    private static final long INDEX_VALUE = 0L;
    private static final String DATA_VALUE = "some encrypted data";
    private static final String SUMMARY_VALUE = "some summary";
    private static final String TEXT_VALUE = "some text";
    private static final String SIGNATURE_VALUE = "some signature";

    public void testParsingRequestEncryptedReasoningDetail_AllFields() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "format": "%s",
                    "id": "%s",
                    "index": %d,
                    "data": "%s"
                }
                """, FORMAT_VALUE, ID_VALUE, INDEX_VALUE, DATA_VALUE),
            ReasoningDetail.REQUEST_PARSER,
            new ReasoningDetail.EncryptedReasoningDetail(FORMAT_VALUE, ID_VALUE, INDEX_VALUE, DATA_VALUE)
        );
    }

    public void testParsingResponseEncryptedReasoningDetail_AllFields() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "format": "%s",
                    "id": "%s",
                    "index": %d,
                    "data": "%s"
                }
                """, FORMAT_VALUE, ID_VALUE, INDEX_VALUE, DATA_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.EncryptedReasoningDetail(FORMAT_VALUE, ID_VALUE, INDEX_VALUE, DATA_VALUE)
        );
    }

    public void testParsingRequestEncryptedReasoningDetail_OnlyRequiredFields() throws IOException {
        testSuccessfulParsing(Strings.format("""
            {
                "type": "reasoning.encrypted",
                "data": "%s"
            }
            """, DATA_VALUE), ReasoningDetail.REQUEST_PARSER, new ReasoningDetail.EncryptedReasoningDetail(null, null, null, DATA_VALUE));
    }

    public void testParsingRequestEncryptedReasoningDetail_NoData_ThrowsException() throws IOException {
        testFailedParsing("""
            {
                "type": "reasoning.encrypted"
            }
            """, "Required field [data] is missing for reasoning details of type [reasoning.encrypted]");
    }

    public void testParsingResponseEncryptedReasoningDetail_NoData_Ignored() throws IOException {
        testSuccessfulParsing("""
            {
                "type": "reasoning.encrypted"
            }
            """, ReasoningDetail.RESPONSE_PARSER, new ReasoningDetail.EncryptedReasoningDetail(null, null, null, null));
    }

    public void testParsingRequestEncryptedReasoningDetail_SummaryFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "data": "%s",
                    "summary": "%s"
                }
                """, DATA_VALUE, SUMMARY_VALUE),
            Strings.format(
                "Field [summary] is not expected for reasoning details of type [reasoning.encrypted], but found [%s]",
                SUMMARY_VALUE
            )
        );
    }

    public void testParsingResponseEncryptedReasoningDetail_SummaryFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "data": "%s",
                    "summary": "%s"
                }
                """, DATA_VALUE, SUMMARY_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.EncryptedReasoningDetail(null, null, null, DATA_VALUE)
        );
    }

    public void testParsingRequestEncryptedReasoningDetail_TextFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "data": "%s",
                    "text": "%s"
                }
                """, DATA_VALUE, TEXT_VALUE),
            Strings.format("Field [text] is not expected for reasoning details of type [reasoning.encrypted], but found [%s]", TEXT_VALUE)
        );
    }

    public void testParsingResponseEncryptedReasoningDetail_TextFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "data": "%s",
                    "text": "%s"
                }
                """, DATA_VALUE, TEXT_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.EncryptedReasoningDetail(null, null, null, DATA_VALUE)
        );
    }

    public void testParsingRequestEncryptedReasoningDetail_SignatureFieldPresent_ThrowsException() throws IOException {
        testFailedParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "data": "%s",
                    "signature": "%s"
                }
                """, DATA_VALUE, SIGNATURE_VALUE),
            Strings.format(
                "Field [signature] is not expected for reasoning details of type [reasoning.encrypted], but found [%s]",
                SIGNATURE_VALUE
            )
        );
    }

    public void testParsingResponseEncryptedReasoningDetail_SignatureFieldPresent_Ignored() throws IOException {
        testSuccessfulParsing(
            Strings.format("""
                {
                    "type": "reasoning.encrypted",
                    "data": "%s",
                    "signature": "%s"
                }
                """, DATA_VALUE, SIGNATURE_VALUE),
            ReasoningDetail.RESPONSE_PARSER,
            new ReasoningDetail.EncryptedReasoningDetail(null, null, null, DATA_VALUE)
        );
    }

    private void testSuccessfulParsing(
        String reasoningDetailJson,
        ConstructingObjectParser<ReasoningDetail, Void> responseParser,
        ReasoningDetail.EncryptedReasoningDetail expectedReasoningDetail
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
    protected ReasoningDetail.EncryptedReasoningDetail mutateInstanceForVersion(
        ReasoningDetail.EncryptedReasoningDetail instance,
        TransportVersion version
    ) {
        // checks for version compatibility are done outside tested class, so we can return the instance as is without mutation
        return instance;
    }

    @Override
    protected Writeable.Reader<ReasoningDetail.EncryptedReasoningDetail> instanceReader() {
        return ReasoningDetail.EncryptedReasoningDetail::new;
    }

    @Override
    protected ReasoningDetail.EncryptedReasoningDetail createTestInstance() {
        return randomEncryptedReasoningDetail();
    }

    static ReasoningDetail.EncryptedReasoningDetail randomEncryptedReasoningDetail() {
        return new ReasoningDetail.EncryptedReasoningDetail(
            randomAlphaOfLengthOrNull(10),
            randomAlphaOfLengthOrNull(10),
            randomNonNegativeLongOrNull(),
            randomAlphaOfLength(10)
        );
    }

    @Override
    protected ReasoningDetail.EncryptedReasoningDetail mutateInstance(ReasoningDetail.EncryptedReasoningDetail instance)
        throws IOException {
        var format = instance.format();
        var id = instance.id();
        var index = instance.index();
        var data = instance.data();

        switch (between(0, 3)) {
            case 0 -> format = randomValueOtherThan(format, () -> randomAlphaOfLengthOrNull(10));
            case 1 -> id = randomValueOtherThan(id, () -> randomAlphaOfLengthOrNull(10));
            case 2 -> index = randomValueOtherThan(index, ESTestCase::randomNonNegativeLongOrNull);
            case 3 -> data = randomValueOtherThan(data, () -> randomAlphaOfLength(10));
            default -> throw new AssertionError("Illegal randomisation branch");
        }
        return new ReasoningDetail.EncryptedReasoningDetail(format, id, index, data);
    }

    @Override
    protected ReasoningDetail.EncryptedReasoningDetail doParseInstance(XContentParser parser) throws IOException {
        return (ReasoningDetail.EncryptedReasoningDetail) ReasoningDetail.REQUEST_PARSER.apply(parser, null);
    }
}
