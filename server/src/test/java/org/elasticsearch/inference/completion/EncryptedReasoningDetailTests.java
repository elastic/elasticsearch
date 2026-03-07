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

public class EncryptedReasoningDetailTests extends AbstractBWCSerializationTestCase<ReasoningDetail.EncryptedReasoningDetail> {

    public void testParsingEncryptedReasoningDetails_AllFields() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "format": "some encrypted reasoning detail format",
                "id": "some id 0",
                "index": 0,
                "data": "some encrypted data"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.EncryptedReasoningDetail(
                "some encrypted reasoning detail format",
                "some id 0",
                0L,
                "some encrypted data"
            );

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingEncryptedReasoningDetail_OnlyRequiredFields() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "data": "some encrypted data"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.EncryptedReasoningDetail(null, null, null, "some encrypted data");

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingEncryptedReasoningDetail_UnknownField() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted",
                "data": "some encrypted data",
                "unknown_field": "some value"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.EncryptedReasoningDetail(null, null, null, "some encrypted data");

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingEncryptedReasoningDetail_NoData_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.encrypted"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(XContentParseException.class, () -> ReasoningDetail.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Required field [data] is missing for reasoning details of type [reasoning.encrypted]"));
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
        return (ReasoningDetail.EncryptedReasoningDetail) ReasoningDetail.PARSER.apply(parser, null);
    }
}
