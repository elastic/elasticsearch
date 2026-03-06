/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.inference.action;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.inference.completion.ReasoningDetail;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.ml.AbstractBWCWireSerializationTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.is;

public class SummaryReasoningDetailTests extends AbstractBWCWireSerializationTestCase<ReasoningDetail.SummaryReasoningDetail> {

    public void testParsingSummaryReasoningDetails_AllFields() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.summary",
                "format": "some summary reasoning detail format",
                "id": "some id 1",
                "index": 1,
                "summary": "some summary"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.SummaryReasoningDetail(
                "some summary reasoning detail format",
                "some id 1",
                1L,
                "some summary"
            );

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingSummaryReasoningDetail_OnlyRequiredFields() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.summary",
                "summary": "some summary"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.SummaryReasoningDetail(null, null, null, "some summary");

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingSummaryReasoningDetail_UnknownField() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.summary",
                "summary": "some summary",
                "unknown_field": "some value"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var reasoningDetail = ReasoningDetail.PARSER.apply(parser, null);
            var expected = new ReasoningDetail.SummaryReasoningDetail(null, null, null, "some summary");

            assertThat(reasoningDetail, is(expected));
        }
    }

    public void testParsingSummaryReasoningDetail_NoSummary_ThrowsException() throws IOException {
        String reasoningDetailJson = """
            {
                "type": "reasoning.summary"
            }
            """;

        try (var parser = createParser(JsonXContent.jsonXContent, reasoningDetailJson)) {
            var exception = assertThrows(XContentParseException.class, () -> ReasoningDetail.PARSER.apply(parser, null));
            ElasticsearchStatusException rootCause = (ElasticsearchStatusException) ExceptionsHelper.unwrap(
                exception,
                ElasticsearchStatusException.class
            );
            assertThat(rootCause.getMessage(), is("Required field [summary] is missing for reasoning details of type [reasoning.summary]"));
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
}
