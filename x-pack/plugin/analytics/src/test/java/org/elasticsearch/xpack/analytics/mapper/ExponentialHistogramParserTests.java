/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class ExponentialHistogramParserTests extends ESTestCase {

    private static final String TEST_FIELD_NAME = "test_field";

    public void testParseRandomHistogram() throws IOException {
        for (int i = 0; i < 20; i++) {
            ExponentialHistogram histogram = ExponentialHistogramTestUtils.randomHistogram();

            ExponentialHistogramParser.ParsedExponentialHistogram parsed;
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ExponentialHistogramXContent.serialize(builder, histogram);
                String json = Strings.toString(builder);
                parsed = doParse(json);
            }

            List<IndexWithCount> expectedPositiveBuckets = IndexWithCount.fromIterator(histogram.positiveBuckets().iterator());
            List<IndexWithCount> expectedNegativeBuckets = IndexWithCount.fromIterator(histogram.negativeBuckets().iterator());

            assertThat(parsed.scale(), equalTo(histogram.scale()));

            assertThat(parsed.zeroThreshold(), equalTo(histogram.zeroBucket().zeroThreshold()));
            assertThat(parsed.zeroCount(), equalTo(histogram.zeroBucket().count()));

            assertThat(parsed.positiveBuckets(), equalTo(expectedPositiveBuckets));
            assertThat(parsed.negativeBuckets(), equalTo(expectedNegativeBuckets));

            assertThat(parsed.sum(), equalTo(histogram.valueCount() == 0 ? null : histogram.sum()));
            assertThat(parsed.min(), equalTo(Double.isNaN(histogram.min()) ? null : histogram.min()));
            assertThat(parsed.max(), equalTo(Double.isNaN(histogram.max()) ? null : histogram.max()));
        }
    }

    public void testParseScaleMissing() {
        String json = "{}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("expected field called [scale]"));
    }

    public void testParseScaleNotNumber() {
        String json = "{\"scale\":\"foo\"}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [VALUE_NUMBER]"));
    }

    public void testParseScaleTooLow() {
        String json = "{\"scale\":" + (MIN_SCALE - 1) + "}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(
            ex.getMessage(),
            containsString("scale field must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "] but got " + (MIN_SCALE - 1))
        );
    }

    public void testParseScaleTooHigh() {
        String json = "{\"scale\":" + (MAX_SCALE + 1) + "}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(
            ex.getMessage(),
            containsString("scale field must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "] but got " + (MAX_SCALE + 1))
        );
    }

    public void testParseZeroNotObject() {
        String json = "{\"scale\":0,\"zero\":\"not_an_object\"}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [START_OBJECT]"));
    }

    public void testParseZeroThresholdNotNumber() {
        String json = "{\"scale\":0,\"zero\":{\"threshold\":\"not_a_number\"}}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [VALUE_NUMBER]"));
    }

    public void testParseZeroThresholdNegative() {
        String json = "{\"scale\":0,\"zero\":{\"threshold\":-1.0}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("zero.threshold field must be a non-negative, finite number but got -1.0"));
    }

    public void testParseZeroCountNotNumber() {
        String json = "{\"scale\":0,\"zero\":{\"count\":\"not_a_number\"}}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [VALUE_NUMBER]"));
    }

    public void testParseZeroCountNegative() {
        String json = "{\"scale\":0,\"zero\":{\"count\":-1}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("zero.count field must be a non-negative number but got -1"));
    }

    public void testParseZeroUnknownField() {
        String json = "{\"scale\":0,\"zero\":{\"unknown_field\":123}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("with unknown parameter for zero sub-object [unknown_field]"));
    }

    public void testParsePositiveNotObject() {
        String json = "{\"scale\":0,\"positive\":\"not_an_object\"}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [START_OBJECT]"));
    }

    public void testParseNegativeNotObject() {
        String json = "{\"scale\":0,\"negative\":\"not_an_object\"}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [START_OBJECT]"));
    }

    public void testParseIndicesNotArray() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":\"not_an_array\"}}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [START_ARRAY]"));
    }

    public void testParseCountsNotArray() {
        String json = "{\"scale\":0,\"positive\":{\"counts\":\"not_an_array\"}}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [START_ARRAY]"));
    }

    public void testParseIndicesElementNotNumber() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[\"not_a_number\"],\"counts\":[1]}}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [VALUE_NUMBER]"));
    }

    public void testParseCountsElementNotNumber() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[1],\"counts\":[\"not_a_number\"]}}";
        ParsingException ex = expectThrows(ParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("Failed to parse object: expecting token of type [VALUE_NUMBER]"));
    }

    public void testParseIndicesBelowMin() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[" + (MIN_INDEX - 1) + "],\"counts\":[1]}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(
            ex.getMessage(),
            containsString("positive.indices values must all be in range [" + MIN_INDEX + ", " + MAX_INDEX + "] but got " + (MIN_INDEX - 1))
        );
    }

    public void testParseIndicesAboveMax() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[" + (MAX_INDEX + 1) + "],\"counts\":[1]}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(
            ex.getMessage(),
            containsString("positive.indices values must all be in range [" + MIN_INDEX + ", " + MAX_INDEX + "] but got " + (MAX_INDEX + 1))
        );
    }

    public void testParseDuplicateIndices() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[1,1],\"counts\":[1,2]}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("expected entries of [positive.indices] to be unique, but got 1 multiple times"));
    }

    public void testParseCountsZero() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[1],\"counts\":[0]}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("positive.counts values must all be greater than zero but got 0"));
    }

    public void testParseCountsNegative() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[1],\"counts\":[-1]}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("positive.counts values must all be greater than zero but got -1"));
    }

    public void testParseCountsIndicesLengthsDiffer() {
        String json = "{\"scale\":0,\"positive\":{\"indices\":[1,2],\"counts\":[1]}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("expected same length from [positive.indices] and [positive.counts] but got [2 != 1]"));
    }

    public void testParseUnknownFieldInPositiveSubObject() {
        String json = "{\"scale\":0,\"positive\":{\"unknown_field\":123}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("with unknown parameter for positive sub-object [unknown_field]"));
    }

    public void testParseUnknownFieldInNegativeSubObject() {
        String json = "{\"scale\":0,\"negative\":{\"unknown_field\":123}}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("with unknown parameter for negative sub-object [unknown_field]"));
    }

    public void testParseUnknownTopLevelField() {
        String json = "{\"scale\":0,\"unknown_field\":123}";
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("with unknown parameter [unknown_field]"));
    }

    private static ExponentialHistogramParser.ParsedExponentialHistogram doParse(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken();
            parser.nextToken(); // skip START_OBJECT token
            return ExponentialHistogramParser.parse(TEST_FIELD_NAME, parser);
        }
    }

}
