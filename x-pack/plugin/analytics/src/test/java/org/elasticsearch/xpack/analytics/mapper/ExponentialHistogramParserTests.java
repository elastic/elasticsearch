/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.exponentialhistogram.ExponentialHistogram;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramTestUtils;
import org.elasticsearch.exponentialhistogram.ExponentialHistogramXContent;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.sourcebatch.SourceSchema;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_SCALE;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_SCALE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Runs every case through {@link ExponentialHistogramParser#parse} on a document and through
 * {@link ExponentialHistogramParser#parseColumns} on the ESCF columns of the same document, so that both agree on what
 * they accept and on what they read from it. Where {@code parse} fails, {@code parseColumns} must fail too, so that the
 * batch goes to the row path, which then reports the document's error.
 */
public class ExponentialHistogramParserTests extends ESTestCase {

    private static final String TEST_FIELD_NAME = "test_field";

    public enum Mode {
        /** Parses an x-content document with {@link ExponentialHistogramParser#parse}. */
        DOCUMENT,
        /** Encodes the documents into one ESCF batch and reads its columns with {@link ExponentialHistogramParser#parseColumns}. */
        COLUMNS
    }

    private final Mode mode;

    public ExponentialHistogramParserTests(@Name("mode") Mode mode) {
        this.mode = mode;
    }

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(Mode.values()).map(mode -> new Object[] { mode }).toList();
    }

    public void testParseRandomHistogram() throws IOException {
        final List<ExponentialHistogram> histograms = new ArrayList<>();
        final List<String> sources = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            if (randomBoolean()) {
                // A document without the field, so that the columns of the others have gaps.
                histograms.add(null);
                sources.add(null);
                continue;
            }
            ExponentialHistogram histogram = ExponentialHistogramTestUtils.randomHistogram();
            histograms.add(histogram);
            try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
                ExponentialHistogramXContent.serialize(builder, histogram);
                sources.add(Strings.toString(builder));
            }
        }

        final List<ExponentialHistogramParser.ParsedExponentialHistogram> parsedHistograms = parseAll(sources.toArray(String[]::new));
        for (int i = 0; i < histograms.size(); i++) {
            final ExponentialHistogram histogram = histograms.get(i);
            final ExponentialHistogramParser.ParsedExponentialHistogram parsed = parsedHistograms.get(i);
            if (histogram == null) {
                assertThat(parsed, nullValue());
                continue;
            }

            List<IndexWithCount> expectedPositiveBuckets = IndexWithCount.fromIterator(histogram.positiveBuckets().iterator());
            List<IndexWithCount> expectedNegativeBuckets = IndexWithCount.fromIterator(histogram.negativeBuckets().iterator());

            assertThat(parsed.scale(), equalTo(histogram.scale()));

            assertThat(parsed.zeroThreshold(), equalTo(histogram.zeroBucket().zeroThreshold()));
            assertThat(parsed.zeroCount(), equalTo(histogram.zeroBucket().count()));

            assertThat(parsed.positiveBuckets(), equalTo(expectedPositiveBuckets));
            assertThat(parsed.negativeBuckets(), equalTo(expectedNegativeBuckets));

            assertThat(parsed.sum(), equalTo(histogram.isEmpty() ? null : histogram.sum()));
            assertThat(parsed.min(), equalTo(Double.isNaN(histogram.min()) ? null : histogram.min()));
            assertThat(parsed.max(), equalTo(Double.isNaN(histogram.max()) ? null : histogram.max()));
        }
    }

    public void testParseFullHistogram() throws IOException {
        String json = """
            {"max":5.0,"negative":{"counts":[4],"indices":[-2]},"positive":{"indices":[3,1],"counts":[2,1]},
            "zero":{"threshold":0.5,"count":7},"min":-3.5,"sum":12.25,"scale":2}""";
        ExponentialHistogramParser.ParsedExponentialHistogram parsed = doParse(json);
        assertThat(parsed.scale(), equalTo(2));
        assertThat(parsed.zeroThreshold(), equalTo(0.5));
        assertThat(parsed.zeroCount(), equalTo(7L));
        assertThat(parsed.positiveBuckets(), equalTo(List.of(new IndexWithCount(1, 1), new IndexWithCount(3, 2))));
        assertThat(parsed.negativeBuckets(), equalTo(List.of(new IndexWithCount(-2, 4))));
        assertThat(parsed.sum(), equalTo(12.25));
        assertThat(parsed.min(), equalTo(-3.5));
        assertThat(parsed.max(), equalTo(5.0));
    }

    public void testParseOnlyScale() throws IOException {
        ExponentialHistogramParser.ParsedExponentialHistogram parsed = doParse("{\"scale\":0}");
        assertThat(parsed.scale(), equalTo(0));
        assertThat(parsed.zeroThreshold(), equalTo(0.0));
        assertThat(parsed.zeroCount(), equalTo(0L));
        assertThat(parsed.positiveBuckets(), equalTo(List.of()));
        assertThat(parsed.negativeBuckets(), equalTo(List.of()));
        assertThat(parsed.sum(), nullValue());
        assertThat(parsed.min(), nullValue());
        assertThat(parsed.max(), nullValue());
    }

    public void testParseEmptyBuckets() throws IOException {
        ExponentialHistogramParser.ParsedExponentialHistogram parsed = doParse(
            "{\"scale\":0,\"positive\":{\"indices\":[],\"counts\":[]},\"negative\":{\"indices\":[]}}"
        );
        assertThat(parsed.positiveBuckets(), equalTo(List.of()));
        assertThat(parsed.negativeBuckets(), equalTo(List.of()));
    }

    public void testParseInfinity() throws IOException {
        ExponentialHistogramParser.ParsedExponentialHistogram parsed = doParse(
            "{\"scale\":0,\"sum\":\"Infinity\",\"min\":\"-Infinity\",\"max\":\"Infinity\",\"positive\":{\"indices\":[1],\"counts\":[1]}}"
        );
        assertThat(parsed.sum(), equalTo(Double.POSITIVE_INFINITY));
        assertThat(parsed.min(), equalTo(Double.NEGATIVE_INFINITY));
        assertThat(parsed.max(), equalTo(Double.POSITIVE_INFINITY));
    }

    public void testParseTruncatesFractionalIntegers() throws IOException {
        ExponentialHistogramParser.ParsedExponentialHistogram parsed = doParse(
            "{\"scale\":1.7,\"zero\":{\"count\":2.9},\"positive\":{\"indices\":[-0.5,1.5],\"counts\":[3.2,1]}}"
        );
        assertThat(parsed.scale(), equalTo(1));
        assertThat(parsed.zeroCount(), equalTo(2L));
        assertThat(parsed.positiveBuckets(), equalTo(List.of(new IndexWithCount(0, 3), new IndexWithCount(1, 1))));
    }

    public void testParseMixedNumberWidths() throws IOException {
        ExponentialHistogramParser.ParsedExponentialHistogram parsed = doParse(
            "{\"scale\":0,\"zero\":{\"threshold\":1,\"count\":5000000000},\"sum\":3,\"positive\":{\"indices\":[1,"
                + MAX_INDEX
                + "],\"counts\":[5000000000,2]}}"
        );
        assertThat(parsed.zeroThreshold(), equalTo(1.0));
        assertThat(parsed.zeroCount(), equalTo(5_000_000_000L));
        assertThat(parsed.sum(), equalTo(3.0));
        assertThat(parsed.positiveBuckets(), equalTo(List.of(new IndexWithCount(1, 5_000_000_000L), new IndexWithCount(MAX_INDEX, 2))));
    }

    public void testParseDocumentsWithDifferentKeys() throws IOException {
        List<ExponentialHistogramParser.ParsedExponentialHistogram> parsed = parseAll(
            "{\"scale\":1,\"positive\":{\"indices\":[1,2],\"counts\":[1,2]}}",
            null,
            "{\"scale\":2,\"negative\":{\"indices\":[3],\"counts\":[3]},\"sum\":1.5}",
            "{\"scale\":3}",
            null,
            "{\"scale\":4,\"positive\":{\"indices\":[5],\"counts\":[5]},\"zero\":{\"count\":1}}"
        );
        assertThat(parsed.size(), equalTo(6));
        assertThat(parsed.get(0).scale(), equalTo(1));
        assertThat(parsed.get(0).positiveBuckets(), equalTo(List.of(new IndexWithCount(1, 1), new IndexWithCount(2, 2))));
        assertThat(parsed.get(0).negativeBuckets(), equalTo(List.of()));
        assertThat(parsed.get(0).sum(), nullValue());
        assertThat(parsed.get(1), nullValue());
        assertThat(parsed.get(2).scale(), equalTo(2));
        assertThat(parsed.get(2).positiveBuckets(), equalTo(List.of()));
        assertThat(parsed.get(2).negativeBuckets(), equalTo(List.of(new IndexWithCount(3, 3))));
        assertThat(parsed.get(2).sum(), equalTo(1.5));
        assertThat(parsed.get(3).scale(), equalTo(3));
        assertThat(parsed.get(3).positiveBuckets(), equalTo(List.of()));
        assertThat(parsed.get(3).negativeBuckets(), equalTo(List.of()));
        assertThat(parsed.get(4), nullValue());
        assertThat(parsed.get(5).scale(), equalTo(4));
        assertThat(parsed.get(5).positiveBuckets(), equalTo(List.of(new IndexWithCount(5, 5))));
        assertThat(parsed.get(5).zeroCount(), equalTo(1L));
    }

    public void testParseFailureInLaterDocument() {
        String[] sources = { "{\"scale\":0}", null, "{\"scale\":0,\"zero\":{\"count\":-1}}" };
        final Class<? extends Exception> expected = mode == Mode.DOCUMENT
            ? DocumentParsingException.class
            : UnsupportedOperationException.class;
        Exception ex = expectThrows(expected, () -> parseAll(sources));
        assertThat(ex.getMessage(), containsString("zero.count field must be a non-negative number but got -1"));
    }

    public void testParseEmpty() {
        assertParseFails(
            "{}",
            DocumentParsingException.class,
            "expected field called [scale]",
            // An empty object is a leaf at the field's own path, which has no relative key the parser could read.
            "column group has unexpected relative key []"
        );
    }

    public void testParseScaleMissingWithOtherFields() {
        assertParseFails("{\"zero\":{\"count\":1}}", DocumentParsingException.class, "expected field called [scale]");
    }

    public void testParseScaleNotNumber() {
        assertParseFails(
            "{\"scale\":\"foo\"}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [VALUE_NUMBER]",
            "[scale] value in document [0] is [STRING], not a number"
        );
    }

    public void testParseScaleNull() {
        assertParseFails(
            "{\"scale\":null}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [VALUE_NUMBER]",
            "[scale] value in document [0] is [NULL], not a number"
        );
    }

    public void testParseScaleTooLow() {
        assertParseFails(
            "{\"scale\":" + (MIN_SCALE - 1) + "}",
            DocumentParsingException.class,
            "scale field must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "] but got " + (MIN_SCALE - 1)
        );
    }

    public void testParseScaleTooHigh() {
        assertParseFails(
            "{\"scale\":" + (MAX_SCALE + 1) + "}",
            DocumentParsingException.class,
            "scale field must be in range [" + MIN_SCALE + ", " + MAX_SCALE + "] but got " + (MAX_SCALE + 1)
        );
    }

    public void testParseSumNotNumber() {
        assertParseFails(
            "{\"scale\":0,\"sum\":\"foo\"}",
            DocumentParsingException.class,
            "expected a number but got VALUE_STRING",
            "[sum] value in document [0] is [STRING], not a number"
        );
    }

    public void testParseMinNull() {
        assertParseFails(
            "{\"scale\":0,\"min\":null}",
            DocumentParsingException.class,
            "expected a number but got VALUE_NULL",
            "[min] value in document [0] is [NULL], not a number"
        );
    }

    public void testParseMaxBoolean() {
        assertParseFails(
            "{\"scale\":0,\"max\":true}",
            DocumentParsingException.class,
            "expected a number but got VALUE_BOOLEAN",
            "[max] value in document [0] is [TRUE], not a number"
        );
    }

    public void testParseZeroNotObject() {
        assertParseFails(
            "{\"scale\":0,\"zero\":\"not_an_object\"}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [START_OBJECT]",
            "column group has unexpected relative key [zero]"
        );
    }

    public void testParseZeroThresholdNotNumber() {
        assertParseFails(
            "{\"scale\":0,\"zero\":{\"threshold\":\"not_a_number\"}}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [VALUE_NUMBER]",
            "[zero.threshold] value in document [0] is [STRING], not a number"
        );
    }

    public void testParseZeroThresholdNegative() {
        assertParseFails(
            "{\"scale\":0,\"zero\":{\"threshold\":-1.0}}",
            DocumentParsingException.class,
            "zero.threshold field must be a non-negative, finite number but got -1.0"
        );
    }

    public void testParseZeroCountNotNumber() {
        assertParseFails(
            "{\"scale\":0,\"zero\":{\"count\":\"not_a_number\"}}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [VALUE_NUMBER]",
            "[zero.count] value in document [0] is [STRING], not a number"
        );
    }

    public void testParseZeroCountNegative() {
        assertParseFails(
            "{\"scale\":0,\"zero\":{\"count\":-1}}",
            DocumentParsingException.class,
            "zero.count field must be a non-negative number but got -1"
        );
    }

    public void testParseZeroUnknownField() {
        assertParseFails(
            "{\"scale\":0,\"zero\":{\"unknown_field\":123}}",
            DocumentParsingException.class,
            "with unknown parameter for zero sub-object [unknown_field]",
            "column group has unexpected relative key [zero.unknown_field]"
        );
    }

    public void testParsePositiveNotObject() {
        assertParseFails(
            "{\"scale\":0,\"positive\":\"not_an_object\"}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [START_OBJECT]",
            "column group has unexpected relative key [positive]"
        );
    }

    public void testParseNegativeNotObject() {
        assertParseFails(
            "{\"scale\":0,\"negative\":\"not_an_object\"}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [START_OBJECT]",
            "column group has unexpected relative key [negative]"
        );
    }

    public void testParseIndicesNotArray() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":\"not_an_array\"}}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [START_ARRAY]",
            "[positive.indices] value in document [0] is [STRING], not an array"
        );
    }

    public void testParseIndicesNull() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":null}}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [START_ARRAY]",
            "[positive.indices] value in document [0] is [NULL], not an array"
        );
    }

    public void testParseCountsNotArray() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"counts\":\"not_an_array\"}}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [START_ARRAY]",
            "[positive.counts] value in document [0] is [STRING], not an array"
        );
    }

    public void testParseIndicesElementNotNumber() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[\"not_a_number\"],\"counts\":[1]}}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [VALUE_NUMBER]",
            "[positive.indices] element of type [STRING] is not a number"
        );
    }

    public void testParseCountsElementNotNumber() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[1],\"counts\":[\"not_a_number\"]}}",
            ParsingException.class,
            "Failed to parse object: expecting token of type [VALUE_NUMBER]",
            "[positive.counts] element of type [STRING] is not a number"
        );
    }

    public void testParseIndicesBelowMin() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[" + (MIN_INDEX - 1) + "],\"counts\":[1]}}",
            DocumentParsingException.class,
            "positive.indices values must all be in range [" + MIN_INDEX + ", " + MAX_INDEX + "] but got " + (MIN_INDEX - 1)
        );
    }

    public void testParseIndicesAboveMax() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[" + (MAX_INDEX + 1) + "],\"counts\":[1]}}",
            DocumentParsingException.class,
            "positive.indices values must all be in range [" + MIN_INDEX + ", " + MAX_INDEX + "] but got " + (MAX_INDEX + 1)
        );
    }

    public void testParseDuplicateIndices() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[1,1],\"counts\":[1,2]}}",
            DocumentParsingException.class,
            "expected entries of [positive.indices] to be unique, but got 1 multiple times"
        );
    }

    public void testParseCountsZero() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[1],\"counts\":[0]}}",
            DocumentParsingException.class,
            "positive.counts values must all be greater than zero but got 0"
        );
    }

    public void testParseCountsNegative() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[1],\"counts\":[-1]}}",
            DocumentParsingException.class,
            "positive.counts values must all be greater than zero but got -1"
        );
    }

    public void testParseCountsIndicesLengthsDiffer() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[1,2],\"counts\":[1]}}",
            DocumentParsingException.class,
            "expected same length from [positive.indices] and [positive.counts] but got [2 != 1]"
        );
    }

    public void testParseCountsLongerThanIndices() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"indices\":[1],\"counts\":[1,2,3]}}",
            DocumentParsingException.class,
            "expected same length from [positive.indices] and [positive.counts] but got [1 != 3]"
        );
    }

    public void testParseCountsWithoutIndices() {
        assertParseFails(
            "{\"scale\":0,\"negative\":{\"counts\":[1]}}",
            DocumentParsingException.class,
            "expected same length from [negative.indices] and [negative.counts] but got [0 != 1]"
        );
    }

    public void testParseUnknownFieldInPositiveSubObject() {
        assertParseFails(
            "{\"scale\":0,\"positive\":{\"unknown_field\":123}}",
            DocumentParsingException.class,
            "with unknown parameter for positive sub-object [unknown_field]",
            "column group has unexpected relative key [positive.unknown_field]"
        );
    }

    public void testParseUnknownFieldInNegativeSubObject() {
        assertParseFails(
            "{\"scale\":0,\"negative\":{\"unknown_field\":123}}",
            DocumentParsingException.class,
            "with unknown parameter for negative sub-object [unknown_field]",
            "column group has unexpected relative key [negative.unknown_field]"
        );
    }

    public void testParseUnknownTopLevelField() {
        assertParseFails(
            "{\"scale\":0,\"unknown_field\":123}",
            DocumentParsingException.class,
            "with unknown parameter [unknown_field]",
            "column group has unexpected relative key [unknown_field]"
        );
    }

    public void testDuplicateRelativeKey() throws IOException {
        assumeTrue("relative keys only exist for columns", mode == Mode.COLUMNS);
        try (EscfBatch batch = encode("{\"scale\":0}")) {
            final EscfColumn scale = batch.column(0);
            UnsupportedOperationException ex = expectThrows(
                UnsupportedOperationException.class,
                () -> ExponentialHistogramParser.parseColumns(
                    TEST_FIELD_NAME,
                    new EscfColumn[] { scale, scale },
                    new String[] { "scale", "scale" },
                    UnsupportedOperationException::new,
                    (histogram, row) -> fail("parsed a histogram")
                )
            );
            assertThat(ex.getMessage(), equalTo("column group has duplicate [scale] key"));
        }
    }

    private ExponentialHistogramParser.ParsedExponentialHistogram doParse(String json) throws IOException {
        return parseAll(json).get(0);
    }

    /**
     * Parses one document per entry of {@code histograms}, {@code null} standing for a document without the field, and
     * returns the parsed histogram of each, {@code null} for those without.
     */
    private List<ExponentialHistogramParser.ParsedExponentialHistogram> parseAll(String... histograms) throws IOException {
        return switch (mode) {
            case DOCUMENT -> {
                final List<ExponentialHistogramParser.ParsedExponentialHistogram> results = new ArrayList<>(histograms.length);
                for (String histogram : histograms) {
                    results.add(histogram == null ? null : parseDocument(histogram));
                }
                yield results;
            }
            case COLUMNS -> parseColumns(histograms);
        };
    }

    /** Asserts that both parsers reject {@code json} with the same message, the document parser behind a location prefix. */
    private void assertParseFails(String json, Class<? extends Exception> documentException, String message) {
        assertParseFails(json, documentException, message, "error parsing field [" + TEST_FIELD_NAME + "], " + message);
    }

    private void assertParseFails(
        String json,
        Class<? extends Exception> documentException,
        String documentMessage,
        String columnsMessage
    ) {
        switch (mode) {
            case DOCUMENT -> {
                Exception ex = expectThrows(documentException, () -> doParse(json));
                assertThat(ex.getMessage(), containsString(documentMessage));
            }
            case COLUMNS -> {
                UnsupportedOperationException ex = expectThrows(UnsupportedOperationException.class, () -> doParse(json));
                assertThat(ex.getMessage(), equalTo(columnsMessage));
            }
        }
    }

    private static ExponentialHistogramParser.ParsedExponentialHistogram parseDocument(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken();
            parser.nextToken(); // skip START_OBJECT token
            return ExponentialHistogramParser.parse(TEST_FIELD_NAME, parser);
        }
    }

    /** Reads the histograms from the columns of their ESCF batch, as the batch mapping path does. */
    private static List<ExponentialHistogramParser.ParsedExponentialHistogram> parseColumns(String... histograms) throws IOException {
        try (EscfBatch batch = encode(histograms)) {
            final SourceSchema schema = batch.schema();
            final EscfColumn[] columns = new EscfColumn[schema.leafCount()];
            final String[] relativeKeys = new String[schema.leafCount()];
            for (int leaf = 0; leaf < schema.leafCount(); leaf++) {
                columns[leaf] = batch.column(leaf);
                relativeKeys[leaf] = relativeKey(schema.getFullPath(leaf));
            }
            final List<ExponentialHistogramParser.ParsedExponentialHistogram> results = Arrays.asList(
                new ExponentialHistogramParser.ParsedExponentialHistogram[histograms.length]
            );
            ExponentialHistogramParser.parseColumns(
                TEST_FIELD_NAME,
                columns,
                relativeKeys,
                UnsupportedOperationException::new,
                (histogram, row) -> {
                    assertThat("document [" + row + "] parsed twice", results.get(row), nullValue());
                    results.set(row, histogram);
                }
            );
            return results;
        }
    }

    /** Encodes one document per entry of {@code histograms}, holding it as {@link #TEST_FIELD_NAME} unless it is {@code null}. */
    private static EscfBatch encode(String... histograms) throws IOException {
        try (
            EscfEncoder encoder = new EscfEncoder(BytesRefRecycler.NON_RECYCLING_INSTANCE, EscfEncoder.isSimdEnabled() && randomBoolean())
        ) {
            for (String histogram : histograms) {
                final String source = histogram == null ? "{}" : "{\"" + TEST_FIELD_NAME + "\":" + histogram + "}";
                encoder.addDocument(new BytesArray(source), XContentType.JSON, 0);
            }
            return encoder.buildPartition(0);
        }
    }

    /** The key of {@code path} relative to {@link #TEST_FIELD_NAME}, empty for a leaf at the field's own path. */
    private static String relativeKey(String path) {
        if (path.equals(TEST_FIELD_NAME)) {
            return "";
        }
        assert path.startsWith(TEST_FIELD_NAME + ".") : path;
        return path.substring(TEST_FIELD_NAME.length() + 1);
    }
}
