/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.mapper;

import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.DocumentParsingException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.List;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class HistogramParserTests extends ESTestCase {

    private static final String TEST_FIELD_NAME = "test_field";

    public void testParseRandomHistogram() throws IOException {

        List<Double> centroids = IntStream.range(0, randomInt(100))
            .mapToDouble(i -> randomDoubleBetween(-1e6, 1e6, true))
            .sorted()
            .distinct()
            .boxed()
            .toList();
        List<Long> counts = centroids.stream().map(d -> randomLongBetween(0, 1000)).toList();

        HistogramParser.ParsedHistogram parsed;
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject().field("values", centroids).field("counts", counts).endObject();
            String json = Strings.toString(builder);
            parsed = doParse(json);
        }

        assertThat(parsed.values(), equalTo(centroids));
        assertThat(parsed.counts(), equalTo(counts));
    }

    public void testValuesNotIncreasing() {
        String json = """
            {
              "values": [1.0, 0.5, 2.0],
              "counts": [1, 2, 3]
            }
            """;
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("values must be in increasing order"));
    }

    public void testNegativeCount() {
        String json = """
            {
              "values": [1.0, 2.0, 3.0],
              "counts": [1, -2, 3]
            }
            """;
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("elements must be >= 0 but got -2"));
    }

    public void testUnknownField() {
        String json = """
            {
              "values": [1.0, 2.0],
              "counts": [1, 2],
              "unknown": 123
            }
            """;
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("unknown parameter [unknown]"));
    }

    public void testMissingValuesField() {
        String json = """
            {
              "counts": [1, 2]
            }
            """;
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("expected field called [values]"));
    }

    public void testMissingCountsField() {
        String json = """
            {
              "values": [1.0, 2.0]
            }
            """;
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("expected field called [counts]"));
    }

    public void testMismatchedLengths() {
        String json = """
            {
              "values": [1.0, 2.0, 3.0],
              "counts": [1, 2]
            }
            """;
        DocumentParsingException ex = expectThrows(DocumentParsingException.class, () -> doParse(json));
        assertThat(ex.getMessage(), containsString("expected same length from [values] and"));
    }

    private static HistogramParser.ParsedHistogram doParse(String json) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json)) {
            parser.nextToken();
            parser.nextToken(); // skip START_OBJECT token
            return HistogramParser.parse(TEST_FIELD_NAME, parser);
        }
    }

}
