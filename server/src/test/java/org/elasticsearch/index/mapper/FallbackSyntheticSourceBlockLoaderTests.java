/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

/**
 * Unit tests for {@link FallbackSyntheticSourceBlockLoader.SingleValueReader#parse}: a scalar reader (keyword, number, ip, date, ...)
 * must skip an object/array synthetic {@code _source} value and yield no value rather than assert or throw.
 */
public class FallbackSyntheticSourceBlockLoaderTests extends ESTestCase {

    public void testParsesScalar() throws IOException {
        assertThat(parseValue("\"x\"", null), contains("x"));
    }

    public void testParsesNumberScalar() throws IOException {
        assertThat(parseValue("5", null), contains("5"));
    }

    public void testParsesArrayOfScalars() throws IOException {
        assertThat(parseValue("[\"a\",\"b\"]", null), contains("a", "b"));
    }

    public void testNullWithoutNullValueYieldsNothing() throws IOException {
        assertThat(parseValue("null", null), empty());
    }

    public void testNullSubstitutesConfiguredNullValue() throws IOException {
        assertThat(parseValue("null", "NV"), contains("NV"));
        assertThat(parseValue("[null,\"a\"]", "NV"), contains("NV", "a"));
    }

    public void testObjectIsSkippedAndYieldsNothing() throws IOException {
        assertThat(parseValue("{\"nested\":\"a\"}", null), empty());
        // A configured null_value must not turn an object into that value.
        assertThat(parseValue("{\"nested\":\"a\"}", "NV"), empty());
    }

    public void testObjectElementsInsideArrayAreSkipped() throws IOException {
        assertThat(parseValue("[\"a\",{\"nested\":\"b\"},\"c\"]", null), contains("a", "c"));
    }

    public void testObjectAsFirstArrayElementThenScalar() throws IOException {
        assertThat(parseValue("[{\"n\":1},\"a\"]", null), contains("a"));
    }

    public void testArrayOfObjectsYieldsNothing() throws IOException {
        assertThat(parseValue("[{\"a\":1},{\"b\":2}]", null), empty());
    }

    public void testNestedArrayElementsAreSkipped() throws IOException {
        assertThat(parseValue("[\"a\",[\"b\",\"c\"]]", null), contains("a"));
    }

    /**
     * Runs {@link FallbackSyntheticSourceBlockLoader.SingleValueReader#parse} over {@code jsonValue}, positioned exactly the way
     * the row-stride reader positions it: on the value token of a field. The reader stringifies whatever scalar(s) it accepts so
     * the assertions can compare plain {@link String}s.
     */
    private List<String> parseValue(String jsonValue, String nullValue) throws IOException {
        var accumulator = new ArrayList<String>();
        var reader = new FallbackSyntheticSourceBlockLoader.SingleValueReader<String>(nullValue) {
            @Override
            public void convertValue(Object value, List<String> acc) {
                acc.add(String.valueOf(value));
            }

            @Override
            public void writeToBlock(List<String> values, BlockLoader.Builder blockBuilder) {
                throw new UnsupportedOperationException("not needed for parse() coverage");
            }

            @Override
            protected void parseNonNullValue(XContentParser parser, List<String> acc) throws IOException {
                acc.add(parser.text());
            }
        };

        String json = "{\"f\":" + jsonValue + "}";
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(XContentParserConfiguration.EMPTY, json)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            parser.nextToken(); // advance onto the field's value token, matching how readFromFieldValue/parseFieldFromParent call parse
            reader.parse(parser, accumulator);
        }
        return accumulator;
    }
}
