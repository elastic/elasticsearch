/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.process;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.contains;

public class ProcessResultsParserTests extends ESTestCase {

    public void testParse_GivenEmptyArray() throws IOException {
        String json = "[]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            ProcessResultsParser<TestResult> parser = new ProcessResultsParser<>(TestResult.PARSER, NamedXContentRegistry.EMPTY);
            assertFalse(parser.parseResults(inputStream).hasNext());
        }
    }

    public void testParse_GivenUnknownObject() throws IOException {
        String json = "[{\"unknown\":{\"id\": 18}}]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            ProcessResultsParser<TestResult> parser = new ProcessResultsParser<>(TestResult.PARSER, NamedXContentRegistry.EMPTY);
            XContentParseException e = expectThrows(XContentParseException.class,
                () -> parser.parseResults(inputStream).forEachRemaining(a -> {
                }));
            assertEquals("[1:3] [test_result] unknown field [unknown]", e.getMessage());
        }
    }

    public void testParse_GivenArrayContainsAnotherArray() throws IOException {
        String json = "[[]]";
        try (InputStream inputStream = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8))) {
            ProcessResultsParser<TestResult> parser = new ProcessResultsParser<>(TestResult.PARSER, NamedXContentRegistry.EMPTY);
            ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
                () -> parser.parseResults(inputStream).forEachRemaining(a -> {
                }));
            assertEquals("unexpected token [START_ARRAY]", e.getMessage());
        }
    }

    public void testParseResults() throws IOException {
        String input = "[{\"field_1\": \"a\", \"field_2\": 1.0}, {\"field_1\": \"b\", \"field_2\": 2.0},"
                + " {\"field_1\": \"c\", \"field_2\": 3.0}]";
        try (InputStream inputStream = new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8))) {

            ProcessResultsParser<TestResult> parser = new ProcessResultsParser<>(TestResult.PARSER, NamedXContentRegistry.EMPTY);
            Iterator<TestResult> testResultIterator = parser.parseResults(inputStream);

            List<TestResult> parsedResults = new ArrayList<>();
            while (testResultIterator.hasNext()) {
                parsedResults.add(testResultIterator.next());
            }

            assertThat(parsedResults, contains(new TestResult("a", 1.0), new TestResult("b", 2.0), new TestResult("c", 3.0)));
        }
    }

    private static class TestResult {

        private static final ParseField FIELD_1 = new ParseField("field_1");
        private static final ParseField FIELD_2 = new ParseField("field_2");

        private static final ConstructingObjectParser<TestResult, Void> PARSER = new ConstructingObjectParser<>("test_result",
                a -> new TestResult((String) a[0], (Double) a[1]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), FIELD_1);
            PARSER.declareDouble(ConstructingObjectParser.constructorArg(), FIELD_2);
        }

        private final String field1;
        private final double field2;

        private TestResult(String field1, double field2) {
            this.field1 = field1;
            this.field2 = field2;
        }

        @Override
        public boolean equals(Object other) {
            if (this == other) {
                return true;
            }
            if (other == null || getClass() != other.getClass()) {
                return false;
            }
            TestResult that = (TestResult) other;
            return Objects.equals(field1, that.field1) && Objects.equals(field2, that.field2);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2);
        }
    }
}
