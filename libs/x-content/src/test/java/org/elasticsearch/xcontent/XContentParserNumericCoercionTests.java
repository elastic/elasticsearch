/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.xcontent;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.support.AbstractXContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

/**
 * {@link XContentParser} tests parameterized over {@link XContentType}, so every test runs on every format.
 */
public class XContentParserNumericCoercionTests extends ESTestCase {

    private final XContentType xContentType;

    @ParametersFactory
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(XContentType.values()).map(xContentType -> new Object[] { xContentType }).toList();
    }

    public XContentParserNumericCoercionTests(@Name("xContentType") XContentType xContentType) {
        this.xContentType = xContentType;
    }

    /**
     * Hook for subclasses that want to re-run this suite against a parser wrapper. Mirrors
     * {@link XContentParserTests#decorateParser(XContentParser)}.
     */
    protected XContentParser decorateParser(XContentParser parser) {
        return parser;
    }

    public void testFloat() throws IOException {
        final String field = randomAlphaOfLengthBetween(1, 5);
        final Float value = randomFloat();

        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();
            if (randomBoolean()) {
                builder.field(field, value);
            } else {
                builder.field(field).value(value);
            }
            builder.endObject();

            final Number number;
            BytesReference data = BytesReference.bytes(builder);
            try (XContentParser parser = decorateParser(createParser(xContentType.xContent(), data))) {
                assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
                assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
                assertEquals(field, parser.currentName());
                assertEquals(XContentParser.Token.VALUE_NUMBER, parser.nextToken());

                number = parser.numberValue();

                assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
                assertNull(parser.nextToken());
            }

            assertEquals(value, number.floatValue(), 0.0f);

            switch (xContentType) {
                case VND_CBOR, VND_SMILE, CBOR, SMILE -> assertThat(number, instanceOf(Float.class));
                case VND_JSON, VND_YAML, JSON, YAML -> assertThat(number, instanceOf(Double.class));
                default -> throw new AssertionError("unexpected x-content type [" + xContentType + "]");
            }
        }
    }

    public void testLongCoercion() throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject();

            builder.field("five", "5.5");
            builder.field("minusFive", "-5.5");

            builder.field("minNegative", "-9.2233720368547758089999e18");
            builder.field("tooNegative", "-9.223372036854775809e18");
            builder.field("maxPositive", "9.2233720368547758079999e18");
            builder.field("tooPositive", "9.223372036854775808e18");

            builder.field("expTooBig", "2e100");
            builder.field("minusExpTooBig", "-2e100");
            builder.field("maxPositiveExp", "1e2147483647");
            builder.field("tooPositiveExp", "1e2147483648");

            builder.field("expTooSmall", "2e-100");
            builder.field("minusExpTooSmall", "-2e-100");
            builder.field("maxNegativeExp", "1e-2147483647");

            builder.field("tooNegativeExp", "1e-2147483648");

            builder.endObject();

            BytesReference data = BytesReference.bytes(builder);
            try (XContentParser parser = decorateParser(createParser(xContentType.xContent(), data))) {
                assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));

                assertFieldWithValue("five", 5L, parser);
                assertFieldWithValue("minusFive", -5L, parser); // Rounds toward zero

                assertFieldWithValue("minNegative", Long.MIN_VALUE, parser);
                assertFieldWithInvalidLongValue("tooNegative", parser);
                assertFieldWithValue("maxPositive", Long.MAX_VALUE, parser);
                assertFieldWithInvalidLongValue("tooPositive", parser);

                assertFieldWithInvalidLongValue("expTooBig", parser);
                assertFieldWithInvalidLongValue("minusExpTooBig", parser);
                assertFieldWithInvalidLongValue("maxPositiveExp", parser);
                assertFieldWithInvalidLongValue("tooPositiveExp", parser);

                // too small goes to zero
                assertFieldWithValue("expTooSmall", 0L, parser);
                assertFieldWithValue("minusExpTooSmall", 0L, parser);
                assertFieldWithValue("maxNegativeExp", 0L, parser);

                assertFieldWithInvalidLongValue("tooNegativeExp", parser);
            }
        }
    }

    public void testNumericCoercionBoundsStringLength() throws IOException {
        String atLimit = "0".repeat(AbstractXContentParser.MAX_NUMERIC_STRING_LENGTH - 1) + "1";
        String overLimit = "0".repeat(AbstractXContentParser.MAX_NUMERIC_STRING_LENGTH) + "1";
        assertThat(atLimit.length(), equalTo(AbstractXContentParser.MAX_NUMERIC_STRING_LENGTH));
        assertThat(overLimit.length(), equalTo(AbstractXContentParser.MAX_NUMERIC_STRING_LENGTH + 1));

        for (CheckedConsumer<XContentParser, IOException> accessor : List.<CheckedConsumer<XContentParser, IOException>>of(
            XContentParser::shortValue,
            XContentParser::intValue,
            XContentParser::longValue,
            XContentParser::floatValue,
            XContentParser::doubleValue
        )) {
            assertNumericAccessor(atLimit, accessor);
            assertNumericAccessor(overLimit, parser -> {
                IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> accessor.accept(parser));
                assertThat(e.getMessage(), containsString("exceeds the maximum"));
            });
        }
    }

    private void assertNumericAccessor(String value, CheckedConsumer<XContentParser, IOException> assertion) throws IOException {
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            builder.startObject().field("n", value).endObject();
            try (XContentParser parser = decorateParser(createParser(xContentType.xContent(), BytesReference.bytes(builder)))) {
                assertThat(parser.nextToken(), is(XContentParser.Token.START_OBJECT));
                assertThat(parser.nextToken(), is(XContentParser.Token.FIELD_NAME));
                assertThat(parser.nextToken(), is(XContentParser.Token.VALUE_STRING));
                assertion.accept(parser);
            }
        }
    }

    private static void assertFieldWithValue(String fieldName, long fieldValue, XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), is(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), is(fieldName));
        assertThat(parser.nextToken(), is(XContentParser.Token.VALUE_STRING));
        assertThat(parser.longValue(), equalTo(fieldValue));
    }

    private static void assertFieldWithInvalidLongValue(String fieldName, XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), is(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), is(fieldName));
        assertThat(parser.nextToken(), is(XContentParser.Token.VALUE_STRING));
        expectThrows(IllegalArgumentException.class, parser::longValue);
    }
}
