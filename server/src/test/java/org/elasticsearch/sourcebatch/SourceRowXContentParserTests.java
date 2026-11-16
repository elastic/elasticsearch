/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParser.Token;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.instanceOf;

/**
 * Tests {@link SourceRowXContentParser} against rows produced by the production batch encoder
 * ({@link EscfEncoder}). The encoder widens scalar numbers (int → long, float → double), so
 * top-level numeric leaves parse with {@code LONG}/{@code DOUBLE} number types; elements of
 * inline arrays and key-value structures keep the narrower packed types.
 */
public class SourceRowXContentParserTests extends ESTestCase {

    /** Reads one typed value from the parser's current token. */
    @FunctionalInterface
    private interface TypedAccessor {
        Object read(XContentParser parser) throws IOException;
    }

    /** What an accessor did: the value it returned, or the exception it threw. */
    private record Outcome(Object value, Exception error) {
        static Outcome of(XContentParser parser, TypedAccessor accessor) {
            try {
                return new Outcome(accessor.read(parser), null);
            } catch (Exception e) {
                return new Outcome(null, e);
            }
        }

        @Override
        public String toString() {
            return error == null ? "returned [" + value + "]" : "threw " + error.getClass().getSimpleName();
        }
    }

    private static final Map<String, TypedAccessor> ACCESSORS = new LinkedHashMap<>();
    static {
        ACCESSORS.put("booleanValue", XContentParser::booleanValue);
        ACCESSORS.put("shortValue", XContentParser::shortValue);
        ACCESSORS.put("intValue", XContentParser::intValue);
        ACCESSORS.put("longValue", XContentParser::longValue);
        ACCESSORS.put("floatValue", XContentParser::floatValue);
        ACCESSORS.put("doubleValue", XContentParser::doubleValue);
        ACCESSORS.put("numberValue", XContentParser::numberValue);
        ACCESSORS.put("numberType", XContentParser::numberType);
        ACCESSORS.put("text", XContentParser::text);
        ACCESSORS.put("optimizedText", p -> p.optimizedText().string());
        ACCESSORS.put("charBuffer", p -> p.charBuffer().toString());
        ACCESSORS.put("textLength", XContentParser::textLength);
        ACCESSORS.put("objectText", XContentParser::objectText);
        ACCESSORS.put("objectBytes", p -> {
            final Object bytes = p.objectBytes();
            return bytes instanceof CharSequence ? bytes.toString() : bytes;
        });
    }

    /**
     * Numbers around the limits of a short, an int and a long, read at the top level of a row and as elements of arrays and objects. The
     * encoder widens an int to a long and a float to a double at the top level of a row, but a number that is an element of an array or an
     * object keeps the narrower type it was stored as, and that is the only place the int and float branches of the accessors are reached.
     * Only a value that a float holds exactly is stored as a float, so the values beyond the range of a long include powers of two: 2^64
     * and 2^100.
     */
    private static final List<String> RANGE_NUMBERS = List.of(
        "32767",
        "32768",
        "-32768",
        "-32769",
        "70000",
        "2147483647",
        "2147483648",
        "-2147483648",
        "-2147483649",
        "2147483647.5",
        "-2147483648.5",
        "1e10",
        "9007199254740993",
        "123456789012345678",
        "9223372036854775807",
        "9.223372036854775807E18",
        "9.223372036854775808E18",
        "-9.223372036854775808E18",
        "9.3E18",
        "1.8446744073709552E19",
        "-1.8446744073709552E19",
        "1.2676506002282294E30",
        "1e40",
        "1e-50"
    );

    /**
     * The typed accessors are what the row-path mappers call when a field falls back to {@link SourceRowXContentParser}. Callers such as
     * {@code ignore_malformed} rely on a value of the wrong type making a parser throw an {@link IllegalArgumentException}, which is what
     * the JSON parser does, so the row parser has to behave the same: for every kind of value and every typed accessor, it returns an
     * equivalent value when the JSON parser does and throws an {@link IllegalArgumentException} when the JSON parser throws one. The test
     * reports every difference rather than stopping at the first.
     */
    public void testTypedAccessorsMatchTheJsonParser() throws IOException {
        final String[] values = {
            "true",
            "false",
            "null",
            "0",
            "42",
            "-7",
            "1.5",
            "-0.0",
            "1e3",
            "\"12\"",
            "\"abc\"",
            "\"\"",
            "\"true\"",
            "{\"a\":1}",
            "{}",
            "[1,2]",
            "[]" };

        final Map<String, BytesReference> sources = new LinkedHashMap<>();
        for (String value : values) {
            sources.put(value, new BytesArray("{\"v\":" + value + "}"));
        }
        for (String value : RANGE_NUMBERS) {
            sources.put(value, new BytesArray("{\"v\":" + value + "}"));
        }
        assertAccessorsMatch(XContentType.JSON, sources);
    }

    /**
     * Values the JSON text format cannot spell: NaN, the infinities and values beyond the range of a float. The range checks must reject
     * the infinities and the huge values. NaN is neither below the minimum nor above the maximum, so it is not rejected and narrows to 0,
     * as it does in the CBOR parser. The test pins that parity, not an endorsement of narrowing NaN to 0.
     */
    public void testTypedAccessorsMatchTheCborParserForNonFiniteNumbers() throws IOException {
        final Map<String, BytesReference> sources = new LinkedHashMap<>();
        final Map<String, Number> values = new LinkedHashMap<>();
        values.put("double NaN", Double.NaN);
        values.put("double +Infinity", Double.POSITIVE_INFINITY);
        values.put("double -Infinity", Double.NEGATIVE_INFINITY);
        values.put("float NaN", Float.NaN);
        values.put("float +Infinity", Float.POSITIVE_INFINITY);
        values.put("double 1e300", 1e300);
        values.put("float 3e38", 3e38f);
        for (Map.Entry<String, Number> value : values.entrySet()) {
            try (XContentBuilder builder = XContentFactory.cborBuilder()) {
                builder.startObject().field("v", value.getValue()).endObject();
                sources.put(value.getKey(), BytesReference.bytes(builder));
            }
        }
        assertAccessorsMatch(XContentType.CBOR, sources);
    }

    private static final Set<String> TYPED_ACCESSORS = Set.of(
        "booleanValue",
        "shortValue",
        "intValue",
        "longValue",
        "floatValue",
        "doubleValue",
        "numberValue",
        "numberType"
    );

    /**
     * The typed accessors only make sense on a value token of the right type, and on every other token they have to throw, as the JSON
     * parser does, rather than return something left over from an earlier value. The other tests read each accessor on a value token only,
     * so this one reads them on every token of a document with nested objects and arrays, including the tokens that follow a number.
     */
    public void testTypedAccessorsMatchTheJsonParserOnEveryToken() throws IOException {
        // The object comes first because the encoder lists the nested objects of a row before its leaves.
        final BytesReference source = new BytesArray("{\"o\":{\"c\":\"x\"},\"a\":5,\"d\":[7,[1],{\"e\":1}]}");
        final List<String> differences = new ArrayList<>();
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            final int tokens = countTokens(source);
            for (int position = 1; position <= tokens; position++) {
                for (String name : TYPED_ACCESSORS) {
                    // A fresh pair of parsers per accessor and position, for the reason given in assertAccessorsMatch.
                    try (
                        XContentParser json = XContentHelper.createParserNotCompressed(
                            XContentParserConfiguration.EMPTY,
                            source,
                            XContentType.JSON
                        );
                        SourceRowXContentParser row = new SourceRowXContentParser(tree, batch.row(0))
                    ) {
                        for (int i = 0; i < position; i++) {
                            json.nextToken();
                            row.nextToken();
                        }
                        assertEquals("token " + position, json.currentToken(), row.currentToken());
                        final String problem = compare(name, Outcome.of(json, ACCESSORS.get(name)), Outcome.of(row, ACCESSORS.get(name)));
                        if (problem != null) {
                            differences.add("token " + position + " (" + json.currentToken() + ") -> " + name + ": " + problem);
                        }
                    }
                }
            }
        }
        assertTrue(
            "typed accessors differing from the JSON parser (" + differences.size() + "):\n" + String.join("\n", differences),
            differences.isEmpty()
        );
    }

    private static int countTokens(BytesReference source) throws IOException {
        try (
            XContentParser parser = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, XContentType.JSON)
        ) {
            int count = 0;
            while (parser.nextToken() != null) {
                count++;
            }
            return count;
        }
    }

    /**
     * With no current token, before the first token or after the last, reading a value is a programming error. The JSON parser throws a
     * {@link NullPointerException} for it, and so does the row parser, rather than an {@link IllegalArgumentException} that a caller
     * handling malformed values could mistake for bad input.
     */
    public void testTextAccessorsWithoutACurrentTokenThrowNullPointerException() throws IOException {
        final BytesReference source = new BytesArray("{\"a\":1}");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            final SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            for (String name : List.of("text", "optimizedText", "charBuffer", "objectText", "objectBytes")) {
                for (boolean afterTheLastToken : new boolean[] { false, true }) {
                    try (
                        XContentParser json = XContentHelper.createParserNotCompressed(
                            XContentParserConfiguration.EMPTY,
                            source,
                            XContentType.JSON
                        );
                        SourceRowXContentParser row = new SourceRowXContentParser(tree, batch.row(0))
                    ) {
                        if (afterTheLastToken) {
                            while (json.nextToken() != null) {
                                // read to the end
                            }
                            while (row.nextToken() != null) {
                                // read to the end
                            }
                        }
                        final String where = name + (afterTheLastToken ? " after the last token" : " before the first token");
                        assertNull(where, json.currentToken());
                        assertNull(where, row.currentToken());
                        assertThat(
                            where + " on the JSON parser",
                            Outcome.of(json, ACCESSORS.get(name)).error(),
                            instanceOf(NullPointerException.class)
                        );
                        assertThat(
                            where + " on the row parser",
                            Outcome.of(row, ACCESSORS.get(name)).error(),
                            instanceOf(NullPointerException.class)
                        );
                    }
                }
            }
        }
    }

    /**
     * Reads {@link #RANGE_NUMBERS} as the only element of an array and as the value in an object that is the only element of an array.
     */
    public void testTypedAccessorsMatchTheJsonParserForElementsOfArraysAndObjects() throws IOException {
        final Map<String, BytesReference> inArray = new LinkedHashMap<>();
        final Map<String, BytesReference> inObject = new LinkedHashMap<>();
        for (String value : RANGE_NUMBERS) {
            inArray.put("[" + value + "]", new BytesArray("{\"d\":[" + value + "]}"));
            inObject.put("[{\"k\":" + value + "}]", new BytesArray("{\"d\":[{\"k\":" + value + "}]}"));
        }
        // START_OBJECT, FIELD_NAME, START_ARRAY, then the value
        assertAccessorsMatch(XContentType.JSON, inArray, 4);
        // START_OBJECT, FIELD_NAME, START_ARRAY, START_OBJECT, FIELD_NAME, then the value
        assertAccessorsMatch(XContentType.JSON, inObject, 6);
    }

    private void assertAccessorsMatch(XContentType type, Map<String, BytesReference> sources) throws IOException {
        // START_OBJECT, FIELD_NAME, then the value
        assertAccessorsMatch(type, sources, 3);
    }

    /**
     * Reads each accessor from the JSON parser and from the row parser, both positioned on the {@code valueToken}th token of each source,
     * and fails with every difference found.
     */
    private void assertAccessorsMatch(XContentType type, Map<String, BytesReference> sources, int valueToken) throws IOException {
        final List<String> differences = new ArrayList<>();
        for (Map.Entry<String, BytesReference> entry : sources.entrySet()) {
            final BytesReference source = entry.getValue();
            try (EscfBatch batch = EscfEncoder.encode(List.of(source), type)) {
                final SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
                for (Map.Entry<String, TypedAccessor> accessor : ACCESSORS.entrySet()) {
                    // A fresh pair of parsers per accessor: the JSON parser converts numbers lazily and remembers the last conversion, so
                    // an earlier accessor would change what a later one returns.
                    try (
                        XContentParser json = XContentHelper.createParserNotCompressed(XContentParserConfiguration.EMPTY, source, type);
                        SourceRowXContentParser row = new SourceRowXContentParser(tree, batch.row(0))
                    ) {
                        // Read up to the value token.
                        for (int i = 0; i < valueToken; i++) {
                            json.nextToken();
                            row.nextToken();
                        }
                        assertEquals("token for " + entry.getKey(), json.currentToken(), row.currentToken());
                        final boolean floatingNumber = row.currentToken() == Token.VALUE_NUMBER
                            && numberKind(row.numberType()).equals("floating");
                        if (isKnownDifference(accessor.getKey(), json.currentToken(), floatingNumber)) {
                            continue;
                        }
                        final String problem = compare(
                            accessor.getKey(),
                            Outcome.of(json, accessor.getValue()),
                            Outcome.of(row, accessor.getValue())
                        );
                        if (problem != null) {
                            differences.add(entry.getKey() + " (" + json.currentToken() + ") -> " + accessor.getKey() + ": " + problem);
                        }
                    }
                }
            }
        }
        assertTrue(
            "typed accessors differing from the JSON parser (" + differences.size() + "):\n" + String.join("\n", differences),
            differences.isEmpty()
        );
    }

    /**
     * Differences that are known and not what this test is about:
     * <ul>
     * <li>The text of a floating-point number is the row parser's rendering of the stored value, not the spelling in the source
     * ({@code 1e3} reads back as {@code 1000.0}). The text of an integral number is compared.</li>
     * <li>{@code charBuffer}, {@code textLength} and {@code objectBytes} on a token that is not a scalar value return the token's text
     * (an opening brace) from the JSON parser; the row parser throws, as {@code text()} does for both parsers. No mapper reads those.</li>
     * </ul>
     */
    private static boolean isKnownDifference(String accessor, Token token, boolean floatingNumber) {
        return switch (accessor) {
            case "text", "optimizedText" -> floatingNumber;
            case "charBuffer", "textLength" -> floatingNumber || token.isValue() == false;
            case "objectBytes" -> token.isValue() == false;
            case "booleanValue", "shortValue", "intValue", "longValue", "floatValue", "doubleValue", "numberValue", "numberType",
                "objectText" -> false;
            default -> throw new AssertionError("unexpected accessor [" + accessor + "]");
        };
    }

    /** Returns a description of how {@code actual} differs from {@code expected}, or {@code null} if it is equivalent. */
    private static String compare(String accessor, Outcome expected, Outcome actual) {
        if (expected.error() != null) {
            if (actual.error() == null) {
                return "JSON parser " + expected + " but the row parser " + actual;
            }
            return expected.error() instanceof IllegalArgumentException && (actual.error() instanceof IllegalArgumentException) == false
                ? "JSON parser threw an IllegalArgumentException ("
                    + expected.error().getClass().getSimpleName()
                    + ") but the row parser threw "
                    + actual.error().getClass().getName()
                : null;
        }
        if (actual.error() != null) {
            return "JSON parser " + expected + " but the row parser " + actual;
        }
        if (accessor.equals("numberType")) {
            // The encoder widens int to long and float to double, so only the kind of number is compared.
            return numberKind(expected.value()).equals(numberKind(actual.value()))
                ? null
                : "JSON parser " + expected + " but the row parser " + actual;
        }
        if (expected.value() instanceof Number e && actual.value() instanceof Number a) {
            // Two integral values are compared as longs, because a double cannot tell apart two longs close to 2^63.
            final boolean same = isIntegral(e) && isIntegral(a)
                ? e.longValue() == a.longValue()
                : Double.compare(e.doubleValue(), a.doubleValue()) == 0;
            return same ? null : "JSON parser " + expected + " but the row parser " + actual;
        }
        return Objects.equals(expected.value(), actual.value()) ? null : "JSON parser " + expected + " but the row parser " + actual;
    }

    private static String numberKind(Object numberType) {
        return switch ((XContentParser.NumberType) numberType) {
            case INT, LONG, BIG_INTEGER -> "integral";
            case FLOAT, DOUBLE, BIG_DECIMAL -> "floating";
        };
    }

    private static boolean isIntegral(Number number) {
        return number instanceof Byte || number instanceof Short || number instanceof Integer || number instanceof Long;
    }

    public void testSimpleFlatDocument() throws IOException {
        BytesReference source = new BytesArray("""
            {"title": "hello", "count": 42, "active": true}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "title");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("hello", parser.text());
                assertFieldName(parser, "count");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(42, parser.intValue());
                assertFieldName(parser, "active");
                assertToken(parser, Token.VALUE_BOOLEAN);
                assertTrue(parser.booleanValue());
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    public void testNestedObject() throws IOException {
        BytesReference source = new BytesArray("""
            {"user": {"name": "alice", "age": 30}, "status": "ok"}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "user");
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "name");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("alice", parser.text());
                assertFieldName(parser, "age");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(30, parser.intValue());
                assertToken(parser, Token.END_OBJECT); // close user
                assertFieldName(parser, "status");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("ok", parser.text());
                assertToken(parser, Token.END_OBJECT); // close root
                assertNull(parser.nextToken());
            }
        }
    }

    public void testAbsentColumnsSkipped() throws IOException {
        // First doc sets the schema with 3 columns; second doc only sets a and c (b is absent).
        BytesReference src0 = new BytesArray("""
            {"a": "val", "b": 1, "c": "end"}""");
        BytesReference src1 = new BytesArray("""
            {"a": "hello", "c": "world"}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(src0, src1), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(1))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "a");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("hello", parser.text());
                // b is absent in this doc, should be skipped
                assertFieldName(parser, "c");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("world", parser.text());
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    public void testExplicitNullEmitsValueNullToken() throws IOException {
        BytesReference source = new BytesArray("""
            {"a": "hello", "b": null, "c": "world"}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "a");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("hello", parser.text());
                assertFieldName(parser, "b");
                assertToken(parser, Token.VALUE_NULL);
                assertFieldName(parser, "c");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("world", parser.text());
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    public void testAllScalarTypes() throws IOException {
        BytesReference source = new BytesArray("""
            {"i": 123, "l": 9876543210, "f": 1.5, "d": 3.14, "s": "text", "bt": true, "bf": false}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);

                assertFieldName(parser, "i");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(XContentParser.NumberType.LONG, parser.numberType());
                assertEquals(123, parser.intValue());

                assertFieldName(parser, "l");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(XContentParser.NumberType.LONG, parser.numberType());
                assertEquals(9876543210L, parser.longValue());

                assertFieldName(parser, "f");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(XContentParser.NumberType.DOUBLE, parser.numberType());
                assertEquals(1.5f, parser.floatValue(), 0.001f);

                assertFieldName(parser, "d");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(XContentParser.NumberType.DOUBLE, parser.numberType());
                assertEquals(3.14, parser.doubleValue(), 0.001);

                assertFieldName(parser, "s");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("text", parser.text());

                assertFieldName(parser, "bt");
                assertToken(parser, Token.VALUE_BOOLEAN);
                assertTrue(parser.booleanValue());

                assertFieldName(parser, "bf");
                assertToken(parser, Token.VALUE_BOOLEAN);
                assertFalse(parser.booleanValue());

                assertToken(parser, Token.END_OBJECT);
            }
        }
    }

    /**
     * JSON carries binary values as base64 strings, which the encoder stores in a string column, so
     * {@link XContentParser#binaryValue()} must decode the string the way a JSON parser does.
     */
    public void testBase64StringBinaryValue() throws IOException {
        byte[] value = randomByteArrayOfLength(between(1, 64));
        BytesReference source = new BytesArray("{\"bin\": \"" + Base64.getEncoder().encodeToString(value) + "\"}");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "bin");
                assertToken(parser, Token.VALUE_STRING);
                assertArrayEquals(value, parser.binaryValue());
            }
        }
    }

    public void testMapParsing() throws IOException {
        BytesReference source = new BytesArray("""
            {"title": "test", "count": 7}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                Map<String, Object> map = parser.map();
                assertEquals("test", map.get("title"));
                assertEquals(7L, map.get("count"));
            }
        }
    }

    public void testArrayOfObjects() throws IOException {
        BytesReference source = new BytesArray("""
            {"items": [{"name": "a", "val": 1}, {"name": "b", "val": 2}]}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "items");
                assertToken(parser, Token.START_ARRAY);

                // First object
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "name");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("a", parser.text());
                assertFieldName(parser, "val");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(1, parser.intValue());
                assertToken(parser, Token.END_OBJECT);

                // Second object
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "name");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("b", parser.text());
                assertFieldName(parser, "val");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(2, parser.intValue());
                assertToken(parser, Token.END_OBJECT);

                assertToken(parser, Token.END_ARRAY);
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    public void testNestedArrays() throws IOException {
        BytesReference source = new BytesArray("""
            {"matrix": [[1, 2], [3, 4]]}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "matrix");
                assertToken(parser, Token.START_ARRAY);

                // First inner array
                assertToken(parser, Token.START_ARRAY);
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(1, parser.intValue());
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(2, parser.intValue());
                assertToken(parser, Token.END_ARRAY);

                // Second inner array
                assertToken(parser, Token.START_ARRAY);
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(3, parser.intValue());
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(4, parser.intValue());
                assertToken(parser, Token.END_ARRAY);

                assertToken(parser, Token.END_ARRAY);
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    public void testArrayOfObjectsWithNestedArrays() throws IOException {
        BytesReference source = new BytesArray("""
            {"items": [{"tags": ["x", "y"], "id": 1}]}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "items");
                assertToken(parser, Token.START_ARRAY);

                // Object with nested array
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "tags");
                assertToken(parser, Token.START_ARRAY);
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("x", parser.text());
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("y", parser.text());
                assertToken(parser, Token.END_ARRAY);
                assertFieldName(parser, "id");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(1, parser.intValue());
                assertToken(parser, Token.END_OBJECT);

                assertToken(parser, Token.END_ARRAY);
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testArrayOfObjectsMapParsing() throws IOException {
        // End-to-end: parse array-of-objects via map() and verify structure. The top-level scalar
        // widens to Long; inline key-value elements keep their packed integer type.
        BytesReference source = new BytesArray("""
            {"items": [{"k": "a", "v": 1}, {"k": "b", "v": 2}], "count": 2}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                Map<String, Object> map = parser.map();
                assertEquals(2L, map.get("count"));
                List<Map<String, Object>> items = (List<Map<String, Object>>) map.get("items");
                assertEquals(2, items.size());
                assertEquals("a", items.get(0).get("k"));
                assertEquals(1, items.get(0).get("v"));
                assertEquals("b", items.get(1).get("k"));
                assertEquals(2, items.get(1).get("v"));
            }
        }
    }

    public void testPositionAtLeafValueScalars() throws IOException {
        BytesReference source = new BytesArray("""
            {"ts": 1700000000000, "n": 7, "score": 3.5, "host": "alpha", "active": true, "maybe": null}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceSchema schema = batch.schema();
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(schema);
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                // Look up each leaf by path, then position the parser at it.
                int tsLeaf = schema.findLeaf("ts", 0);
                assertEquals(Token.VALUE_NUMBER, parser.positionAtLeafValue(tsLeaf));
                assertEquals(1_700_000_000_000L, parser.longValue());
                assertEquals(XContentParser.NumberType.LONG, parser.numberType());
                assertNull(parser.nextToken());

                int nLeaf = schema.findLeaf("n", 0);
                assertEquals(Token.VALUE_NUMBER, parser.positionAtLeafValue(nLeaf));
                assertEquals(7, parser.intValue());
                assertEquals(XContentParser.NumberType.LONG, parser.numberType());
                assertNull(parser.nextToken());

                int scoreLeaf = schema.findLeaf("score", 0);
                assertEquals(Token.VALUE_NUMBER, parser.positionAtLeafValue(scoreLeaf));
                assertEquals(3.5, parser.doubleValue(), 0.0);
                assertNull(parser.nextToken());

                int hostLeaf = schema.findLeaf("host", 0);
                assertEquals(Token.VALUE_STRING, parser.positionAtLeafValue(hostLeaf));
                assertEquals("alpha", parser.text());
                assertNull(parser.nextToken());

                int activeLeaf = schema.findLeaf("active", 0);
                assertEquals(Token.VALUE_BOOLEAN, parser.positionAtLeafValue(activeLeaf));
                assertTrue(parser.booleanValue());
                assertNull(parser.nextToken());

                int maybeLeaf = schema.findLeaf("maybe", 0);
                assertEquals(Token.VALUE_NULL, parser.positionAtLeafValue(maybeLeaf));
                assertNull(parser.nextToken());
            }
        }
    }

    public void testPositionAtLeafValueArrayAndKeyValue() throws IOException {
        // "items" is a top-level union-array leaf, "meta" is a top-level empty key-value leaf.
        BytesReference source = new BytesArray("""
            {"items": [1, "two", true], "meta": {}, "trailing": 9}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(source), XContentType.JSON)) {
            SourceSchema schema = batch.schema();
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(schema);
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                int itemsLeaf = schema.findLeaf("items", 0);
                assertEquals(Token.START_ARRAY, parser.positionAtLeafValue(itemsLeaf));
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(1, parser.intValue());
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("two", parser.text());
                assertToken(parser, Token.VALUE_BOOLEAN);
                assertTrue(parser.booleanValue());
                assertToken(parser, Token.END_ARRAY);
                assertNull(parser.nextToken());

                int metaLeaf = schema.findLeaf("meta", 0);
                assertEquals(Token.START_OBJECT, parser.positionAtLeafValue(metaLeaf));
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());

                // Repositioning to a scalar after compound walks must continue to work.
                int trailingLeaf = schema.findLeaf("trailing", 0);
                assertEquals(Token.VALUE_NUMBER, parser.positionAtLeafValue(trailingLeaf));
                assertEquals(9, parser.intValue());
                assertNull(parser.nextToken());
            }
        }
    }

    public void testAbsentObjectNodeIsNotEmitted() throws IOException {
        BytesReference withUser = new BytesArray("""
            {"title": "doc1", "user": {"name": "alice"}}""");
        BytesReference withoutUser = new BytesArray("""
            {"title": "doc2"}""");
        try (EscfBatch batch = EscfEncoder.encode(List.of(withUser, withoutUser), XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());

            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                // buildObjectNode adds object children before leaf children, so "user" appears before "title"
                assertFieldName(parser, "user");
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "name");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("alice", parser.text());
                assertToken(parser, Token.END_OBJECT);
                assertFieldName(parser, "title");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("doc1", parser.text());
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }

            // Row 1 has no user field: the parser must not emit "user": {} for the absent object.
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(1))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "title");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("doc2", parser.text());
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
        }
    }

    /**
     * A scalar double keeps the client's spelling for text consumers while numeric consumers see the value (#163555).
     * A double in a union column, here next to a string in another document, still renders canonically.
     */
    public void testScalarDoubleKeepsSpelling() throws IOException {
        List<BytesReference> sources = List.of(
            new BytesArray("{\"d\": -1.90, \"u\": 12.40}"),
            new BytesArray("{\"d\": 3.14, \"u\": \"x\"}")
        );
        try (EscfBatch batch = EscfEncoder.encode(sources, XContentType.JSON)) {
            SourceRowXContentParser.SchemaNode tree = SourceRowXContentParser.buildSchemaTree(batch.schema());
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(0))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "d");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals(XContentParser.NumberType.DOUBLE, parser.numberType());
                assertEquals("-1.90", parser.text());
                assertEquals("-1.90", parser.optimizedText().string());
                assertEquals(-1.9, parser.doubleValue(), 0.0);
                assertEquals(-1, parser.intValue());
                assertFieldName(parser, "u");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals("12.4", parser.text()); // a union row does not carry its spelling yet (#163555 part 2)
                assertEquals(12.4, parser.doubleValue(), 0.0);
                assertToken(parser, Token.END_OBJECT);
                assertNull(parser.nextToken());
            }
            try (SourceRowXContentParser parser = new SourceRowXContentParser(tree, batch.row(1))) {
                assertToken(parser, Token.START_OBJECT);
                assertFieldName(parser, "d");
                assertToken(parser, Token.VALUE_NUMBER);
                assertEquals("3.14", parser.text());
                assertFieldName(parser, "u");
                assertToken(parser, Token.VALUE_STRING);
                assertEquals("x", parser.text());
                assertToken(parser, Token.END_OBJECT);
            }
        }
    }

    private static void assertToken(SourceRowXContentParser parser, Token expected) throws IOException {
        assertEquals(expected, parser.nextToken());
    }

    private static void assertFieldName(SourceRowXContentParser parser, String expected) throws IOException {
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals(expected, parser.currentName());
    }
}
