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
        ACCESSORS.put("shortValue", XContentParser::shortValue);
        ACCESSORS.put("intValue", XContentParser::intValue);
        ACCESSORS.put("longValue", XContentParser::longValue);
        ACCESSORS.put("floatValue", XContentParser::floatValue);
        ACCESSORS.put("doubleValue", XContentParser::doubleValue);
        ACCESSORS.put("numberValue", XContentParser::numberValue);
        ACCESSORS.put("numberType", XContentParser::numberType);
    }

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
        assertAccessorsMatch(XContentType.JSON, sources);
    }

    private void assertAccessorsMatch(XContentType type, Map<String, BytesReference> sources) throws IOException {
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
                        // Position both on the value: START_OBJECT, FIELD_NAME, then the value token.
                        for (int i = 0; i < 3; i++) {
                            json.nextToken();
                            row.nextToken();
                        }
                        assertEquals("token for " + entry.getKey(), json.currentToken(), row.currentToken());
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

    private static void assertToken(SourceRowXContentParser parser, Token expected) throws IOException {
        assertEquals(expected, parser.nextToken());
    }

    private static void assertFieldName(SourceRowXContentParser parser, String expected) throws IOException {
        assertEquals(Token.FIELD_NAME, parser.nextToken());
        assertEquals(expected, parser.currentName());
    }
}
