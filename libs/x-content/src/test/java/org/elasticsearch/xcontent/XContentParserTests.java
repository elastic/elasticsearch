/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.xcontent;

import org.elasticsearch.common.CheckedSupplier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.internal.matchers.ThrowableMessageMatcher.hasMessage;

public class XContentParserTests extends ESTestCase {

    public void testFloat() throws IOException {
        final XContentType xContentType = randomFrom(XContentType.values());

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
            try (XContentParser parser = createParser(xContentType.xContent(), BytesReference.bytes(builder))) {
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
        XContentType xContentType = randomFrom(XContentType.values());

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

            try (XContentParser parser = createParser(xContentType.xContent(), BytesReference.bytes(builder))) {
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

    public void testReadList() throws IOException {
        assertThat(readList("{\"foo\": [\"bar\"]}"), contains("bar"));
        assertThat(readList("{\"foo\": [\"bar\",\"baz\"]}"), contains("bar", "baz"));
        assertThat(readList("{\"foo\": [1, 2, 3], \"bar\": 4}"), contains(1, 2, 3));
        assertThat(readList("{\"foo\": [{\"bar\":1},{\"baz\":2},{\"qux\":3}]}"), hasSize(3));
        assertThat(readList("{\"foo\": [null]}"), contains(nullValue()));
        assertThat(readList("{\"foo\": []}"), hasSize(0));
        assertThat(readList("{\"foo\": [1]}"), contains(1));
        assertThat(readList("{\"foo\": [1,2]}"), contains(1, 2));
        assertThat(readList("{\"foo\": [{},{},{},{}]}"), hasSize(4));
    }

    public void testReadListThrowsException() throws IOException {
        // Calling XContentParser.list() or listOrderedMap() to read a simple
        // value or object should throw an exception
        assertReadListThrowsException("{\"foo\": \"bar\"}");
        assertReadListThrowsException("{\"foo\": 1, \"bar\": 2}");
        assertReadListThrowsException("{\"foo\": {\"bar\":\"baz\"}}");
    }

    @SuppressWarnings("unchecked")
    private <T> List<T> readList(String source) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            XContentParser.Token token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.START_OBJECT));
            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("foo"));
            return (List<T>) (randomBoolean() ? parser.listOrderedMap() : parser.list());
        }
    }

    private void assertReadListThrowsException(String source) {
        try {
            readList(source);
            fail("should have thrown a parse exception");
        } catch (Exception e) {
            assertThat(e, instanceOf(XContentParseException.class));
            assertThat(e.getMessage(), containsString("Failed to parse list"));
        }
    }

    public void testReadMapStrings() throws IOException {
        Map<String, String> map = readMapStrings("""
            {"foo": {"kbar":"vbar"}}""");
        assertThat(map.get("kbar"), equalTo("vbar"));
        assertThat(map.size(), equalTo(1));
        map = readMapStrings("""
            {"foo": {"kbar":"vbar", "kbaz":"vbaz"}}""");
        assertThat(map.get("kbar"), equalTo("vbar"));
        assertThat(map.get("kbaz"), equalTo("vbaz"));
        assertThat(map.size(), equalTo(2));
        map = readMapStrings("{\"foo\": {}}");
        assertThat(map.size(), equalTo(0));
    }

    public void testMap() throws IOException {
        String source = """
            {
              "i": {
                "_doc": {
                  "f1": {
                    "type": "text",
                    "analyzer": "english"
                  },
                  "f2": {
                    "type": "object",
                    "properties": {
                      "sub1": {
                        "type": "keyword",
                        "foo": 17
                      }
                    }
                  }
                }
              }
            }""";
        Map<String, Object> f1 = new HashMap<>();
        f1.put("type", "text");
        f1.put("analyzer", "english");

        Map<String, Object> sub1 = new HashMap<>();
        sub1.put("type", "keyword");
        sub1.put("foo", 17);

        Map<String, Object> properties = new HashMap<>();
        properties.put("sub1", sub1);

        Map<String, Object> f2 = new HashMap<>();
        f2.put("type", "object");
        f2.put("properties", properties);

        Map<String, Object> doc = new HashMap<>();
        doc.put("f1", f1);
        doc.put("f2", f2);

        Map<String, Object> expected = new HashMap<>();
        expected.put("_doc", doc);

        Map<String, Object> i = new HashMap<>();
        i.put("i", expected);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            XContentParser.Token token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.START_OBJECT));
            Map<String, Object> map = parser.map();
            assertThat(map, equalTo(i));
        }
    }

    private Map<String, String> readMapStrings(String source) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, source)) {
            XContentParser.Token token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.START_OBJECT));
            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("foo"));
            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.START_OBJECT));
            return parser.mapStrings();
        }
    }

    public void testReadBooleansFailsForLenientBooleans() throws IOException {
        String falsy = randomFrom("\"off\"", "\"no\"", "\"0\"", "0");
        String truthy = randomFrom("\"on\"", "\"yes\"", "\"1\"", "1");

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"foo\": " + falsy + ", \"bar\": " + truthy + "}")) {
            XContentParser.Token token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.START_OBJECT));

            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("foo"));
            token = parser.nextToken();
            assertThat(token, in(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_NUMBER)));
            assertFalse(parser.isBooleanValue());
            if (token.equals(XContentParser.Token.VALUE_STRING)) {
                expectThrows(IllegalArgumentException.class, parser::booleanValue);
            } else {
                expectThrows(XContentParseException.class, parser::booleanValue);
            }

            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("bar"));
            token = parser.nextToken();
            assertThat(token, in(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_NUMBER)));
            assertFalse(parser.isBooleanValue());
            if (token.equals(XContentParser.Token.VALUE_STRING)) {
                expectThrows(IllegalArgumentException.class, parser::booleanValue);
            } else {
                expectThrows(XContentParseException.class, parser::booleanValue);
            }
        }
    }

    public void testReadBooleans() throws IOException {
        String falsy = randomFrom("\"false\"", "false");
        String truthy = randomFrom("\"true\"", "true");

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"foo\": " + falsy + ", \"bar\": " + truthy + "}")) {
            XContentParser.Token token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.START_OBJECT));
            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("foo"));
            token = parser.nextToken();
            assertThat(token, in(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_BOOLEAN)));
            assertTrue(parser.isBooleanValue());
            assertFalse(parser.booleanValue());

            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("bar"));
            token = parser.nextToken();
            assertThat(token, in(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_BOOLEAN)));
            assertTrue(parser.isBooleanValue());
            assertTrue(parser.booleanValue());
        }
    }

    public void testEmptyList() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder().startObject().startArray("some_array").endArray().endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("some_array", parser.currentName());
            if (random().nextBoolean()) {
                // sometimes read the start array token, sometimes not
                assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            }
            assertEquals(Collections.emptyList(), parser.list());
        }
    }

    public void testSimpleList() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("some_array")
            .value(1)
            .value(3)
            .value(0)
            .endArray()
            .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("some_array", parser.currentName());
            if (random().nextBoolean()) {
                // sometimes read the start array token, sometimes not
                assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            }
            assertEquals(Arrays.asList(1, 3, 0), parser.list());
        }
    }

    public void testNestedList() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("some_array")
            .startArray()
            .endArray()
            .startArray()
            .value(1)
            .value(3)
            .endArray()
            .startArray()
            .value(2)
            .endArray()
            .endArray()
            .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("some_array", parser.currentName());
            if (random().nextBoolean()) {
                // sometimes read the start array token, sometimes not
                assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            }
            assertEquals(Arrays.asList(Collections.<Integer>emptyList(), Arrays.asList(1, 3), Arrays.asList(2)), parser.list());
        }
    }

    public void testNestedMapInList() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .startArray("some_array")
            .startObject()
            .field("foo", "bar")
            .endObject()
            .startObject()
            .endObject()
            .endArray()
            .endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
            assertEquals("some_array", parser.currentName());
            if (random().nextBoolean()) {
                // sometimes read the start array token, sometimes not
                assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken());
            }
            assertEquals(Arrays.asList(singletonMap("foo", "bar"), emptyMap()), parser.list());
        }
    }

    public void testGenericMap() throws IOException {
        String content = """
            {
              "c": {
                "i": 3,
                "d": 0.3,
                "s": "ccc"
              },
              "a": {
                "i": 1,
                "d": 0.1,
                "s": "aaa"
              },
              "b": {
                "i": 2,
                "d": 0.2,
                "s": "bbb"
              }
            }""";
        SimpleStruct structA = new SimpleStruct(1, 0.1, "aaa");
        SimpleStruct structB = new SimpleStruct(2, 0.2, "bbb");
        SimpleStruct structC = new SimpleStruct(3, 0.3, "ccc");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            Map<String, SimpleStruct> actualMap = parser.map(HashMap::new, SimpleStruct::fromXContent);
            // Verify map contents, ignore the iteration order.
            assertThat(actualMap, equalTo(Map.of("a", structA, "b", structB, "c", structC)));
            assertThat(actualMap.values(), containsInAnyOrder(structA, structB, structC));
            assertNull(parser.nextToken());
        }
    }

    public void testGenericMapOrdered() throws IOException {
        String content = """
            {
              "c": {
                "i": 3,
                "d": 0.3,
                "s": "ccc"
              },
              "a": {
                "i": 1,
                "d": 0.1,
                "s": "aaa"
              },
              "b": {
                "i": 2,
                "d": 0.2,
                "s": "bbb"
              }
            }""";
        SimpleStruct structA = new SimpleStruct(1, 0.1, "aaa");
        SimpleStruct structB = new SimpleStruct(2, 0.2, "bbb");
        SimpleStruct structC = new SimpleStruct(3, 0.3, "ccc");
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            Map<String, SimpleStruct> actualMap = parser.map(LinkedHashMap::new, SimpleStruct::fromXContent);
            // Verify map contents, ignore the iteration order.
            assertThat(actualMap, equalTo(Map.of("a", structA, "b", structB, "c", structC)));
            // Verify that map's iteration order is the same as the order in which fields appear in JSON.
            assertThat(actualMap.values(), contains(structC, structA, structB));
            assertNull(parser.nextToken());
        }
    }

    public void testGenericMap_Failure_MapContainingUnparsableValue() throws IOException {
        String content = """
            {
              "a": {
                "i": 1,
                "d": 0.1,
                "s": "aaa"
              },
              "b": {
                "i": 2,
                "d": 0.2,
                "s": 666
              },
              "c": {
                "i": 3,
                "d": 0.3,
                "s": "ccc"
              }
            }""";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            XContentParseException exception = expectThrows(
                XContentParseException.class,
                () -> parser.map(HashMap::new, SimpleStruct::fromXContent)
            );
            assertThat(exception, hasMessage(containsString("s doesn't support values of type: VALUE_NUMBER")));
        }
    }

    public void testSubParserObject() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        int numberOfTokens;
        numberOfTokens = generateRandomObjectForMarking(builder);
        String content = Strings.toString(builder);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken()); // first field
            assertEquals("first_field", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken()); // foo
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken()); // marked field
            assertEquals("marked_field", parser.currentName());
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken()); // {
            XContentParser subParser = new XContentSubParser(parser);
            try {
                int tokensToSkip = randomInt(numberOfTokens - 1);
                for (int i = 0; i < tokensToSkip; i++) {
                    // Simulate incomplete parsing
                    assertNotNull(subParser.nextToken());
                }
                if (randomBoolean()) {
                    // And sometimes skipping children
                    subParser.skipChildren();
                }

            } finally {
                assertFalse(subParser.isClosed());
                subParser.close();
                assertTrue(subParser.isClosed());
            }
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken()); // last field
            assertEquals("last_field", parser.currentName());
            assertEquals(XContentParser.Token.VALUE_STRING, parser.nextToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
    }

    public void testFlatteningParserObject() throws IOException {
        String content = """
            {
              "parent": {
                "child1" : 1,
                "child2": {
                  "grandChild" : 1
                },
                "child3" : 1
              }
            }
            """;
        XContentParser parser = createParser(JsonXContent.jsonXContent, content);
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken());
        assertEquals("parent", parser.currentName());
        assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
        XContentParser subParser = new FlatteningXContentParser(parser, parser.currentName());
        assertEquals(XContentParser.Token.FIELD_NAME, subParser.nextToken());
        assertEquals("parent.child1", subParser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, subParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, subParser.nextToken());
        String secondChildName = subParser.currentName();
        assertEquals("parent.child2", secondChildName);
        assertEquals(XContentParser.Token.START_OBJECT, subParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, subParser.nextToken());
        assertEquals("grandChild", subParser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, subParser.nextToken());
        assertEquals(XContentParser.Token.END_OBJECT, subParser.nextToken());
        assertEquals(XContentParser.Token.FIELD_NAME, subParser.nextToken());
        assertEquals("parent.child3", subParser.currentName());
        assertEquals(XContentParser.Token.VALUE_NUMBER, subParser.nextToken());

    }

    public void testSubParserArray() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        int numberOfArrayElements = randomInt(10);
        builder.startObject();
        builder.field("array");
        builder.startArray();
        int numberOfTokens = 0;
        for (int i = 0; i < numberOfArrayElements; ++i) {
            numberOfTokens += generateRandomObject(builder, 0);
        }
        builder.endArray();
        builder.endObject();

        String content = Strings.toString(builder);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken()); // array field
            assertEquals("array", parser.currentName());
            assertEquals(XContentParser.Token.START_ARRAY, parser.nextToken()); // [
            XContentParser subParser = new XContentSubParser(parser);
            try {
                int tokensToSkip = randomInt(numberOfTokens);
                for (int i = 0; i < tokensToSkip; i++) {
                    // Simulate incomplete parsing
                    assertNotNull(subParser.nextToken());
                }
                if (randomBoolean()) {
                    // And sometimes skipping children
                    subParser.skipChildren();
                }

            } finally {
                assertFalse(subParser.isClosed());
                subParser.close();
                assertTrue(subParser.isClosed());
            }
            assertEquals(XContentParser.Token.END_ARRAY, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
    }

    public void testCreateSubParserAtAWrongPlace() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        generateRandomObjectForMarking(builder);
        String content = Strings.toString(builder);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            assertEquals(XContentParser.Token.FIELD_NAME, parser.nextToken()); // first field
            assertEquals("first_field", parser.currentName());
            IllegalStateException exception = expectThrows(IllegalStateException.class, () -> new XContentSubParser(parser));
            assertEquals("The sub parser has to be created on the start of an object or array", exception.getMessage());
        }
    }

    public void testCreateRootSubParser() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        int numberOfTokens = generateRandomObjectForMarking(builder);
        String content = Strings.toString(builder);

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, content)) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            try (XContentParser subParser = new XContentSubParser(parser)) {
                int tokensToSkip = randomInt(numberOfTokens + 3);
                for (int i = 0; i < tokensToSkip; i++) {
                    // Simulate incomplete parsing
                    assertNotNull(subParser.nextToken());
                }
            }
            assertNull(parser.nextToken());
        }

    }

    /**
     * Generates a random object {"first_field": "foo", "marked_field": {...random...}, "last_field": "bar}
     *
     * Returns the number of tokens in the marked field
     */
    private static int generateRandomObjectForMarking(XContentBuilder builder) throws IOException {
        builder.startObject().field("first_field", "foo").field("marked_field");
        int numberOfTokens = generateRandomObject(builder, 0);
        builder.field("last_field", "bar").endObject();
        return numberOfTokens;
    }

    public static int generateRandomObject(XContentBuilder builder, int level) throws IOException {
        int tokens = 2;
        builder.startObject();
        int numberOfElements = randomInt(5);
        for (int i = 0; i < numberOfElements; i++) {
            builder.field(randomAlphaOfLength(10) + "_" + i);
            tokens += generateRandomValue(builder, level + 1);
        }
        builder.endObject();
        return tokens;
    }

    private static int generateRandomValue(XContentBuilder builder, int level) throws IOException {
        @SuppressWarnings("unchecked")
        CheckedSupplier<Integer, IOException> fieldGenerator = randomFrom(() -> {
            builder.value(randomInt());
            return 1;
        }, () -> {
            builder.value(randomAlphaOfLength(10));
            return 1;
        }, () -> {
            builder.value(randomDouble());
            return 1;
        }, () -> {
            if (level < 3) {
                // don't need to go too deep
                return generateRandomObject(builder, level + 1);
            } else {
                builder.value(0);
                return 1;
            }
        }, () -> {
            if (level < 5) { // don't need to go too deep
                return generateRandomArray(builder, level);
            } else {
                builder.value(0);
                return 1;
            }
        });
        return fieldGenerator.get();
    }

    private static int generateRandomArray(XContentBuilder builder, int level) throws IOException {
        int tokens = 2;
        int arraySize = randomInt(3);
        builder.startArray();
        for (int i = 0; i < arraySize; i++) {
            tokens += generateRandomValue(builder, level + 1);
        }
        builder.endArray();
        return tokens;
    }

}
