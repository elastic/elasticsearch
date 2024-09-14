/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.xcontent;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class ObjectParserTests extends ESTestCase {

    public void testBasics() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"test\" : \"foo\", \"test_number\" : 2, \"test_array\": [1,2,3,4] }"
        );
        class TestStruct {
            public String test;
            int testNumber;
            List<Integer> ints = new ArrayList<>();

            public void setTestNumber(int testNumber) {
                this.testNumber = testNumber;
            }

            public void setInts(List<Integer> ints) {
                this.ints = ints;
            }
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        TestStruct s = new TestStruct();

        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("test"), ObjectParser.ValueType.STRING);
        objectParser.declareInt(TestStruct::setTestNumber, new ParseField("test_number"));
        objectParser.declareIntArray(TestStruct::setInts, new ParseField("test_array"));
        objectParser.parse(parser, s, null);
        assertEquals(s.test, "foo");
        assertEquals(s.testNumber, 2);
        assertEquals(s.ints, Arrays.asList(1, 2, 3, 4));
    }

    public void testNullDeclares() {
        ObjectParser<Void, Void> objectParser = new ObjectParser<>("foo");
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField(null, (r, c) -> null, new ParseField("test"), ObjectParser.ValueType.STRING)
        );
        assertEquals("[consumer] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField(
                (o, v) -> {},
                (ContextParser<Void, Object>) null,
                new ParseField("test"),
                ObjectParser.ValueType.STRING
            )
        );
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField(
                (o, v) -> {},
                (CheckedFunction<XContentParser, Object, IOException>) null,
                new ParseField("test"),
                ObjectParser.ValueType.STRING
            )
        );
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField((o, v) -> {}, (r, c) -> null, null, ObjectParser.ValueType.STRING)
        );
        assertEquals("[parseField] is required", e.getMessage());
        e = expectThrows(
            IllegalArgumentException.class,
            () -> objectParser.declareField((o, v) -> {}, (r, c) -> null, new ParseField("test"), null)
        );
        assertEquals("[type] is required", e.getMessage());
    }

    public void testObjectOrDefault() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"object\" : { \"test\": 2}}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser<>("foo", StaticTestStruct::new);
        objectParser.declareInt(StaticTestStruct::setTest, new ParseField("test"));
        objectParser.declareObjectOrDefault(StaticTestStruct::setObject, objectParser, StaticTestStruct::new, new ParseField("object"));
        StaticTestStruct s = objectParser.parse(parser, null);
        assertEquals(s.object.test, 2);
        parser = createParser(JsonXContent.jsonXContent, "{\"object\" : false }");
        s = objectParser.parse(parser, null);
        assertNull(s.object);

        parser = createParser(JsonXContent.jsonXContent, "{\"object\" : true }");
        s = objectParser.parse(parser, null);
        assertNotNull(s.object);
        assertEquals(s.object.test, 0);
    }

    /**
     * This test ensures we can use a classic pull-parsing parser
     * together with the object parser
     */
    public void testUseClassicPullParsingSubParser() throws IOException {
        class ClassicParser {
            URI parseURI(XContentParser parser) throws IOException {
                String fieldName = null;
                String host = "";
                int port = 0;
                XContentParser.Token token;
                while ((token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_STRING) {
                        if (fieldName.equals("host")) {
                            host = parser.text();
                        } else {
                            throw new IllegalStateException("boom");
                        }
                    } else if (token == XContentParser.Token.VALUE_NUMBER) {
                        if (fieldName.equals("port")) {
                            port = parser.intValue();
                        } else {
                            throw new IllegalStateException("boom");
                        }
                    }
                    parser.nextToken();
                }
                return URI.create(host + ":" + port);
            }
        }
        class Foo {
            public String name;
            public URI uri;

            public void setName(String name) {
                this.name = name;
            }

            public void setUri(URI uri) {
                this.uri = uri;
            }
        }

        class CustomParseContext {

            public final ClassicParser parser;

            CustomParseContext(ClassicParser parser) {
                this.parser = parser;
            }

            public URI parseURI(XContentParser xContentParser) {
                try {
                    return this.parser.parseURI(xContentParser);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"url" : { "host": "http://foobar", "port" : 80}, "name" : "foobarbaz"}""");
        ObjectParser<Foo, CustomParseContext> objectParser = new ObjectParser<>("foo");
        objectParser.declareString(Foo::setName, new ParseField("name"));
        objectParser.declareObjectOrDefault(Foo::setUri, (p, s) -> s.parseURI(p), () -> null, new ParseField("url"));
        Foo s = objectParser.parse(parser, new Foo(), new CustomParseContext(new ClassicParser()));
        assertEquals(s.uri.getHost(), "foobar");
        assertEquals(s.uri.getPort(), 80);
        assertEquals(s.name, "foobarbaz");
    }

    public void testExceptions() throws IOException {
        class TestStruct {
            public void setTest(int test) {}
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("the_parser");
        TestStruct s = new TestStruct();
        objectParser.declareInt(TestStruct::setTest, new ParseField("test"));

        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"test\" : \"foo\"}");
            XContentParseException ex = expectThrows(XContentParseException.class, () -> objectParser.parse(parser, s, null));
            assertThat(ex.getMessage(), containsString("[the_parser] failed to parse field [test]"));
            assertTrue(ex.getCause() instanceof NumberFormatException);
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"not_supported_field\" : \"foo\"}");
            XContentParseException ex = expectThrows(XContentParseException.class, () -> objectParser.parse(parser, s, null));
            assertEquals(ex.getMessage(), "[1:2] [the_parser] unknown field [not_supported_field]");
        }
    }

    public void testDeprecationWarnings() throws IOException {
        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        TestStruct s = new TestStruct();
        XContentParser parser = createParser(XContentType.JSON.xContent(), "{\"old_test\" : \"foo\"}");
        objectParser.declareField((i, v, c) -> v.test = i.text(), new ParseField("test", "old_test"), ObjectParser.ValueType.STRING);
        objectParser.parse(parser, s, null);
        assertEquals("foo", s.test);
        assertWarnings(
            false,
            new DeprecationWarning(Level.WARN, "[foo][1:15] Deprecated field [old_test] used, " + "expected [test] instead")
        );
    }

    public void testFailOnValueType() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"numeric_value\" : false}");
        class TestStruct {
            @SuppressWarnings("unused")
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        TestStruct s = new TestStruct();

        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("numeric_value"), ObjectParser.ValueType.FLOAT);
        Exception e = expectThrows(XContentParseException.class, () -> objectParser.parse(parser, s, null));
        assertThat(e.getMessage(), containsString("[foo] numeric_value doesn't support values of type: VALUE_BOOLEAN"));
    }

    public void testParseNested() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test\" : 1, \"object\" : { \"test\": 2}}");
        class TestStruct {
            public int test;
            TestStruct object;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        TestStruct s = new TestStruct();
        s.object = new TestStruct();
        objectParser.declareField((i, c, x) -> c.test = i.intValue(), new ParseField("test"), ValueType.INT);
        objectParser.declareField((i, c, x) -> objectParser.parse(parser, c.object, null), new ParseField("object"), ValueType.OBJECT);
        objectParser.parse(parser, s, null);
        assertEquals(s.test, 1);
        assertEquals(s.object.test, 2);
    }

    public void testParseNestedShortcut() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test\" : 1, \"object\" : { \"test\": 2}}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser<>("foo", StaticTestStruct::new);
        objectParser.declareInt(StaticTestStruct::setTest, new ParseField("test"));
        objectParser.declareObject(StaticTestStruct::setObject, objectParser, new ParseField("object"));
        StaticTestStruct s = objectParser.parse(parser, null);
        assertEquals(s.test, 1);
        assertEquals(s.object.test, 2);
    }

    public void testEmptyObject() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"object\" : {}}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser<>("foo", StaticTestStruct::new);
        objectParser.declareObject(StaticTestStruct::setObject, objectParser, new ParseField("object"));
        StaticTestStruct s = objectParser.parse(parser, null);
        assertNotNull(s.object);
    }

    public void testObjectOrNullWhenNull() throws IOException {
        StaticTestStruct nullMarker = new StaticTestStruct();
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"object\" : null}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser<>("foo", StaticTestStruct::new);
        objectParser.declareObjectOrNull(StaticTestStruct::setObject, objectParser, nullMarker, new ParseField("object"));
        StaticTestStruct s = objectParser.parse(parser, null);
        assertThat(s.object, equalTo(nullMarker));
    }

    public void testObjectOrNullWhenNonNull() throws IOException {
        StaticTestStruct nullMarker = new StaticTestStruct();
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"object\" : {}}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser<>("foo", StaticTestStruct::new);
        objectParser.declareObjectOrNull(StaticTestStruct::setObject, objectParser, nullMarker, new ParseField("object"));
        StaticTestStruct s = objectParser.parse(parser, null);
        assertThat(s.object, not(nullValue()));
    }

    public void testEmptyObjectInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"object_array\" : [{}]}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser<>("foo", StaticTestStruct::new);
        objectParser.declareObjectArray(StaticTestStruct::setObjectArray, objectParser, new ParseField("object_array"));
        StaticTestStruct s = objectParser.parse(parser, null);
        assertNotNull(s.objectArray);
    }

    static class StaticTestStruct {
        int test;
        StaticTestStruct object;
        List<StaticTestStruct> objectArray;

        public void setTest(int test) {
            this.test = test;
        }

        public void setObject(StaticTestStruct object) {
            this.object = object;
        }

        public void setObjectArray(List<StaticTestStruct> objectArray) {
            this.objectArray = objectArray;
        }
    }

    enum TestEnum {
        FOO,
        BAR
    }

    public void testParseEnumFromString() throws IOException {
        class TestStruct {
            public TestEnum test;

            public void set(TestEnum value) {
                test = value;
            }
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test\" : \"FOO\" }");
            ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
            objectParser.declareString((struct, value) -> struct.set(TestEnum.valueOf(value)), new ParseField("test"));
            TestStruct s = objectParser.parse(parser, new TestStruct(), null);
            assertEquals(s.test, TestEnum.FOO);
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test\" : \"FOO\" }");
            ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
            objectParser.declareString((struct, value) -> struct.set(value), TestEnum::valueOf, new ParseField("test"));
            TestStruct s = objectParser.parse(parser, new TestStruct(), null);
            assertEquals(s.test, TestEnum.FOO);
        }
    }

    public void testAllVariants() throws IOException {
        double expectedNullableDouble;
        int expectedNullableInt;

        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        builder.field("int_field", randomBoolean() ? "1" : 1);
        if (randomBoolean()) {
            builder.nullField("nullable_int_field");
            expectedNullableInt = -1;
        } else {
            expectedNullableInt = randomInt();
            builder.field("nullable_int_field", expectedNullableInt);
        }
        if (randomBoolean()) {
            builder.array("int_array_field", randomBoolean() ? "1" : 1);
        } else {
            builder.field("int_array_field", randomBoolean() ? "1" : 1);
        }
        builder.field("double_field", randomBoolean() ? "2.1" : 2.1d);
        if (randomBoolean()) {
            builder.nullField("nullable_double_field");
            expectedNullableDouble = Double.NaN;
        } else {
            expectedNullableDouble = randomDouble();
            builder.field("nullable_double_field", expectedNullableDouble);
        }
        if (randomBoolean()) {
            builder.array("double_array_field", randomBoolean() ? "2.1" : 2.1d);
        } else {
            builder.field("double_array_field", randomBoolean() ? "2.1" : 2.1d);
        }
        builder.field("float_field", randomBoolean() ? "3.1" : 3.1f);
        if (randomBoolean()) {
            builder.array("float_array_field", randomBoolean() ? "3.1" : 3.1);
        } else {
            builder.field("float_array_field", randomBoolean() ? "3.1" : 3.1);
        }
        builder.field("long_field", randomBoolean() ? "4" : 4);
        if (randomBoolean()) {
            builder.array("long_array_field", randomBoolean() ? "4" : 4);
        } else {
            builder.field("long_array_field", randomBoolean() ? "4" : 4);
        }
        builder.field("string_field", "5");
        if (randomBoolean()) {
            builder.array("string_array_field", "5");
        } else {
            builder.field("string_array_field", "5");
        }
        boolean nullValue = randomBoolean();
        if (randomBoolean()) {
            builder.field("boolean_field", nullValue);
        } else {
            builder.field("boolean_field", Boolean.toString(nullValue));
        }
        builder.field("string_or_null", nullValue ? null : "5");
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.toString(builder));
        class TestStruct {
            int int_field;
            int nullableIntField;
            long long_field;
            float float_field;
            double double_field;
            double nullableDoubleField;
            String string_field;
            List<Integer> int_array_field;
            List<Long> long_array_field;
            List<Float> float_array_field;
            List<Double> double_array_field;
            List<String> string_array_field;
            boolean null_value;
            String string_or_null = "adsfsa";

            public void setInt_field(int int_field) {
                this.int_field = int_field;
            }

            public void setNullableIntField(int nullableIntField) {
                this.nullableIntField = nullableIntField;
            }

            public void setLong_field(long long_field) {
                this.long_field = long_field;
            }

            public void setFloat_field(float float_field) {
                this.float_field = float_field;
            }

            public void setDouble_field(double double_field) {
                this.double_field = double_field;
            }

            public void setNullableDoubleField(double nullableDoubleField) {
                this.nullableDoubleField = nullableDoubleField;
            }

            public void setString_field(String string_field) {
                this.string_field = string_field;
            }

            public void setInt_array_field(List<Integer> int_array_field) {
                this.int_array_field = int_array_field;
            }

            public void setLong_array_field(List<Long> long_array_field) {
                this.long_array_field = long_array_field;
            }

            public void setFloat_array_field(List<Float> float_array_field) {
                this.float_array_field = float_array_field;
            }

            public void setDouble_array_field(List<Double> double_array_field) {
                this.double_array_field = double_array_field;
            }

            public void setString_array_field(List<String> string_array_field) {
                this.string_array_field = string_array_field;
            }

            public void setNull_value(boolean null_value) {
                this.null_value = null_value;
            }

            public void setString_or_null(String string_or_null) {
                this.string_or_null = string_or_null;
            }
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        objectParser.declareInt(TestStruct::setInt_field, new ParseField("int_field"));
        objectParser.declareIntOrNull(TestStruct::setNullableIntField, -1, new ParseField("nullable_int_field"));
        objectParser.declareIntArray(TestStruct::setInt_array_field, new ParseField("int_array_field"));
        objectParser.declareLong(TestStruct::setLong_field, new ParseField("long_field"));
        objectParser.declareLongArray(TestStruct::setLong_array_field, new ParseField("long_array_field"));
        objectParser.declareDouble(TestStruct::setDouble_field, new ParseField("double_field"));
        objectParser.declareDoubleOrNull(TestStruct::setNullableDoubleField, Double.NaN, new ParseField("nullable_double_field"));
        objectParser.declareDoubleArray(TestStruct::setDouble_array_field, new ParseField("double_array_field"));
        objectParser.declareFloat(TestStruct::setFloat_field, new ParseField("float_field"));
        objectParser.declareFloatArray(TestStruct::setFloat_array_field, new ParseField("float_array_field"));
        objectParser.declareString(TestStruct::setString_field, new ParseField("string_field"));
        objectParser.declareStringArray(TestStruct::setString_array_field, new ParseField("string_array_field"));

        objectParser.declareStringOrNull(TestStruct::setString_or_null, new ParseField("string_or_null"));
        objectParser.declareBoolean(TestStruct::setNull_value, new ParseField("boolean_field"));
        TestStruct parse = objectParser.parse(parser, new TestStruct(), null);
        assertArrayEquals(parse.double_array_field.toArray(), Collections.singletonList(2.1d).toArray());
        assertEquals(parse.double_field, 2.1d, 0.0d);
        assertThat(parse.nullableDoubleField, equalTo(expectedNullableDouble));

        assertArrayEquals(parse.long_array_field.toArray(), Collections.singletonList(4L).toArray());
        assertEquals(parse.long_field, 4L);

        assertArrayEquals(parse.string_array_field.toArray(), Collections.singletonList("5").toArray());
        assertEquals(parse.string_field, "5");

        assertArrayEquals(parse.int_array_field.toArray(), Collections.singletonList(1).toArray());
        assertEquals(parse.int_field, 1);
        assertThat(parse.nullableIntField, equalTo(expectedNullableInt));

        assertArrayEquals(parse.float_array_field.toArray(), Collections.singletonList(3.1f).toArray());
        assertEquals(parse.float_field, 3.1f, 0.0f);

        assertEquals(nullValue, parse.null_value);
        if (nullValue) {
            assertNull(parse.string_or_null);
        } else {
            assertEquals(parse.string_field, "5");
        }
    }

    public void testParseNamedObject() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "named": {
                "a": {
                  "foo": 11
                }
              },
              "bar": "baz"
            }""");
        NamedObjectHolder h = NamedObjectHolder.PARSER.apply(parser, null);
        assertEquals("a", h.named.name);
        assertEquals(11, h.named.foo);
        assertEquals("baz", h.bar);
    }

    public void testParseNamedObjectUnexpectedArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"named": [ "a": {"foo" : 11} }]""");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] named doesn't support values of type: START_ARRAY"));
    }

    public void testParseNamedObjects() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": { \"a\": {} }}");
        NamedObjectsHolder h = NamedObjectsHolder.PARSER.apply(parser, null);
        assertThat(h.named, hasSize(1));
        assertEquals("a", h.named.get(0).name);
        assertFalse(h.namedSuppliedInOrder);
    }

    public void testParseNamedObjectsInOrder() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ {\"a\": {}} ] }");
        NamedObjectsHolder h = NamedObjectsHolder.PARSER.apply(parser, null);
        assertThat(h.named, hasSize(1));
        assertEquals("a", h.named.get(0).name);
        assertTrue(h.namedSuppliedInOrder);
    }

    public void testParseNamedObjectsTwoFieldsInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {"named": [ {"a": {}, "b": {}}]}""");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectsHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_objects_holder] failed to parse field [named]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectsNoFieldsInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ {} ]}");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectsHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_objects_holder] failed to parse field [named]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectsJunkInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ \"junk\" ] }");
        XContentParseException e = expectThrows(XContentParseException.class, () -> NamedObjectsHolder.PARSER.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_objects_holder] failed to parse field [named]"));
        assertThat(
            e.getCause().getMessage(),
            containsString(
                "[named] can be a single object with any number of fields "
                    + "or an array where each entry is an object with a single field"
            )
        );
    }

    public void testParseNamedObjectsInOrderNotSupported() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"named\": [ {\"a\": {}} ] }");

        // Create our own parser for this test so we can disable support for the "ordered" mode specified by the array above
        ObjectParser<NamedObjectsHolder, Void> objectParser = new ObjectParser<>("named_object_holder", NamedObjectsHolder::new);
        objectParser.declareNamedObjects(NamedObjectsHolder::setNamed, NamedObject.PARSER, new ParseField("named"));

        // Now firing the xml through it fails
        XContentParseException e = expectThrows(XContentParseException.class, () -> objectParser.apply(parser, null));
        assertThat(e.getMessage(), containsString("[named_object_holder] failed to parse field [named]"));
        assertEquals("[named] doesn't support arrays. Use a single object with multiple fields.", e.getCause().getMessage());
    }

    public void testIgnoreUnknownFields() throws IOException {
        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        {
            b.field("test", "foo");
            b.field("junk", 2);
        }
        b.endObject();
        b = shuffleXContent(b);
        b.flush();
        byte[] bytes = ((ByteArrayOutputStream) b.getOutputStream()).toByteArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);

        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo", true, null);
        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("test"), ObjectParser.ValueType.STRING);
        TestStruct s = objectParser.parse(parser, new TestStruct(), null);
        assertEquals(s.test, "foo");
    }

    public void testIgnoreUnknownObjects() throws IOException {
        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        {
            b.field("test", "foo");
            b.startObject("junk");
            {
                b.field("really", "junk");
            }
            b.endObject();
        }
        b.endObject();
        b = shuffleXContent(b);
        b.flush();
        byte[] bytes = ((ByteArrayOutputStream) b.getOutputStream()).toByteArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);

        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo", true, null);
        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("test"), ObjectParser.ValueType.STRING);
        TestStruct s = objectParser.parse(parser, new TestStruct(), null);
        assertEquals(s.test, "foo");
    }

    public void testIgnoreUnknownArrays() throws IOException {
        XContentBuilder b = XContentBuilder.builder(XContentType.JSON.xContent());
        b.startObject();
        {
            b.field("test", "foo");
            b.startArray("junk");
            {
                b.startObject();
                {
                    b.field("really", "junk");
                }
                b.endObject();
            }
            b.endArray();
        }
        b.endObject();
        b = shuffleXContent(b);
        b.flush();
        byte[] bytes = ((ByteArrayOutputStream) b.getOutputStream()).toByteArray();
        XContentParser parser = createParser(JsonXContent.jsonXContent, bytes);
        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo", true, null);
        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("test"), ObjectParser.ValueType.STRING);
        TestStruct s = objectParser.parse(parser, new TestStruct(), null);
        assertEquals(s.test, "foo");
    }

    public void testArraysOfGenericValues() throws IOException {
        XContentParser parser = createParser(
            JsonXContent.jsonXContent,
            "{ \"test_array\": [ 1, null, \"3\", 4.2], \"int_array\":  [ 1, 2, 3] }"
        );
        class TestStruct {
            List<Object> testArray = new ArrayList<>();

            List<Integer> ints = new ArrayList<>();

            public void setInts(List<Integer> ints) {
                this.ints = ints;
            }

            public void setArray(List<Object> array) {
                this.testArray = array;
            }
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        TestStruct s = new TestStruct();

        objectParser.declareFieldArray(
            TestStruct::setArray,
            (p, c) -> XContentParserUtils.parseFieldsValue(p),
            new ParseField("test_array"),
            ValueType.VALUE_ARRAY
        );
        objectParser.declareIntArray(TestStruct::setInts, new ParseField("int_array"));
        objectParser.parse(parser, s, null);
        assertEquals(s.testArray, Arrays.asList(1, null, "3", 4.2));
        assertEquals(s.ints, Arrays.asList(1, 2, 3));

        parser = createParser(JsonXContent.jsonXContent, "{\"test_array\": 42}");
        s = new TestStruct();
        objectParser.parse(parser, s, null);
        assertEquals(s.testArray, Collections.singletonList(42));

        parser = createParser(JsonXContent.jsonXContent, "{\"test_array\": [null]}");
        s = new TestStruct();
        objectParser.parse(parser, s, null);
        assertThat(s.testArray, hasSize(1));
        assertNull(s.testArray.get(0));

        parser = createParser(JsonXContent.jsonXContent, "{\"test_array\": null}");
        s = new TestStruct();
        objectParser.parse(parser, s, null);
        assertThat(s.testArray, hasSize(1));
        assertNull(s.testArray.get(0));

        // Make sure that we didn't break the null handling in arrays that shouldn't support nulls
        XContentParser parser2 = createParser(JsonXContent.jsonXContent, "{\"int_array\": [1, null, 3]}");
        TestStruct s2 = new TestStruct();
        XContentParseException ex = expectThrows(XContentParseException.class, () -> objectParser.parse(parser2, s2, null));
        assertThat(ex.getMessage(), containsString("[foo] failed to parse field [int_array]"));
    }

    public void testNoopDeclareObject() throws IOException {
        ObjectParser<AtomicReference<String>, Void> parser = new ObjectParser<>("noopy", AtomicReference::new);
        parser.declareString(AtomicReference::set, new ParseField("body"));
        parser.declareObject((a, b) -> {}, (p, c) -> null, new ParseField("noop"));

        assertEquals("i", parser.parse(createParser(JsonXContent.jsonXContent, "{\"body\": \"i\"}"), null).get());
        Exception garbageException = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(createParser(JsonXContent.jsonXContent, """
                {"noop": {"garbage": "shouldn't"}}
                """), null)
        );
        assertEquals("parser for [noop] did not end on END_OBJECT", garbageException.getMessage());
        Exception sneakyException = expectThrows(IllegalStateException.class, () -> parser.parse(createParser(JsonXContent.jsonXContent, """
            {"noop": {"body": "shouldn't"}}
            """), null));
        assertEquals("parser for [noop] did not end on END_OBJECT", sneakyException.getMessage());
    }

    public void testNoopDeclareField() throws IOException {
        ObjectParser<AtomicReference<String>, Void> parser = new ObjectParser<>("noopy", AtomicReference::new);
        parser.declareString(AtomicReference::set, new ParseField("body"));
        parser.declareField((a, b) -> {}, (p, c) -> null, new ParseField("noop"), ValueType.STRING_ARRAY);

        assertEquals("i", parser.parse(createParser(JsonXContent.jsonXContent, "{\"body\": \"i\"}"), null).get());
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> parser.parse(createParser(JsonXContent.jsonXContent, "{\"noop\": [\"ignored\"]}"), null)
        );
        assertEquals("parser for [noop] did not end on END_ARRAY", e.getMessage());
    }

    public void testNoopDeclareObjectArray() {
        ObjectParser<AtomicReference<String>, Void> parser = new ObjectParser<>("noopy", AtomicReference::new);
        parser.declareString(AtomicReference::set, new ParseField("body"));
        parser.declareObjectArray((a, b) -> {}, (p, c) -> null, new ParseField("noop"));

        XContentParseException garbageError = expectThrows(
            XContentParseException.class,
            () -> parser.parse(createParser(JsonXContent.jsonXContent, """
                {"noop": [{"garbage": "shouldn't"}}]
                """), null)
        );
        assertEquals("expected value but got [FIELD_NAME]", garbageError.getCause().getMessage());
        XContentParseException sneakyError = expectThrows(
            XContentParseException.class,
            () -> parser.parse(createParser(JsonXContent.jsonXContent, """
                {"noop": [{"body": "shouldn't"}}]
                """), null)
        );
        assertEquals("expected value but got [FIELD_NAME]", sneakyError.getCause().getMessage());
    }

    // singular
    static class NamedObjectHolder {
        public static final ObjectParser<NamedObjectHolder, Void> PARSER = new ObjectParser<>(
            "named_object_holder",
            NamedObjectHolder::new
        );
        static {
            PARSER.declareNamedObject(NamedObjectHolder::setNamed, NamedObject.PARSER, new ParseField("named"));
            PARSER.declareString(NamedObjectHolder::setBar, new ParseField("bar"));
        }

        private NamedObject named;
        private String bar;

        public void setNamed(NamedObject named) {
            this.named = named;
        }

        public void setBar(String bar) {
            this.bar = bar;
        }
    }

    // plural
    static class NamedObjectsHolder {
        public static final ObjectParser<NamedObjectsHolder, Void> PARSER = new ObjectParser<>(
            "named_objects_holder",
            NamedObjectsHolder::new
        );
        static {
            PARSER.declareNamedObjects(
                NamedObjectsHolder::setNamed,
                NamedObject.PARSER,
                NamedObjectsHolder::keepNamedInOrder,
                new ParseField("named")
            );
        }

        private List<NamedObject> named;
        private boolean namedSuppliedInOrder = false;

        public void setNamed(List<NamedObject> named) {
            this.named = named;
        }

        public void keepNamedInOrder() {
            namedSuppliedInOrder = true;
        }
    }

    public static class NamedObject {
        public static final NamedObjectParser<NamedObject, Void> PARSER;
        static {
            ObjectParser<NamedObject, Void> parser = new ObjectParser<>("named");
            parser.declareInt(NamedObject::setFoo, new ParseField("foo"));
            PARSER = (XContentParser p, Void v, String name) -> parser.parse(p, new NamedObject(name), null);
        }

        final String name;
        int foo;

        public NamedObject(String name) {
            this.name = name;
        }

        public void setFoo(int foo) {
            this.foo = foo;
        }
    }

    private static class ObjectWithArbitraryFields {
        String name;
        Map<String, Object> fields = new HashMap<>();

        void setField(String key, Object value) {
            fields.put(key, value);
        }

        void setName(String name) {
            this.name = name;
        }
    }

    public void testConsumeUnknownFields() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, """
            {
              "test" : "foo",
              "test_number" : 2,
              "name" : "geoff",
              "test_boolean" : true,
              "test_null" : null,
              "test_array":  [1,2,3,4],
              "test_nested": { "field" : "value", "field2" : [ "list1", "list2" ] }
            }""");
        ObjectParser<ObjectWithArbitraryFields, Void> op = new ObjectParser<>(
            "unknown",
            ObjectWithArbitraryFields::setField,
            ObjectWithArbitraryFields::new
        );
        op.declareString(ObjectWithArbitraryFields::setName, new ParseField("name"));

        ObjectWithArbitraryFields o = op.parse(parser, null);
        assertEquals("geoff", o.name);
        assertEquals(6, o.fields.size());
        assertEquals("foo", o.fields.get("test"));
        assertEquals(2, o.fields.get("test_number"));
        assertEquals(true, o.fields.get("test_boolean"));
        assertNull(o.fields.get("test_null"));
        assertEquals(List.of(1, 2, 3, 4), o.fields.get("test_array"));
        assertEquals(Map.of("field", "value", "field2", List.of("list1", "list2")), o.fields.get("test_nested"));
    }

    public void testRequiredFieldSet() throws IOException {
        class TestStruct {
            private Long a;
            private Long b;

            private void setA(long value) {
                this.a = value;
            }

            private void setB(long value) {
                this.b = value;
            }
        }

        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"a\": \"123\"}");
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo", true, TestStruct::new);
        objectParser.declareLong(TestStruct::setA, new ParseField("a"));
        objectParser.declareLong(TestStruct::setB, new ParseField("b"));
        objectParser.declareRequiredFieldSet(new String[] { "a", "b" });

        TestStruct obj = objectParser.apply(parser, null);
        assertThat(obj.a, equalTo(123L));
        assertThat(obj.b, nullValue());

        parser = createParser(JsonXContent.jsonXContent, "{\"b\": \"123\"}");
        objectParser = new ObjectParser<>("foo", true, TestStruct::new);
        objectParser.declareLong(TestStruct::setA, new ParseField("a"));
        objectParser.declareLong(TestStruct::setB, new ParseField("b"));
        objectParser.declareRequiredFieldSet(new String[] { "a", "b" });

        obj = objectParser.apply(parser, null);
        assertThat(obj.a, nullValue());
        assertThat(obj.b, equalTo(123L));

        parser = createParser(JsonXContent.jsonXContent, """
            {"a": "123", "b": "456"}""");
        objectParser = new ObjectParser<>("foo", true, TestStruct::new);
        objectParser.declareLong(TestStruct::setA, new ParseField("a"));
        objectParser.declareLong(TestStruct::setB, new ParseField("b"));
        objectParser.declareRequiredFieldSet(new String[] { "a", "b" });

        obj = objectParser.apply(parser, null);
        assertThat(obj.a, equalTo(123L));
        assertThat(obj.b, equalTo(456L));
    }

    private static class TestStruct {
        private Long a;
        private Long b;
        private Long c;
        private Long d;

        private void setA(long value) {
            this.a = value;
        }

        private void setB(long value) {
            this.b = value;
        }

        private void setC(long value) {
            this.c = value;
        }

        private void setD(long value) {
            this.d = value;
        }
    }

    public void testMultipleRequiredFieldSet() throws IOException {

        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"unrelated\": \"123\"}");
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo", true, TestStruct::new);
        objectParser.declareLong(TestStruct::setA, new ParseField("a"));
        objectParser.declareLong(TestStruct::setB, new ParseField("b"));
        objectParser.declareLong(TestStruct::setC, new ParseField("c"));
        objectParser.declareLong(TestStruct::setD, new ParseField("d"));
        objectParser.declareRequiredFieldSet(new String[] { "a", "b" });
        objectParser.declareRequiredFieldSet(new String[] { "c", "d" });

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> objectParser.apply(parser, null));
        assertThat(
            e.getMessage(),
            equalTo("Required one of fields [a, b], but none were specified. " + "Required one of fields [c, d], but none were specified. ")
        );
    }

    public void testExclusiveFieldSet() throws IOException {

        XContentParser goodA = createParser(JsonXContent.jsonXContent, "{\"a\" : 1, \"c\" : 4}");
        XContentParser bad = createParser(JsonXContent.jsonXContent, "{\"a\" : 1, \"b\" : 2}");
        XContentParser badmulti = createParser(JsonXContent.jsonXContent, "{\"a\" : 1, \"b\" : 2, \"c\" : 3, \"d\" : 4 }");

        ObjectParser<TestStruct, Void> parser = new ObjectParser<>("foo", TestStruct::new);
        parser.declareLong(TestStruct::setA, new ParseField("a"));
        parser.declareLong(TestStruct::setB, new ParseField("b"));
        parser.declareLong(TestStruct::setC, new ParseField("c"));
        parser.declareLong(TestStruct::setD, new ParseField("d"));
        parser.declareExclusiveFieldSet("a", "b");
        parser.declareExclusiveFieldSet("c", "d");

        TestStruct actualA = parser.parse(goodA, null);
        assertThat(actualA.a, equalTo(1L));
        assertThat(actualA.c, equalTo(4L));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parser.parse(bad, null));
        assertThat(e.getMessage(), containsString("The following fields are not allowed together: [a, b]"));

        e = expectThrows(IllegalArgumentException.class, () -> parser.parse(badmulti, null));
        assertThat(e.getMessage(), containsString("allowed together: [a, b] The following fields are not allowed together: [c, d]"));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(Object.class, new ParseField("str"), p -> p.text()),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("int"), p -> p.intValue()),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("float"), p -> p.floatValue()),
                new NamedXContentRegistry.Entry(Object.class, new ParseField("bool"), p -> p.booleanValue())
            )
        );
    }

    private static class TopLevelNamedXConent {
        public static final ObjectParser<TopLevelNamedXConent, Void> PARSER = new ObjectParser<>(
            "test",
            Object.class,
            TopLevelNamedXConent::setNamed,
            TopLevelNamedXConent::new
        );

        Object named;

        void setNamed(Object named) {
            if (this.named != null) {
                throw new IllegalArgumentException("Only one [named] allowed!");
            }
            this.named = named;
        }
    }

    public void testTopLevelNamedXContent() throws IOException {
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"str\": \"foo\"}");
            TopLevelNamedXConent o = TopLevelNamedXConent.PARSER.parse(parser, null);
            assertEquals("foo", o.named);
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"int\": 1}");
            TopLevelNamedXConent o = TopLevelNamedXConent.PARSER.parse(parser, null);
            assertEquals(1, o.named);
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"float\": 4.0}");
            TopLevelNamedXConent o = TopLevelNamedXConent.PARSER.parse(parser, null);
            assertEquals(4.0F, o.named);
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"bool\": false}");
            TopLevelNamedXConent o = TopLevelNamedXConent.PARSER.parse(parser, null);
            assertEquals(false, o.named);
        }
        {
            XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"not_supported_field\" : \"foo\"}");
            XContentParseException ex = expectThrows(XContentParseException.class, () -> TopLevelNamedXConent.PARSER.parse(parser, null));
            assertEquals("[1:2] [test] unknown field [not_supported_field]", ex.getMessage());
            NamedObjectNotFoundException cause = (NamedObjectNotFoundException) ex.getCause();
            assertThat(cause.getCandidates(), containsInAnyOrder("str", "int", "float", "bool"));
        }
    }

    public void testContextBuilder() throws IOException {
        ObjectParser<AtomicReference<String>, String> parser = ObjectParser.fromBuilder("test", AtomicReference::new);
        String context = randomAlphaOfLength(5);
        AtomicReference<String> parsed = parser.parse(createParser(JsonXContent.jsonXContent, "{}"), context);
        assertThat(parsed.get(), equalTo(context));
    }

    public static class StructWithCompatibleFields {
        // real usage would have RestApiVersion.V_7 instead of currentVersion or minimumSupported

        static final ObjectParser<StructWithCompatibleFields, Void> PARSER = new ObjectParser<>(
            "struct_with_compatible_fields",
            StructWithCompatibleFields::new
        );
        static {
            // declare a field with `new_name` being preferable, and old_name deprecated.
            // The declaration is only available for lookup when parser has compatibility set
            PARSER.declareInt(
                StructWithCompatibleFields::setIntField,
                new ParseField("new_name", "old_name").forRestApiVersion(RestApiVersion.equalTo(RestApiVersion.minimumSupported()))
            );

            // declare `new_name` to be parsed when compatibility is NOT used
            PARSER.declareInt(
                StructWithCompatibleFields::setIntField,
                new ParseField("new_name").forRestApiVersion(RestApiVersion.onOrAfter(RestApiVersion.current()))
            );

            // declare `old_name` to throw exception when compatibility is NOT used
            PARSER.declareInt(
                (r, s) -> failWithException(),
                new ParseField("old_name").forRestApiVersion(RestApiVersion.onOrAfter(RestApiVersion.current()))
            );
        }

        private static void failWithException() {
            throw new IllegalArgumentException("invalid parameter [old_name], use [new_name] instead");
        }

        private int intField;

        private void setIntField(int intField) {
            this.intField = intField;
        }
    }

    public void testCompatibleFieldDeclarations() throws IOException {
        {
            // new_name is the only way to parse when compatibility is not set
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"new_name\": 1}",
                RestApiVersion.current()
            );
            StructWithCompatibleFields o = StructWithCompatibleFields.PARSER.parse(parser, null);
            assertEquals(1, o.intField);
        }

        {
            // old_name results with an exception when compatibility is not set
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"old_name\": 1}",
                RestApiVersion.current()
            );
            expectThrows(IllegalArgumentException.class, () -> StructWithCompatibleFields.PARSER.parse(parser, null));
        }
        {
            // new_name is allowed to be parsed with compatibility
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"new_name\": 1}",
                RestApiVersion.minimumSupported()
            );
            StructWithCompatibleFields o = StructWithCompatibleFields.PARSER.parse(parser, null);
            assertEquals(1, o.intField);
        }
        {
            // old_name is allowed to be parsed with compatibility, but results in deprecation
            XContentParser parser = createParserWithCompatibilityFor(
                JsonXContent.jsonXContent,
                "{\"old_name\": 1}",
                RestApiVersion.minimumSupported()
            );
            StructWithCompatibleFields o = StructWithCompatibleFields.PARSER.parse(parser, null);
            assertEquals(1, o.intField);
            assertWarnings(
                false,
                new DeprecationWarning(
                    DeprecationLogger.CRITICAL,
                    "[struct_with_compatible_fields][1:14] " + "Deprecated field [old_name] used, expected [new_name] instead"
                )
            );

        }
    }

    public static class StructWithOnOrAfterField {
        // real usage would have exact version like RestApiVersion.V_7 (equal to current version) instead of minimumSupported

        static final ObjectParser<StructWithOnOrAfterField, Void> PARSER = new ObjectParser<>(
            "struct_with_on_or_after_field",
            StructWithOnOrAfterField::new
        );
        static {

            // in real usage you would use a real version like RestApiVersion.V_8 and expect it to parse for version V_9, V_10 etc
            PARSER.declareInt(
                StructWithOnOrAfterField::setIntField,
                new ParseField("new_name").forRestApiVersion(RestApiVersion.onOrAfter(RestApiVersion.minimumSupported()))
            );

        }

        private int intField;

        private void setIntField(int intField) {
            this.intField = intField;
        }
    }

    public void testFieldsForVersionsOnOrAfter() throws IOException {
        // this test needs to verify that a field declared in version N will be available in version N+1
        // to do this, we assume a version N is minimum (so that the test passes for future releases) and the N+1 is current()

        // new name is accessed in "current" version - lets assume the current is minimumSupported
        XContentParser parser = createParserWithCompatibilityFor(
            JsonXContent.jsonXContent,
            "{\"new_name\": 1}",
            RestApiVersion.minimumSupported()
        );
        StructWithOnOrAfterField o1 = StructWithOnOrAfterField.PARSER.parse(parser, null);
        assertEquals(1, o1.intField);

        // new name is accessed in "future" version - lets assume the future is currentVersion (minimumSupported+1)
        XContentParser futureParser = createParserWithCompatibilityFor(
            JsonXContent.jsonXContent,
            "{\"new_name\": 1}",
            RestApiVersion.current()
        );
        StructWithOnOrAfterField o2 = StructWithOnOrAfterField.PARSER.parse(futureParser, null);
        assertEquals(1, o2.intField);
    }

    public void testDoubleDeclarationThrowsException() throws IOException {
        class DoubleFieldDeclaration {
            private int intField;

            private void setIntField(int intField) {
                this.intField = intField;
            }
        }

        ObjectParser<DoubleFieldDeclaration, Void> PARSER = new ObjectParser<>("double_field_declaration", DoubleFieldDeclaration::new);
        PARSER.declareInt(DoubleFieldDeclaration::setIntField, new ParseField("name"));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> PARSER.declareInt(DoubleFieldDeclaration::setIntField, new ParseField("name"))
        );

        assertThat(exception, instanceOf(IllegalArgumentException.class));
        assertThat(exception.getMessage(), startsWith("Parser already registered for name=[name]"));
    }
}
