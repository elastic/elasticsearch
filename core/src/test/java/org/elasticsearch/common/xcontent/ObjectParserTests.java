/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.common.xcontent;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.ObjectParser.NamedObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser.ValueType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;

public class ObjectParserTests extends ESTestCase {

    public void testBasics() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, 
                  "{\n"
                + "  \"test\" : \"foo\",\n"
                + "  \"test_number\" : 2,\n"
                + "  \"test_array\":  [1,2,3,4]\n"
                + "}");
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
        assertEquals(objectParser.toString(), "ObjectParser{name='foo', fields=["
                + "FieldParser{preferred_name=test, supportedTokens=[VALUE_STRING], type=STRING}, "
                + "FieldParser{preferred_name=test_array, supportedTokens=[START_ARRAY, VALUE_STRING, VALUE_NUMBER], type=INT_ARRAY}, "
                + "FieldParser{preferred_name=test_number, supportedTokens=[VALUE_STRING, VALUE_NUMBER], type=INT}]}");
    }

    public void testNullDeclares() {
        ObjectParser<Void, Void> objectParser = new ObjectParser<>("foo");
        Exception e = expectThrows(IllegalArgumentException.class,
                () -> objectParser.declareField(null, (r, c) -> null, new ParseField("test"), ObjectParser.ValueType.STRING));
        assertEquals("[consumer] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (ContextParser<Void, Object>) null,
                new ParseField("test"), ObjectParser.ValueType.STRING));
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (CheckedFunction<XContentParser, Object, IOException>) null,
                new ParseField("test"), ObjectParser.ValueType.STRING));
        assertEquals("[parser] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (r, c) -> null, null, ObjectParser.ValueType.STRING));
        assertEquals("[parseField] is required", e.getMessage());
        e = expectThrows(IllegalArgumentException.class, () -> objectParser.declareField(
                (o, v) -> {}, (r, c) -> null, new ParseField("test"), null));
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
                while (( token = parser.currentToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        fieldName = parser.currentName();
                    } else if (token == XContentParser.Token.VALUE_STRING){
                        if (fieldName.equals("host")) {
                            host = parser.text();
                        } else {
                            throw new IllegalStateException("boom");
                        }
                    } else if (token == XContentParser.Token.VALUE_NUMBER){
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

            public void setURI(URI uri) {
                this.uri = uri;
            }
        }

        class CustomParseContext {

            public final ClassicParser parser;

            CustomParseContext(ClassicParser parser) {
                this.parser = parser;
            }

            public URI parseURI(XContentParser parser) {
                try {
                    return this.parser.parseURI(parser);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        XContentParser parser = createParser(JsonXContent.jsonXContent,
                "{\"url\" : { \"host\": \"http://foobar\", \"port\" : 80}, \"name\" : \"foobarbaz\"}");
        ObjectParser<Foo, CustomParseContext> objectParser = new ObjectParser<>("foo");
        objectParser.declareString(Foo::setName, new ParseField("name"));
        objectParser.declareObjectOrDefault(Foo::setURI, (p, s) -> s.parseURI(p), () -> null, new ParseField("url"));
        Foo s = objectParser.parse(parser, new Foo(), new CustomParseContext(new ClassicParser()));
        assertEquals(s.uri.getHost(),  "foobar");
        assertEquals(s.uri.getPort(),  80);
        assertEquals(s.name, "foobarbaz");
    }

    public void testExceptions() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"test\" : \"foo\"}");
        class TestStruct {
            public void setTest(int test) {
            }
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("the_parser");
        TestStruct s = new TestStruct();
        objectParser.declareInt(TestStruct::setTest, new ParseField("test"));

        try {
            objectParser.parse(parser, s, null);
            fail("numeric value expected");
        } catch (ParsingException ex) {
            assertEquals(ex.getMessage(), "[the_parser] failed to parse field [test]");
            assertTrue(ex.getCause() instanceof NumberFormatException);
        }

        parser = createParser(JsonXContent.jsonXContent, "{\"not_supported_field\" : \"foo\"}");
        try {
            objectParser.parse(parser, s, null);
            fail("field not supported");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "[the_parser] unknown field [not_supported_field], parser not found");
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
        assertWarnings("Deprecated field [old_test] used, expected [test] instead");
    }

    public void testFailOnValueType() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"numeric_value\" : false}");
        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        TestStruct s = new TestStruct();

        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("numeric_value"), ObjectParser.ValueType.FLOAT);
        Exception e = expectThrows(ParsingException.class, () -> objectParser.parse(parser, s, null));
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
        objectParser.declareField((i, c, x) -> objectParser.parse(parser, c.object, null), new ParseField("object"),
                ValueType.OBJECT);
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
        FOO, BAR
    };

    public void testParseEnumFromString() throws IOException {
        class TestStruct {
            public TestEnum test;

            public void set(TestEnum value) {
                test = value;
            }
        }
        XContentParser parser = createParser(JsonXContent.jsonXContent, "{ \"test\" : \"FOO\" }");
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo");
        objectParser.declareString((struct, value) -> struct.set(TestEnum.valueOf(value)), new ParseField("test"));
        TestStruct s = objectParser.parse(parser, new TestStruct(), null);
        assertEquals(s.test, TestEnum.FOO);
    }

    public void testAllVariants() throws IOException {
        XContentBuilder builder = XContentBuilder.builder(XContentType.JSON.xContent());
        builder.startObject();
        builder.field("int_field", randomBoolean() ? "1" : 1);
        if (randomBoolean()) {
            builder.array("int_array_field", randomBoolean() ? "1" : 1);
        } else {
            builder.field("int_array_field", randomBoolean() ? "1" : 1);
        }
        builder.field("double_field", randomBoolean() ? "2.1" : 2.1d);
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
        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.string());
        class TestStruct {
            int int_field;
            long long_field;
            float float_field;
            double double_field;
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
            public void setLong_field(long long_field) {
                this.long_field = long_field;
            }
            public void setFloat_field(float float_field) {
                this.float_field = float_field;
            }
            public void setDouble_field(double double_field) {
                this.double_field = double_field;
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
        objectParser.declareIntArray(TestStruct::setInt_array_field, new ParseField("int_array_field"));
        objectParser.declareLong(TestStruct::setLong_field, new ParseField("long_field"));
        objectParser.declareLongArray(TestStruct::setLong_array_field, new ParseField("long_array_field"));
        objectParser.declareDouble(TestStruct::setDouble_field, new ParseField("double_field"));
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

        assertArrayEquals(parse.long_array_field.toArray(), Collections.singletonList(4L).toArray());
        assertEquals(parse.long_field, 4L);

        assertArrayEquals(parse.string_array_field.toArray(), Collections.singletonList("5").toArray());
        assertEquals(parse.string_field, "5");

        assertArrayEquals(parse.int_array_field.toArray(), Collections.singletonList(1).toArray());
        assertEquals(parse.int_field, 1);

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
        XContentParser parser = createParser(JsonXContent.jsonXContent, 
                  "{\"named\": {\n"
                + "  \"a\": {}"
                + "}}");
        NamedObjectHolder h = NamedObjectHolder.PARSER.apply(parser, null);
        assertThat(h.named, hasSize(1));
        assertEquals("a", h.named.get(0).name);
        assertFalse(h.namedSuppliedInOrder);
    }

    public void testParseNamedObjectInOrder() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, 
                  "{\"named\": [\n"
                + "  {\"a\": {}}"
                + "]}");
        NamedObjectHolder h = NamedObjectHolder.PARSER.apply(parser, null);
        assertThat(h.named, hasSize(1));
        assertEquals("a", h.named.get(0).name);
        assertTrue(h.namedSuppliedInOrder);
    }

    public void testParseNamedObjectTwoFieldsInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, 
                  "{\"named\": [\n"
                + "  {\"a\": {}, \"b\": {}}"
                + "]}");
        ParsingException e = expectThrows(ParsingException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertEquals("[named_object_holder] failed to parse field [named]", e.getMessage());
        assertEquals(
                "[named] can be a single object with any number of fields or an array where each entry is an object with a single field",
                e.getCause().getMessage());
    }

    public void testParseNamedObjectNoFieldsInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, 
                  "{\"named\": [\n"
                + "  {}"
                + "]}");
        ParsingException e = expectThrows(ParsingException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertEquals("[named_object_holder] failed to parse field [named]", e.getMessage());
        assertEquals(
                "[named] can be a single object with any number of fields or an array where each entry is an object with a single field",
                e.getCause().getMessage());
    }

    public void testParseNamedObjectJunkInArray() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, 
                  "{\"named\": [\n"
                + "  \"junk\""
                + "]}");
        ParsingException e = expectThrows(ParsingException.class, () -> NamedObjectHolder.PARSER.apply(parser, null));
        assertEquals("[named_object_holder] failed to parse field [named]", e.getMessage());
        assertEquals(
                "[named] can be a single object with any number of fields or an array where each entry is an object with a single field",
                e.getCause().getMessage());
    }

    public void testParseNamedObjectInOrderNotSupported() throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent,
                  "{\"named\": [\n"
                + "  {\"a\": {}}"
                + "]}");

        // Create our own parser for this test so we can disable support for the "ordered" mode specified by the array above
        ObjectParser<NamedObjectHolder, Void> objectParser = new ObjectParser<>("named_object_holder",
                NamedObjectHolder::new);
        objectParser.declareNamedObjects(NamedObjectHolder::setNamed, NamedObject.PARSER, new ParseField("named"));

        // Now firing the xml through it fails
        ParsingException e = expectThrows(ParsingException.class, () -> objectParser.apply(parser, null));
        assertEquals("[named_object_holder] failed to parse field [named]", e.getMessage());
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
        XContentParser parser = createParser(JsonXContent.jsonXContent, b.bytes());

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
        XContentParser parser = createParser(JsonXContent.jsonXContent, b.bytes());

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
        XContentParser parser = createParser(JsonXContent.jsonXContent, b.bytes());
        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser<>("foo", true, null);
        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("test"), ObjectParser.ValueType.STRING);
        TestStruct s = objectParser.parse(parser, new TestStruct(), null);
        assertEquals(s.test, "foo");
    }

    static class NamedObjectHolder {
        public static final ObjectParser<NamedObjectHolder, Void> PARSER = new ObjectParser<>("named_object_holder",
                NamedObjectHolder::new);
        static {
            PARSER.declareNamedObjects(NamedObjectHolder::setNamed, NamedObject.PARSER, NamedObjectHolder::keepNamedInOrder,
                    new ParseField("named"));
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
}
