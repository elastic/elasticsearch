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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ObjectParserTests extends ESTestCase {

    public void testBasics() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser("{\"test\" : \"foo\", \"test_number\" : 2, \"testArray\":  [1,2,3,4]}");
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
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser("foo");
        TestStruct s = new TestStruct();

        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("test"), ObjectParser.ValueType.STRING);
        objectParser.declareInt(TestStruct::setTestNumber, new ParseField("test_number"));
        objectParser.declareIntArray(TestStruct::setInts, new ParseField("test_array"));
        parser.setParseFieldMatcher(ParseFieldMatcher.STRICT);
        objectParser.parse(parser, s);
        assertEquals(s.test, "foo");
        assertEquals(s.testNumber, 2);
        assertEquals(s.ints, Arrays.asList(1, 2, 3, 4));
        assertEquals(objectParser.toString(), "ObjectParser{name='foo', fields=[FieldParser{preferred_name=test, supportedTokens=[VALUE_STRING], type=STRING}, FieldParser{preferred_name=test_number, supportedTokens=[VALUE_STRING, VALUE_NUMBER], type=INT}, FieldParser{preferred_name=test_array, supportedTokens=[START_ARRAY, VALUE_STRING, VALUE_NUMBER], type=INT_ARRAY}, FieldParser{preferred_name=test_array, supportedTokens=[START_ARRAY, VALUE_STRING, VALUE_NUMBER], type=INT_ARRAY}, FieldParser{preferred_name=test_number, supportedTokens=[VALUE_STRING, VALUE_NUMBER], type=INT}]}");
    }

    public void testObjectOrDefault() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser("{\"object\" : { \"test\": 2}}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser("foo", StaticTestStruct::new);
        objectParser.declareInt(StaticTestStruct::setTest, new ParseField("test"));
        objectParser.declareObjectOrDefault(StaticTestStruct::setObject, objectParser, StaticTestStruct::new, new ParseField("object"));
        StaticTestStruct s = objectParser.parse(parser);
        assertEquals(s.object.test, 2);
        parser = XContentType.JSON.xContent().createParser("{\"object\" : false }");
        s = objectParser.parse(parser);
        assertNull(s.object);

        parser = XContentType.JSON.xContent().createParser("{\"object\" : true }");
        s = objectParser.parse(parser);
        assertNotNull(s.object);
        assertEquals(s.object.test, 0);

    }

    public void testExceptions() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser("{\"test\" : \"foo\"}");
        class TestStruct {
            public int test;

            public void setTest(int test) {
                this.test = test;
            }
        }
        ObjectParser<TestStruct, TestStruct> objectParser = new ObjectParser("the_parser");
        TestStruct s = new TestStruct();
        objectParser.declareInt(TestStruct::setTest, new ParseField("test"));

        try {
            objectParser.parse(parser, s);
            fail("numeric value expected");
        } catch (ParsingException ex) {
            assertEquals(ex.getMessage(), "[the_parser] failed to parse field [test]");
            assertTrue(ex.getCause() instanceof NumberFormatException);
        }

        parser = XContentType.JSON.xContent().createParser("{\"not_supported_field\" : \"foo\"}");
        try {
            objectParser.parse(parser, s);
            fail("field not supported");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "[the_parser] unknown field [not_supported_field], parser not found");
        }
    }

    public void testDeprecationFail() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser("{\"old_test\" : \"foo\"}");
        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser("foo");
        TestStruct s = new TestStruct();

        objectParser.declareField((i, v, c) -> v.test = i.text(), new ParseField("test", "old_test"), ObjectParser.ValueType.STRING);
        parser.setParseFieldMatcher(ParseFieldMatcher.STRICT);

        try {
            objectParser.parse(parser, s);
            fail("deprecated value");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "Deprecated field [old_test] used, expected [test] instead");

        }
        assertNull(s.test);
        parser = XContentType.JSON.xContent().createParser("{\"old_test\" : \"foo\"}");
        parser.setParseFieldMatcher(ParseFieldMatcher.EMPTY);
        objectParser.parse(parser, s);
        assertEquals("foo", s.test);
    }

    public void testFailOnValueType() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser("{\"numeric_value\" : false}");
        class TestStruct {
            public String test;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser("foo");
        TestStruct s = new TestStruct();

        objectParser.declareField((i, c, x) -> c.test = i.text(), new ParseField("numeric_value"), ObjectParser.ValueType.FLOAT);
        parser.setParseFieldMatcher(ParseFieldMatcher.STRICT);
        try {
            objectParser.parse(parser, s);
            fail("wrong type - must be number");
        } catch (IllegalArgumentException ex) {
            assertEquals(ex.getMessage(), "[foo] numeric_value doesn't support values of type: VALUE_BOOLEAN");
        }
    }

    public void testParseNested() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser("{ \"test\" : 1, \"object\" : { \"test\": 2}}");
        class TestStruct {
            public int test;
            TestStruct object;
        }
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser("foo");
        TestStruct s = new TestStruct();
        s.object = new TestStruct();
        objectParser.declareField((i, c, x) -> c.test = i.intValue(), new ParseField("test"), ObjectParser.ValueType.INT);
        objectParser.declareField((i, c, x) -> objectParser.parse(parser, c.object), new ParseField("object"), ObjectParser.ValueType.OBJECT);
        objectParser.parse(parser, s);
        assertEquals(s.test, 1);
        assertEquals(s.object.test, 2);
    }

    public void testParseNestedShortcut() throws IOException {
        XContentParser parser = XContentType.JSON.xContent().createParser("{ \"test\" : 1, \"object\" : { \"test\": 2}}");
        ObjectParser<StaticTestStruct, Void> objectParser = new ObjectParser("foo", StaticTestStruct::new);
        objectParser.declareInt(StaticTestStruct::setTest, new ParseField("test"));
        objectParser.declareObject(StaticTestStruct::setObject, objectParser, new ParseField("object"));
        StaticTestStruct s = objectParser.parse(parser);
        assertEquals(s.test, 1);
        assertEquals(s.object.test, 2);
    }

    static class StaticTestStruct {
        public int test;
        StaticTestStruct object;

        public void setTest(int test) {
            this.test = test;
        }

        public void setObject(StaticTestStruct object) {
            this.object = object;
        }
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
        builder.field("boolean_field", nullValue);
        builder.field("string_or_null", nullValue ? null : "5");
        builder.endObject();
        XContentParser parser = XContentType.JSON.xContent().createParser(builder.string());
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
        ObjectParser<TestStruct, Void> objectParser = new ObjectParser("foo");
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
        TestStruct parse = objectParser.parse(parser, new TestStruct());
        assertArrayEquals(parse.double_array_field.toArray(), Arrays.asList(2.1d).toArray());
        assertEquals(parse.double_field, 2.1d, 0.0d);

        assertArrayEquals(parse.long_array_field.toArray(), Arrays.asList(4l).toArray());
        assertEquals(parse.long_field, 4l);

        assertArrayEquals(parse.string_array_field.toArray(), Arrays.asList("5").toArray());
        assertEquals(parse.string_field, "5");

        assertArrayEquals(parse.int_array_field.toArray(), Arrays.asList(1).toArray());
        assertEquals(parse.int_field, 1);

        assertArrayEquals(parse.float_array_field.toArray(), Arrays.asList(3.1f).toArray());
        assertEquals(parse.float_field, 3.1f, 0.0f);

        assertEquals(nullValue, parse.null_value);
        if (nullValue) {
            assertNull(parse.string_or_null);
        } else {
            assertEquals(parse.string_field, "5");
        }
    }

}
