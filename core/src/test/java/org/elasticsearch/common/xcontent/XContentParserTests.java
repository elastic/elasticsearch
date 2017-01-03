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

import com.fasterxml.jackson.core.JsonParseException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isIn;
import static org.hamcrest.Matchers.nullValue;

public class XContentParserTests extends ESTestCase {

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
            assertThat(e, instanceOf(ElasticsearchParseException.class));
            assertThat(e.getMessage(), containsString("Failed to parse list"));
        }
    }

    public void testReadMapStrings() throws IOException {
        Map<String, String> map = readMapStrings("{\"foo\": {\"kbar\":\"vbar\"}}");
        assertThat(map.get("kbar"), equalTo("vbar"));
        assertThat(map.size(), equalTo(1));
        map = readMapStrings("{\"foo\": {\"kbar\":\"vbar\", \"kbaz\":\"vbaz\"}}");
        assertThat(map.get("kbar"), equalTo("vbar"));
        assertThat(map.get("kbaz"), equalTo("vbaz"));
        assertThat(map.size(), equalTo(2));
        map = readMapStrings("{\"foo\": {}}");
        assertThat(map.size(), equalTo(0));
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
            return randomBoolean() ? parser.mapStringsOrdered() : parser.mapStrings();
        }
    }

    @SuppressWarnings("deprecation") // #isBooleanValueLenient() and #booleanValueLenient() are the test subjects
    public void testReadLenientBooleans() throws IOException {
        // allow String, boolean and int representations of lenient booleans
        String falsy = randomFrom("\"off\"", "\"no\"", "\"0\"", "0", "\"false\"", "false");
        String truthy = randomFrom("\"on\"", "\"yes\"", "\"1\"", "1", "\"true\"", "true");

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{\"foo\": " + falsy + ", \"bar\": " + truthy + "}")) {
            XContentParser.Token token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.START_OBJECT));
            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("foo"));
            token = parser.nextToken();
            assertThat(token, isIn(
                Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_BOOLEAN)));
            assertTrue(parser.isBooleanValueLenient());
            assertFalse(parser.booleanValueLenient());

            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("bar"));
            token = parser.nextToken();
            assertThat(token, isIn(
                Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_NUMBER, XContentParser.Token.VALUE_BOOLEAN)));
            assertTrue(parser.isBooleanValueLenient());
            assertTrue(parser.booleanValueLenient());
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
            assertThat(token, isIn(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_NUMBER)));
            assertFalse(parser.isBooleanValue());
            if (token.equals(XContentParser.Token.VALUE_STRING)) {
                expectThrows(IllegalArgumentException.class, parser::booleanValue);
            } else {
                expectThrows(JsonParseException.class, parser::booleanValue);
            }

            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("bar"));
            token = parser.nextToken();
            assertThat(token, isIn(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_NUMBER)));
            assertFalse(parser.isBooleanValue());
            if (token.equals(XContentParser.Token.VALUE_STRING)) {
                expectThrows(IllegalArgumentException.class, parser::booleanValue);
            } else {
                expectThrows(JsonParseException.class, parser::booleanValue);
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
            assertThat(token, isIn(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_BOOLEAN)));
            assertTrue(parser.isBooleanValue());
            assertFalse(parser.booleanValue());

            token = parser.nextToken();
            assertThat(token, equalTo(XContentParser.Token.FIELD_NAME));
            assertThat(parser.currentName(), equalTo("bar"));
            token = parser.nextToken();
            assertThat(token, isIn(Arrays.asList(XContentParser.Token.VALUE_STRING, XContentParser.Token.VALUE_BOOLEAN)));
            assertTrue(parser.isBooleanValue());
            assertTrue(parser.booleanValue());
        }
    }
}
