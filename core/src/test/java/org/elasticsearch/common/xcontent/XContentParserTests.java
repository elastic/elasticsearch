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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class XContentParserTests extends ESTestCase {

    @Test
    public void testReadList() throws IOException {
        List<String> strings;
        List<Integer> ints;
        List<Object> objs;

        strings = readList("{\"foo\": [\"bar\"]}");
        assertThat(strings, contains("bar"));
        strings = readList("{\"foo\": [\"bar\",\"baz\"]}");
        assertThat(strings, contains("bar", "baz"));
        ints = readList("{\"foo\": [1, 2, 3], \"bar\": 4}");
        assertThat(ints, contains(1, 2, 3));
        objs = readList("{\"foo\": [{\"bar\":1},{\"baz\":2},{\"qux\":3}]}");
        assertThat(objs, hasSize(3));
        objs = readList("{\"foo\": [null]}");
        assertThat(objs, contains(nullValue()));
        objs = readList("{\"foo\": []}");
        assertThat(objs, hasSize(0));
        ints = readList("{\"foo\": [1]}");
        assertThat(ints, contains(1));
        ints = readList("{\"foo\": [1,2]}");
        assertThat(ints, contains(1, 2));
        objs = readList("{\"foo\": [{},{},{},{}]}");
        assertThat(objs, hasSize(4));
    }

    @Test
    public void testReadListThrowsException() throws IOException {
        // Calling XContentParser.list() or listOrderedMap() to read a simple
        // value or object should throw an exception
        assertReadListThrowsException("{\"foo\": \"bar\"}");
        assertReadListThrowsException("{\"foo\": 1, \"bar\": 2}");
        assertReadListThrowsException("{\"foo\": {\"bar\":\"baz\"}}");
    }

    @SuppressWarnings("unchecked")
    private static <T> List<T> readList(String source) throws IOException {
        try (XContentParser parser = XContentType.JSON.xContent().createParser(source)) {
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
}
