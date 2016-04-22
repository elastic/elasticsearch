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

package org.elasticsearch.script;


import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.ToXContent.MapParams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;
import org.elasticsearch.script.Script.ScriptParseException;
import org.elasticsearch.script.ScriptParameterParser.ScriptParameterValue;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class ScriptParameterParserTests extends ESTestCase {
    public void testTokenDefaultInline() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"script\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        ScriptParameterParser paramParser = new ScriptParameterParser();
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        paramParser = new ScriptParameterParser(null);
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        paramParser = new ScriptParameterParser(new HashSet<String>());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenDefaultFile() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"script_file\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        ScriptParameterParser paramParser = new ScriptParameterParser();
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenDefaultIndexed() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"script_id\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        ScriptParameterParser paramParser = new ScriptParameterParser();
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenDefaultNotFound() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"bar\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        ScriptParameterParser paramParser = new ScriptParameterParser();
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(false));
        assertThat(paramParser.getDefaultScriptParameterValue(), nullValue());
        assertThat(paramParser.getScriptParameterValue("script"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenSingleParameter() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenSingleParameterFile() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo_file\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenSingleParameterIndexed() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo_id\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenSingleParameterDelcaredTwiceInlineFile() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"scriptValue\", \"foo_file\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        try {
            paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testTokenSingleParameterDelcaredTwiceInlineIndexed() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"scriptValue\", \"foo_id\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        try {
            paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testTokenSingleParameterDelcaredTwiceFileInline() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo_file\" : \"scriptValue\", \"foo\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        try {
            paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testTokenSingleParameterDelcaredTwiceFileIndexed() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo_file\" : \"scriptValue\", \"foo_id\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        try {
            paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testTokenSingleParameterDelcaredTwiceIndexedInline() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo_id\" : \"scriptValue\", \"foo\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        try {
            paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testTokenSingleParameterDelcaredTwiceIndexedFile() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo_id\" : \"scriptValue\", \"foo_file\" : \"scriptValue\" }"));
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        try {
            paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testTokenMultipleParameters() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"fooScriptValue\", \"bar_file\" : \"barScriptValue\", \"baz_id\" : \"bazScriptValue\" }"));
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenMultipleParametersWithLang() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"fooScriptValue\", \"bar_file\" : \"barScriptValue\", \"lang\" : \"myLang\", \"baz_id\" : \"bazScriptValue\" }"));
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), equalTo("myLang"));
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), equalTo("myLang"));
    }

    public void testTokenMultipleParametersNotFound() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"other\" : \"scriptValue\" }"));
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(false));
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenMultipleParametersSomeNotFound() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"fooScriptValue\", \"other_file\" : \"barScriptValue\", \"baz_id\" : \"bazScriptValue\" }"));
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        Token token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(false));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        token = parser.nextToken();
        while (token != Token.VALUE_STRING) {
            token = parser.nextToken();
        }
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(true));
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testTokenMultipleParametersWrongType() throws IOException {
        XContentParser parser = XContentHelper.createParser(new BytesArray("{ \"foo\" : \"fooScriptValue\", \"bar_file\" : \"barScriptValue\", \"baz_id\" : \"bazScriptValue\" }"));
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        assertThat(paramParser.token(parser.currentName(), parser.currentToken(), parser, ParseFieldMatcher.STRICT), equalTo(false));
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testReservedParameters() {
        try {
            new ScriptParameterParser(singleton("lang"));
            fail("Expected IllegalArgumentException");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("lang is reserved"));
        }
    }

    public void testConfigDefaultInline() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("script", "scriptValue");
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
        config = new HashMap<>();
        config.put("script", "scriptValue");
        paramParser = new ScriptParameterParser(null);
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
        config = new HashMap<>();
        config.put("script", "scriptValue");
        paramParser = new ScriptParameterParser(new HashSet<String>());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigDefaultFile() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("script_file", "scriptValue");
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigDefaultIndexed() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("script_id", "scriptValue");
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigDefaultIndexedNoRemove() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("script_id", "scriptValue");
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseConfig(config, false, ParseFieldMatcher.STRICT);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.size(), equalTo(1));
        assertThat((String) config.get("script_id"), equalTo("scriptValue"));
    }

    public void testConfigDefaultNotFound() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "bar");
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertThat(paramParser.getDefaultScriptParameterValue(), nullValue());
        assertThat(paramParser.getScriptParameterValue("script"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.size(), equalTo(1));
        assertThat((String) config.get("foo"), equalTo("bar"));
    }

    public void testConfigSingleParameter() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigSingleParameterFile() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo_file", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigSingleParameterIndexed() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo_id", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigSingleParameterDelcaredTwiceInlineFile() throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("foo", "scriptValue");
        config.put("foo_file", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testConfigSingleParameterDelcaredTwiceInlineIndexed() throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("foo", "scriptValue");
        config.put("foo_id", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testConfigSingleParameterDelcaredTwiceFileInline() throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("foo_file", "scriptValue");
        config.put("foo", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testConfigSingleParameterDelcaredTwiceFileIndexed() throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("foo_file", "scriptValue");
        config.put("foo_id", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testConfigSingleParameterDelcaredTwiceIndexedInline() throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("foo_id", "scriptValue");
        config.put("foo", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testConfigSingleParameterDelcaredTwiceIndexedFile() throws IOException {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("foo_id", "scriptValue");
        config.put("foo_file", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testConfigMultipleParameters() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigMultipleParametersWithLang() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("lang", "myLang");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), equalTo("myLang"));
        assertThat(config.isEmpty(), equalTo(true));
    }

    public void testConfigMultipleParametersWithLangNoRemove() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("lang", "myLang");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        paramParser.parseConfig(config, false, ParseFieldMatcher.STRICT);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), equalTo("myLang"));
        assertThat(config.size(), equalTo(4));
        assertThat((String) config.get("foo"), equalTo("fooScriptValue"));
        assertThat((String) config.get("bar_file"), equalTo("barScriptValue"));
        assertThat((String) config.get("baz_id"), equalTo("bazScriptValue"));
        assertThat((String) config.get("lang"), equalTo("myLang"));
    }

    public void testConfigMultipleParametersNotFound() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("other", "scriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.size(), equalTo(1));
        assertThat((String) config.get("other"), equalTo("scriptValue"));
    }

    public void testConfigMultipleParametersSomeNotFound() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("other_file", "barScriptValue");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        assertThat(config.size(), equalTo(1));
        assertThat((String) config.get("other_file"), equalTo("barScriptValue"));
    }

    public void testConfigMultipleParametersInlineWrongType() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", 1L);
        config.put("bar_file", "barScriptValue");
        config.put("baz_id", "bazScriptValue");
        config.put("lang", "myLang");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Value must be of type String: [foo]"));
        }
    }

    public void testConfigMultipleParametersFileWrongType() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", 1L);
        config.put("baz_id", "bazScriptValue");
        config.put("lang", "myLang");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Value must be of type String: [bar_file]"));
        }

    }

    public void testConfigMultipleParametersIndexedWrongType() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("baz_id", 1L);
        config.put("lang", "myLang");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Value must be of type String: [baz_id]"));
        }
    }

    public void testConfigMultipleParametersLangWrongType() throws IOException {
        Map<String, Object> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("baz_id", "bazScriptValue");
        config.put("lang", 1L);
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        try {
            paramParser.parseConfig(config, true, ParseFieldMatcher.STRICT);
            fail("Expected ScriptParseException");
        } catch (ScriptParseException e) {
            assertThat(e.getMessage(), is("Value must be of type String: [lang]"));
        }
    }

    public void testParamsDefaultInline() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("script", "scriptValue");
        MapParams params = new MapParams(config);
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseParams(params);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());

        paramParser = new ScriptParameterParser(null);
        paramParser.parseParams(params);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());

        paramParser = new ScriptParameterParser(new HashSet<String>());
        paramParser.parseParams(params);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsDefaultFile() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("script_file", "scriptValue");
        MapParams params = new MapParams(config);
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseParams(params);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsDefaultIndexed() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("script_id", "scriptValue");
        MapParams params = new MapParams(config);
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseParams(params);
        assertDefaultParameterValue(paramParser, "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsDefaultNotFound() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo", "bar");
        MapParams params = new MapParams(config);
        ScriptParameterParser paramParser = new ScriptParameterParser();
        paramParser.parseParams(params);
        assertThat(paramParser.getDefaultScriptParameterValue(), nullValue());
        assertThat(paramParser.getScriptParameterValue("script"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsSingleParameter() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.INLINE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsSingleParameterFile() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo_file", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.FILE);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsSingleParameterIndexed() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo_id", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertParameterValue(paramParser, "foo", "scriptValue", ScriptType.STORED);
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsSingleParameterDelcaredTwiceInlineFile() throws IOException {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("foo", "scriptValue");
        config.put("foo_file", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        try {
            paramParser.parseParams(params);
            fail("Expected ScriptParseException");
        } catch(ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testParamsSingleParameterDelcaredTwiceInlineIndexed() throws IOException {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("foo", "scriptValue");
        config.put("foo_id", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        try {
            paramParser.parseParams(params);
            fail("Expected ScriptParseException");
        } catch(ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testParamsSingleParameterDelcaredTwiceFileInline() throws IOException {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("foo_file", "scriptValue");
        config.put("foo", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        try {
            paramParser.parseParams(params);
            fail("Expected ScriptParseException");
        } catch(ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testParamsSingleParameterDelcaredTwiceFileIndexed() throws IOException {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("foo_file", "scriptValue");
        config.put("foo_id", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        try {
            paramParser.parseParams(params);
            fail("Expected ScriptParseException");
        } catch(ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testParamsSingleParameterDelcaredTwiceIndexedInline() throws IOException {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("foo_id", "scriptValue");
        config.put("foo", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        try {
            paramParser.parseParams(params);
            fail("Expected ScriptParseException");
        } catch(ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testParamsSingleParameterDelcaredTwiceIndexedFile() throws IOException {
        Map<String, String> config = new LinkedHashMap<>();
        config.put("foo_id", "scriptValue");
        config.put("foo_file", "scriptValue");
        Set<String> parameters = Collections.singleton("foo");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        MapParams params = new MapParams(config);
        try {
            paramParser.parseParams(params);
            fail("Expected ScriptParseException");
        } catch(ScriptParseException e) {
            assertThat(e.getMessage(), is("Only one of [foo, foo_file, foo_id] is allowed."));
        }
    }

    public void testParamsMultipleParameters() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsMultipleParametersWithLang() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("lang", "myLang");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), equalTo("myLang"));
    }

    public void testParamsMultipleParametersWithLangNoRemove() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("bar_file", "barScriptValue");
        config.put("lang", "myLang");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertParameterValue(paramParser, "bar", "barScriptValue", ScriptType.FILE);
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), equalTo("myLang"));
    }

    public void testParamsMultipleParametersNotFound() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("other", "scriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    public void testParamsMultipleParametersSomeNotFound() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put("foo", "fooScriptValue");
        config.put("other_file", "barScriptValue");
        config.put("baz_id", "bazScriptValue");
        Set<String> parameters = new HashSet<>();
        parameters.add("foo");
        parameters.add("bar");
        parameters.add("baz");
        ScriptParameterParser paramParser = new ScriptParameterParser(parameters);
        assertThat(paramParser.getScriptParameterValue("foo"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz"), nullValue());
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
        MapParams params = new MapParams(config);
        paramParser.parseParams(params);
        assertParameterValue(paramParser, "foo", "fooScriptValue", ScriptType.INLINE);
        assertThat(paramParser.getScriptParameterValue("bar"), nullValue());
        assertParameterValue(paramParser, "baz", "bazScriptValue", ScriptType.STORED);
        assertThat(paramParser.getScriptParameterValue("bar_file"), nullValue());
        assertThat(paramParser.getScriptParameterValue("baz_id"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other"), nullValue());
        assertThat(paramParser.getScriptParameterValue("other_file"), nullValue());
        assertThat(paramParser.lang(), nullValue());
    }

    private void assertDefaultParameterValue(ScriptParameterParser paramParser, String expectedScript, ScriptType expectedScriptType) throws IOException {
        ScriptParameterValue defaultValue = paramParser.getDefaultScriptParameterValue();
        ScriptParameterValue defaultValueByName = paramParser.getScriptParameterValue("script");
        assertThat(defaultValue.scriptType(), equalTo(expectedScriptType));
        assertThat(defaultValue.script(), equalTo(expectedScript));
        assertThat(defaultValueByName.scriptType(), equalTo(expectedScriptType));
        assertThat(defaultValueByName.script(), equalTo(expectedScript));
    }

    private void assertParameterValue(ScriptParameterParser paramParser, String parameterName, String expectedScript, ScriptType expectedScriptType) throws IOException {
        ScriptParameterValue value = paramParser.getScriptParameterValue(parameterName);
        assertThat(value.scriptType(), equalTo(expectedScriptType));
        assertThat(value.script(), equalTo(expectedScript));
    }
}
