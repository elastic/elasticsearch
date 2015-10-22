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
package org.elasticsearch.test.rest.parser;

import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.section.DoSection;
import org.elasticsearch.test.rest.section.ExecutableSection;
import org.elasticsearch.test.rest.section.SetupSection;
import org.elasticsearch.test.rest.section.SkipSection;
import org.elasticsearch.test.rest.section.TestSection;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Context shared across the whole tests parse phase.
 * Provides shared parse methods and holds information needed to parse the test sections (e.g. es version)
 */
public class RestTestSuiteParseContext {

    private static final SetupSectionParser SETUP_SECTION_PARSER = new SetupSectionParser();
    private static final RestTestSectionParser TEST_SECTION_PARSER = new RestTestSectionParser();
    private static final SkipSectionParser SKIP_SECTION_PARSER = new SkipSectionParser();
    private static final DoSectionParser DO_SECTION_PARSER = new DoSectionParser();
    private static final Map<String, RestTestFragmentParser<? extends ExecutableSection>> EXECUTABLE_SECTIONS_PARSERS = new HashMap<>();
    static {
        EXECUTABLE_SECTIONS_PARSERS.put("do", DO_SECTION_PARSER);
        EXECUTABLE_SECTIONS_PARSERS.put("set", new SetSectionParser());
        EXECUTABLE_SECTIONS_PARSERS.put("match", new MatchParser());
        EXECUTABLE_SECTIONS_PARSERS.put("is_true", new IsTrueParser());
        EXECUTABLE_SECTIONS_PARSERS.put("is_false", new IsFalseParser());
        EXECUTABLE_SECTIONS_PARSERS.put("gt", new GreaterThanParser());
        EXECUTABLE_SECTIONS_PARSERS.put("gte", new GreaterThanEqualToParser());
        EXECUTABLE_SECTIONS_PARSERS.put("lt", new LessThanParser());
        EXECUTABLE_SECTIONS_PARSERS.put("lte", new LessThanOrEqualToParser());
        EXECUTABLE_SECTIONS_PARSERS.put("length", new LengthParser());
    }

    private final String api;
    private final String suiteName;
    private final XContentParser parser;

    public RestTestSuiteParseContext(String api, String suiteName, XContentParser parser) {
        this.api = api;
        this.suiteName = suiteName;
        this.parser = parser;
    }

    public String getApi() {
        return api;
    }

    public String getSuiteName() {
        return suiteName;
    }

    public XContentParser parser() {
        return parser;
    }

    public SetupSection parseSetupSection() throws IOException, RestTestParseException {

        advanceToFieldName();

        if ("setup".equals(parser.currentName())) {
            parser.nextToken();
            SetupSection setupSection = SETUP_SECTION_PARSER.parse(this);
            parser.nextToken();
            return setupSection;
        }

        return SetupSection.EMPTY;
    }

    public TestSection parseTestSection() throws IOException, RestTestParseException {
        return TEST_SECTION_PARSER.parse(this);
    }

    public SkipSection parseSkipSection() throws IOException, RestTestParseException {

        advanceToFieldName();

        if ("skip".equals(parser.currentName())) {
            SkipSection skipSection = SKIP_SECTION_PARSER.parse(this);
            parser.nextToken();
            return skipSection;
        }

        return SkipSection.EMPTY;
    }

    public ExecutableSection parseExecutableSection() throws IOException, RestTestParseException {
        advanceToFieldName();
        String section = parser.currentName();
        RestTestFragmentParser<? extends ExecutableSection> execSectionParser = EXECUTABLE_SECTIONS_PARSERS.get(section);
        if (execSectionParser == null) {
            throw new RestTestParseException("no parser found for executable section [" + section + "]");
        }
        ExecutableSection executableSection = execSectionParser.parse(this);
        parser.nextToken();
        return executableSection;
    }

    public DoSection parseDoSection() throws IOException, RestTestParseException {
        return DO_SECTION_PARSER.parse(this);
    }

    public void advanceToFieldName() throws IOException, RestTestParseException {
        XContentParser.Token token = parser.currentToken();
        //we are in the beginning, haven't called nextToken yet
        if (token == null) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_ARRAY) {
            token = parser.nextToken();
        }
        if (token == XContentParser.Token.START_OBJECT) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.FIELD_NAME) {
            throw new RestTestParseException("malformed test section: field name expected but found " + token);
        }
    }

    public String parseField() throws IOException, RestTestParseException {
        parser.nextToken();
        assert parser.currentToken().isValue();
        String field = parser.text();
        parser.nextToken();
        return field;
    }

    public Tuple<String, Object> parseTuple() throws IOException, RestTestParseException {
        parser.nextToken();
        advanceToFieldName();
        Map<String,Object> map = parser.map();
        assert parser.currentToken() == XContentParser.Token.END_OBJECT;
        parser.nextToken();

        if (map.size() != 1) {
            throw new RestTestParseException("expected key value pair but found " + map.size() + " ");
        }

        Map.Entry<String, Object> entry = map.entrySet().iterator().next();
        return Tuple.tuple(entry.getKey(), entry.getValue());
    }
}
