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
package org.elasticsearch.test.rest.yaml.parser;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.ParseFieldMatcherSupplier;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.rest.yaml.section.ClientYamlTestSection;
import org.elasticsearch.test.rest.yaml.section.DoSection;
import org.elasticsearch.test.rest.yaml.section.ExecutableSection;
import org.elasticsearch.test.rest.yaml.section.GreaterThanAssertion;
import org.elasticsearch.test.rest.yaml.section.GreaterThanEqualToAssertion;
import org.elasticsearch.test.rest.yaml.section.IsFalseAssertion;
import org.elasticsearch.test.rest.yaml.section.IsTrueAssertion;
import org.elasticsearch.test.rest.yaml.section.MatchAssertion;
import org.elasticsearch.test.rest.yaml.section.SetSection;
import org.elasticsearch.test.rest.yaml.section.SetupSection;
import org.elasticsearch.test.rest.yaml.section.SkipSection;
import org.elasticsearch.test.rest.yaml.section.TeardownSection;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Context shared across the whole tests parse phase.
 * Provides shared parse methods and holds information needed to parse the test sections (e.g. es version)
 */
public class ClientYamlTestSuiteParseContext implements ParseFieldMatcherSupplier {

    private static final SetupSectionParser SETUP_SECTION_PARSER = new SetupSectionParser();
    private static final TeardownSectionParser TEARDOWN_SECTION_PARSER = new TeardownSectionParser();
    private static final ClientYamlTestSectionParser TEST_SECTION_PARSER = new ClientYamlTestSectionParser();
    private static final SkipSectionParser SKIP_SECTION_PARSER = new SkipSectionParser();
    private static final DoSectionParser DO_SECTION_PARSER = new DoSectionParser();
    private static final Map<String, ClientYamlTestFragmentParser<? extends ExecutableSection>> EXECUTABLE_SECTIONS_PARSERS =
            new HashMap<>();
    static {
        EXECUTABLE_SECTIONS_PARSERS.put("lt", new LessThanParser());
        EXECUTABLE_SECTIONS_PARSERS.put("lte", new LessThanOrEqualToParser());
        EXECUTABLE_SECTIONS_PARSERS.put("length", new LengthParser());
    }
    private static final NamedXContentRegistry XCONTENT_REGISTRY = new NamedXContentRegistry(Arrays.asList(
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("do"), (p, c) -> DO_SECTION_PARSER.parse((ClientYamlTestSuiteParseContext) c)),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("set"), SetSection::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("match"), MatchAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("is_true"), IsTrueAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("is_false"), IsFalseAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("gt"), GreaterThanAssertion::parse),
            new NamedXContentRegistry.Entry(ExecutableSection.class, new ParseField("gte"), GreaterThanEqualToAssertion::parse)


            )); 

    private final String api;
    private final String suiteName;
    private final XContentParser parser;

    public ClientYamlTestSuiteParseContext(String api, String suiteName, XContentParser parser) {
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

    public SetupSection parseSetupSection() throws IOException, ClientYamlTestParseException {

        advanceToFieldName();

        if ("setup".equals(parser.currentName())) {
            parser.nextToken();
            SetupSection setupSection = SETUP_SECTION_PARSER.parse(this);
            parser.nextToken();
            return setupSection;
        }

        return SetupSection.EMPTY;
    }

    public TeardownSection parseTeardownSection() throws IOException, ClientYamlTestParseException {
        advanceToFieldName();

        if ("teardown".equals(parser.currentName())) {
            parser.nextToken();
            TeardownSection teardownSection = TEARDOWN_SECTION_PARSER.parse(this);
            parser.nextToken();
            return teardownSection;
        }

        return TeardownSection.EMPTY;
    }

    public ClientYamlTestSection parseTestSection() throws IOException, ClientYamlTestParseException {
        return TEST_SECTION_PARSER.parse(this);
    }

    public SkipSection parseSkipSection() throws IOException, ClientYamlTestParseException {

        advanceToFieldName();

        if ("skip".equals(parser.currentName())) {
            SkipSection skipSection = SKIP_SECTION_PARSER.parse(this);
            parser.nextToken();
            return skipSection;
        }

        return SkipSection.EMPTY;
    }

    public ExecutableSection parseExecutableSection() throws IOException, ClientYamlTestParseException {
        advanceToFieldName();
        String section = parser.currentName();
        XContentLocation location = parser.getTokenLocation();
        try {
            ExecutableSection executableSection = parser.namedObject(ExecutableSection.class, section, null);
            parser.nextToken();
            return executableSection;
        } catch (Exception e) {
            throw new IOException("Error parsing section starting at ["+ location + "]", e);
        }
    }

    public DoSection parseDoSection() throws IOException, ClientYamlTestParseException {
        return DO_SECTION_PARSER.parse(this);
    }

    public void advanceToFieldName() throws IOException {
        ParserUtils.advanceToFieldName(parser);
    }

    public String parseField() throws IOException, ClientYamlTestParseException {
        return ParserUtils.parseField(parser);
    }

    public Tuple<String, Object> parseTuple() throws IOException, ClientYamlTestParseException {
        return ParserUtils.parseTuple(parser);
    }

    @Override
    public ParseFieldMatcher getParseFieldMatcher() {
        return ParseFieldMatcher.STRICT;
    }
}
