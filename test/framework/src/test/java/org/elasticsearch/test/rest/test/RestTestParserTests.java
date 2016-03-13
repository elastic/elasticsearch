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
package org.elasticsearch.test.rest.test;

import org.elasticsearch.Version;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.test.rest.parser.RestTestSuiteParseContext;
import org.elasticsearch.test.rest.parser.RestTestSuiteParser;
import org.elasticsearch.test.rest.section.DoSection;
import org.elasticsearch.test.rest.section.IsTrueAssertion;
import org.elasticsearch.test.rest.section.MatchAssertion;
import org.elasticsearch.test.rest.section.RestTestSuite;
import org.junit.After;

import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RestTestParserTests extends ESTestCase {
    private XContentParser parser;

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();
        //makes sure that we consumed the whole stream, XContentParser doesn't expose isClosed method
        //next token can be null even in the middle of the document (e.g. with "---"), but not too many consecutive times
        assertThat(parser.currentToken(), nullValue());
        assertThat(parser.nextToken(), nullValue());
        assertThat(parser.nextToken(), nullValue());
        parser.close();
    }

    public void testParseTestSetupAndSections() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                        "setup:\n" +
                        "  - do:\n" +
                        "        indices.create:\n" +
                        "          index: test_index\n" +
                        "\n" +
                        "---\n" +
                        "\"Get index mapping\":\n" +
                        "  - do:\n" +
                        "      indices.get_mapping:\n" +
                        "        index: test_index\n" +
                        "\n" +
                        "  - match: {test_index.test_type.properties.text.type:     string}\n" +
                        "  - match: {test_index.test_type.properties.text.analyzer: whitespace}\n" +
                        "\n" +
                        "---\n" +
                        "\"Get type mapping - pre 1.0\":\n" +
                        "\n" +
                        "  - skip:\n" +
                        "      version:     \"2.0.0 - \"\n" +
                        "      reason:      \"for newer versions the index name is always returned\"\n" +
                        "\n" +
                        "  - do:\n" +
                        "      indices.get_mapping:\n" +
                        "        index: test_index\n" +
                        "        type: test_type\n" +
                        "\n" +
                        "  - match: {test_type.properties.text.type:     string}\n" +
                        "  - match: {test_type.properties.text.analyzer: whitespace}\n"
        );

        RestTestSuiteParser testParser = new RestTestSuiteParser();
        RestTestSuite restTestSuite = testParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo("suite"));
        assertThat(restTestSuite.getSetupSection(), notNullValue());
        assertThat(restTestSuite.getSetupSection().getSkipSection().isEmpty(), equalTo(true));

        assertThat(restTestSuite.getSetupSection().getDoSections().size(), equalTo(1));
        assertThat(restTestSuite.getSetupSection().getDoSections().get(0).getApiCallSection().getApi(), equalTo("indices.create"));
        assertThat(restTestSuite.getSetupSection().getDoSections().get(0).getApiCallSection().getParams().size(), equalTo(1));
        assertThat(restTestSuite.getSetupSection().getDoSections().get(0).getApiCallSection().getParams().get("index"), equalTo("test_index"));

        assertThat(restTestSuite.getTestSections().size(), equalTo(2));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Get index mapping"));
        assertThat(restTestSuite.getTestSections().get(0).getSkipSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(3));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(1));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(MatchAssertion.class));
        MatchAssertion matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(1);
        assertThat(matchAssertion.getField(), equalTo("test_index.test_type.properties.text.type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("string"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(2), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("test_index.test_type.properties.text.analyzer"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("whitespace"));

        assertThat(restTestSuite.getTestSections().get(1).getName(), equalTo("Get type mapping - pre 1.0"));
        assertThat(restTestSuite.getTestSections().get(1).getSkipSection().isEmpty(), equalTo(false));
        assertThat(restTestSuite.getTestSections().get(1).getSkipSection().getReason(), equalTo("for newer versions the index name is always returned"));
        assertThat(restTestSuite.getTestSections().get(1).getSkipSection().getLowerVersion(), equalTo(Version.V_2_0_0));
        assertThat(restTestSuite.getTestSections().get(1).getSkipSection().getUpperVersion(), equalTo(Version.CURRENT));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().size(), equalTo(3));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(0), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(1).getExecutableSections().get(0);
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        assertThat(doSection.getApiCallSection().getParams().get("type"), equalTo("test_type"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(1).getExecutableSections().get(1);
        assertThat(matchAssertion.getField(), equalTo("test_type.properties.text.type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("string"));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(2), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(1).getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("test_type.properties.text.analyzer"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("whitespace"));
    }

    public void testParseTestSingleTestSection() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
        "---\n" +
                "\"Index with ID\":\n" +
                "\n" +
                " - do:\n" +
                "      index:\n" +
                "          index:  test-weird-index-中文\n" +
                "          type:   weird.type\n" +
                "          id:     1\n" +
                "          body:   { foo: bar }\n" +
                "\n" +
                " - is_true:   ok\n" +
                " - match:   { _index:   test-weird-index-中文 }\n" +
                " - match:   { _type:    weird.type }\n" +
                " - match:   { _id:      \"1\"}\n" +
                " - match:   { _version: 1}\n" +
                "\n" +
                " - do:\n" +
                "      get:\n" +
                "          index:  test-weird-index-中文\n" +
                "          type:   weird.type\n" +
                "          id:     1\n" +
                "\n" +
                " - match:   { _index:   test-weird-index-中文 }\n" +
                " - match:   { _type:    weird.type }\n" +
                " - match:   { _id:      \"1\"}\n" +
                " - match:   { _version: 1}\n" +
                " - match:   { _source: { foo: bar }}"
        );

        RestTestSuiteParser testParser = new RestTestSuiteParser();
        RestTestSuite restTestSuite = testParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo("suite"));

        assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(true));

        assertThat(restTestSuite.getTestSections().size(), equalTo(1));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Index with ID"));
        assertThat(restTestSuite.getTestSections().get(0).getSkipSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(12));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("index"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(IsTrueAssertion.class));
        IsTrueAssertion trueAssertion = (IsTrueAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(1);
        assertThat(trueAssertion.getField(), equalTo("ok"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(2), instanceOf(MatchAssertion.class));
        MatchAssertion matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("_index"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("test-weird-index-中文"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(3), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(3);
        assertThat(matchAssertion.getField(), equalTo("_type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("weird.type"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(4), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(4);
        assertThat(matchAssertion.getField(), equalTo("_id"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(5), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(5);
        assertThat(matchAssertion.getField(), equalTo("_version"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(6), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(6);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(7), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(7);
        assertThat(matchAssertion.getField(), equalTo("_index"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("test-weird-index-中文"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(8), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(8);
        assertThat(matchAssertion.getField(), equalTo("_type"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("weird.type"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(9), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(9);
        assertThat(matchAssertion.getField(), equalTo("_id"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(10), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(10);
        assertThat(matchAssertion.getField(), equalTo("_version"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("1"));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(11), instanceOf(MatchAssertion.class));
        matchAssertion = (MatchAssertion) restTestSuite.getTestSections().get(0).getExecutableSections().get(11);
        assertThat(matchAssertion.getField(), equalTo("_source"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Map.class));
        assertThat(((Map) matchAssertion.getExpectedValue()).get("foo").toString(), equalTo("bar"));
    }

    public void testParseTestMultipleTestSections() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
        "---\n" +
                "\"Missing document (partial doc)\":\n" +
                "\n" +
                "  - do:\n" +
                "      catch:      missing\n" +
                "      update:\n" +
                "          index:  test_1\n" +
                "          type:   test\n" +
                "          id:     1\n" +
                "          body:   { doc: { foo: bar } }\n" +
                "\n" +
                "  - do:\n" +
                "      update:\n" +
                "          index: test_1\n" +
                "          type:  test\n" +
                "          id:    1\n" +
                "          body:  { doc: { foo: bar } }\n" +
                "          ignore: 404\n" +
                "\n" +
                "---\n" +
                "\"Missing document (script)\":\n" +
                "\n" +
                "\n" +
                "  - do:\n" +
                "      catch:      missing\n" +
                "      update:\n" +
                "          index:  test_1\n" +
                "          type:   test\n" +
                "          id:     1\n" +
                "          body:\n" +
                "            script: \"ctx._source.foo = bar\"\n" +
                "            params: { bar: 'xxx' }\n" +
                "\n" +
                "  - do:\n" +
                "      update:\n" +
                "          index:  test_1\n" +
                "          type:   test\n" +
                "          id:     1\n" +
                "          ignore: 404\n" +
                "          body:\n" +
                "            script:       \"ctx._source.foo = bar\"\n" +
                "            params:       { bar: 'xxx' }\n"
        );

        RestTestSuiteParser testParser = new RestTestSuiteParser();
        RestTestSuite restTestSuite = testParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(restTestSuite, notNullValue());
        assertThat(restTestSuite.getName(), equalTo("suite"));

        assertThat(restTestSuite.getSetupSection().isEmpty(), equalTo(true));

        assertThat(restTestSuite.getTestSections().size(), equalTo(2));

        assertThat(restTestSuite.getTestSections().get(0).getName(), equalTo("Missing document (partial doc)"));
        assertThat(restTestSuite.getTestSections().get(0).getSkipSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().size(), equalTo(2));

        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(0), instanceOf(DoSection.class));
        DoSection doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(0).getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(4));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));

        assertThat(restTestSuite.getTestSections().get(1).getName(), equalTo("Missing document (script)"));
        assertThat(restTestSuite.getTestSections().get(1).getSkipSection().isEmpty(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().size(), equalTo(2));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(0), instanceOf(DoSection.class));
        assertThat(restTestSuite.getTestSections().get(1).getExecutableSections().get(1), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(1).getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        assertThat(restTestSuite.getTestSections().get(0).getExecutableSections().get(1), instanceOf(DoSection.class));
        doSection = (DoSection) restTestSuite.getTestSections().get(1).getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("update"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(4));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
    }

    public void testParseTestDuplicateTestSections() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "---\n" +
                        "\"Missing document (script)\":\n" +
                        "\n" +
                        "  - do:\n" +
                        "      catch:      missing\n" +
                        "      update:\n" +
                        "          index:  test_1\n" +
                        "          type:   test\n" +
                        "          id:     1\n" +
                        "          body:   { doc: { foo: bar } }\n" +
                        "\n" +
                        "---\n" +
                        "\"Missing document (script)\":\n" +
                        "\n" +
                        "\n" +
                        "  - do:\n" +
                        "      catch:      missing\n" +
                        "      update:\n" +
                        "          index:  test_1\n" +
                        "          type:   test\n" +
                        "          id:     1\n" +
                        "          body:\n" +
                        "            script: \"ctx._source.foo = bar\"\n" +
                        "            params: { bar: 'xxx' }\n" +
                        "\n"
        );

        RestTestSuiteParser testParser = new RestTestSuiteParser();
        try {
            testParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
            fail("Expected RestTestParseException");
        } catch (RestTestParseException e) {
            assertThat(e.getMessage(), containsString("duplicate test section"));
        }
    }
}
