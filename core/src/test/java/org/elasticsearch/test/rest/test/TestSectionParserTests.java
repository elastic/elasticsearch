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
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.parser.RestTestSectionParser;
import org.elasticsearch.test.rest.parser.RestTestSuiteParseContext;
import org.elasticsearch.test.rest.section.*;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.*;

public class TestSectionParserTests extends AbstractParserTestCase {

    @Test
    public void testParseTestSectionWithDoSection() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "\"First test section\": \n" +
                " - do :\n" +
                "     catch: missing\n" +
                "     indices.get_warmer:\n" +
                "         index: test_index\n" +
                "         name: test_warmer"
        );

        RestTestSectionParser testSectionParser = new RestTestSectionParser();
        TestSection testSection = testSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("First test section"));
        assertThat(testSection.getSkipSection(), equalTo(SkipSection.EMPTY));
        assertThat(testSection.getExecutableSections().size(), equalTo(1));
        DoSection doSection = (DoSection)testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_warmer"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
    }

    @Test
    public void testParseTestSectionWithDoSetAndSkipSectionsNoSkip() throws Exception {
        String yaml =
                "\"First test section\": \n" +
                        "  - skip:\n" +
                        "      version:  \"0.90.0 - 0.90.7\"\n" +
                        "      reason:   \"Update doesn't return metadata fields, waiting for #3259\"\n" +
                        "  - do :\n" +
                        "      catch: missing\n" +
                        "      indices.get_warmer:\n" +
                        "          index: test_index\n" +
                        "          name: test_warmer\n" +
                        "  - set: {_scroll_id: scroll_id}";


        RestTestSectionParser testSectionParser = new RestTestSectionParser();
        parser = YamlXContent.yamlXContent.createParser(yaml);
        TestSection testSection = testSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("First test section"));
        assertThat(testSection.getSkipSection(), notNullValue());
        assertThat(testSection.getSkipSection().getLowerVersion(), equalTo(Version.V_0_90_0));
        assertThat(testSection.getSkipSection().getUpperVersion(), equalTo(Version.V_0_90_7));
        assertThat(testSection.getSkipSection().getReason(), equalTo("Update doesn't return metadata fields, waiting for #3259"));
        assertThat(testSection.getExecutableSections().size(), equalTo(2));
        DoSection doSection = (DoSection)testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_warmer"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        SetSection setSection = (SetSection) testSection.getExecutableSections().get(1);
        assertThat(setSection.getStash().size(), equalTo(1));
        assertThat(setSection.getStash().get("_scroll_id"), equalTo("scroll_id"));
    }

    @Test
    public void testParseTestSectionWithMultipleDoSections() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "\"Basic\":\n" +
                        "\n" +
                        "  - do:\n" +
                        "      index:\n" +
                        "        index: test_1\n" +
                        "        type:  test\n" +
                        "        id:    中文\n" +
                        "        body:  { \"foo\": \"Hello: 中文\" }\n" +
                        "  - do:\n" +
                        "      get:\n" +
                        "        index: test_1\n" +
                        "        type:  test\n" +
                        "        id:    中文"
        );

        RestTestSectionParser testSectionParser = new RestTestSectionParser();
        TestSection testSection = testSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("Basic"));
        assertThat(testSection.getSkipSection(), equalTo(SkipSection.EMPTY));
        assertThat(testSection.getExecutableSections().size(), equalTo(2));
        DoSection doSection = (DoSection)testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("index"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));
        doSection = (DoSection)testSection.getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
    }

    @Test
    public void testParseTestSectionWithDoSectionsAndAssertions() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "\"Basic\":\n" +
                        "\n" +
                        "  - do:\n" +
                        "      index:\n" +
                        "        index: test_1\n" +
                        "        type:  test\n" +
                        "        id:    中文\n" +
                        "        body:  { \"foo\": \"Hello: 中文\" }\n" +
                        "\n" +
                        "  - do:\n" +
                        "      get:\n" +
                        "        index: test_1\n" +
                        "        type:  test\n" +
                        "        id:    中文\n" +
                        "\n" +
                        "  - match: { _index:   test_1 }\n" +
                        "  - is_true: _source\n" +
                        "  - match: { _source:  { foo: \"Hello: 中文\" } }\n" +
                        "\n" +
                        "  - do:\n" +
                        "      get:\n" +
                        "        index: test_1\n" +
                        "        id:    中文\n" +
                        "\n" +
                        "  - length: { _index:   6 }\n" +
                        "  - is_false: whatever\n" +
                        "  - gt: { size: 5      }\n" +
                        "  - lt: { size: 10      }"
        );

        RestTestSectionParser testSectionParser = new RestTestSectionParser();
        TestSection testSection = testSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("Basic"));
        assertThat(testSection.getSkipSection(), equalTo(SkipSection.EMPTY));
        assertThat(testSection.getExecutableSections().size(), equalTo(10));

        DoSection doSection = (DoSection)testSection.getExecutableSections().get(0);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("index"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(true));

        doSection = (DoSection)testSection.getExecutableSections().get(1);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));

        MatchAssertion matchAssertion = (MatchAssertion)testSection.getExecutableSections().get(2);
        assertThat(matchAssertion.getField(), equalTo("_index"));
        assertThat(matchAssertion.getExpectedValue().toString(), equalTo("test_1"));

        IsTrueAssertion trueAssertion = (IsTrueAssertion)testSection.getExecutableSections().get(3);
        assertThat(trueAssertion.getField(), equalTo("_source"));

        matchAssertion = (MatchAssertion)testSection.getExecutableSections().get(4);
        assertThat(matchAssertion.getField(), equalTo("_source"));
        assertThat(matchAssertion.getExpectedValue(), instanceOf(Map.class));
        Map map = (Map) matchAssertion.getExpectedValue();
        assertThat(map.size(), equalTo(1));
        assertThat(map.get("foo").toString(), equalTo("Hello: 中文"));

        doSection = (DoSection)testSection.getExecutableSections().get(5);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("get"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));

        LengthAssertion lengthAssertion = (LengthAssertion) testSection.getExecutableSections().get(6);
        assertThat(lengthAssertion.getField(), equalTo("_index"));
        assertThat(lengthAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lengthAssertion.getExpectedValue(), equalTo(6));

        IsFalseAssertion falseAssertion = (IsFalseAssertion)testSection.getExecutableSections().get(7);
        assertThat(falseAssertion.getField(), equalTo("whatever"));

        GreaterThanAssertion greaterThanAssertion = (GreaterThanAssertion) testSection.getExecutableSections().get(8);
        assertThat(greaterThanAssertion.getField(), equalTo("size"));
        assertThat(greaterThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) greaterThanAssertion.getExpectedValue(), equalTo(5));

        LessThanAssertion lessThanAssertion = (LessThanAssertion) testSection.getExecutableSections().get(9);
        assertThat(lessThanAssertion.getField(), equalTo("size"));
        assertThat(lessThanAssertion.getExpectedValue(), instanceOf(Integer.class));
        assertThat((Integer) lessThanAssertion.getExpectedValue(), equalTo(10));
    }

    @Test
    public void testSmallSection() throws Exception {

        parser = YamlXContent.yamlXContent.createParser(
                "\"node_info test\":\n" +
                "  - do:\n" +
                "      cluster.node_info: {}\n" +
                "  \n" +
                "  - is_true: nodes\n" +
                "  - is_true: cluster_name\n");
        RestTestSectionParser testSectionParser = new RestTestSectionParser();
        TestSection testSection = testSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("node_info test"));
        assertThat(testSection.getExecutableSections().size(), equalTo(3));
    }
}
