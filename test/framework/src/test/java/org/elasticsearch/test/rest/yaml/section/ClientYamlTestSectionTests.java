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

package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.Version;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;

import java.io.IOException;
import java.util.Map;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClientYamlTestSectionTests extends AbstractClientYamlTestFragmentParserTestCase {
    public void testAddingDoWithoutWarningWithoutSkip() {
        int lineNumber = between(1, 10000);
        ClientYamlTestSection section = new ClientYamlTestSection(new XContentLocation(0, 0), "test");
        section.setSkipSection(SkipSection.EMPTY);
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        section.addExecutableSection(doSection);
    }

    public void testAddingDoWithWarningWithSkip() {
        int lineNumber = between(1, 10000);
        ClientYamlTestSection section = new ClientYamlTestSection(new XContentLocation(0, 0), "test");
        section.setSkipSection(new SkipSection(null, singletonList("warnings"), null));
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeaders(singletonList("foo"));
        section.addExecutableSection(doSection);
    }

    public void testAddingDoWithWarningWithSkipButNotWarnings() {
        int lineNumber = between(1, 10000);
        ClientYamlTestSection section = new ClientYamlTestSection(new XContentLocation(0, 0), "test");
        section.setSkipSection(new SkipSection(null, singletonList("yaml"), null));
        DoSection doSection = new DoSection(new XContentLocation(lineNumber, 0));
        doSection.setExpectedWarningHeaders(singletonList("foo"));
        Exception e = expectThrows(IllegalArgumentException.class, () -> section.addExecutableSection(doSection));
        assertEquals("Attempted to add a [do] with a [warnings] section without a corresponding [skip] so runners that do not support the"
                + " [warnings] section can skip the test at line [" + lineNumber + "]", e.getMessage());
    }

    public void testWrongIndentation() throws Exception {
        {
            XContentParser parser = createParser(YamlXContent.yamlXContent,
                    "\"First test section\": \n" +
                            "  - skip:\n" +
                            "    version:  \"2.0.0 - 2.2.0\"\n" +
                            "    reason:   \"Update doesn't return metadata fields, waiting for #3259\"");

            ParsingException e = expectThrows(ParsingException.class, () -> ClientYamlTestSection.parse(parser));
            assertEquals("Error parsing test named [First test section]", e.getMessage());
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("Expected [START_OBJECT, found [VALUE_NULL], the skip section is not properly indented",
                    e.getCause().getMessage());
        }
        {
            XContentParser parser = createParser(YamlXContent.yamlXContent,
                    "\"First test section\": \n" +
                            " - do :\n" +
                            "   catch: missing\n" +
                            "   indices.get_warmer:\n" +
                            "       index: test_index\n" +
                            "       name: test_warmer"
            );
            ParsingException e = expectThrows(ParsingException.class, () -> ClientYamlTestSection.parse(parser));
            assertEquals("Error parsing test named [First test section]", e.getMessage());
            assertThat(e.getCause(), instanceOf(IOException.class));
            assertThat(e.getCause().getCause(), instanceOf(IllegalArgumentException.class));
            assertEquals("expected [START_OBJECT], found [VALUE_NULL], the do section is not properly indented",
                    e.getCause().getCause().getMessage());
        }
    }

    public void testParseTestSectionWithDoSection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "\"First test section\": \n" +
                " - do :\n" +
                "     catch: missing\n" +
                "     indices.get_warmer:\n" +
                "         index: test_index\n" +
                "         name: test_warmer"
        );

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

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

    public void testParseTestSectionWithDoSetAndSkipSectionsNoSkip() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "\"First test section\": \n" +
                        "  - skip:\n" +
                        "      version:  \"5.0.0 - 5.2.0\"\n" +
                        "      reason:   \"Update doesn't return metadata fields, waiting for #3259\"\n" +
                        "  - do :\n" +
                        "      catch: missing\n" +
                        "      indices.get_warmer:\n" +
                        "          index: test_index\n" +
                        "          name: test_warmer\n" +
                        "  - set: {_scroll_id: scroll_id}");


        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("First test section"));
        assertThat(testSection.getSkipSection(), notNullValue());
        assertThat(testSection.getSkipSection().getLowerVersion(), equalTo(Version.V_5_0_0));
        assertThat(testSection.getSkipSection().getUpperVersion(),
                equalTo(Version.V_5_2_0));
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

    public void testParseTestSectionWithMultipleDoSections() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
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

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

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

    public void testParseTestSectionWithDoSectionsAndAssertions() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
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

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);

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

    public void testSmallSection() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "\"node_info test\":\n" +
                "  - do:\n" +
                "      cluster.node_info: {}\n" +
                "  \n" +
                "  - is_true: nodes\n" +
                "  - is_true: cluster_name\n");

        ClientYamlTestSection testSection = ClientYamlTestSection.parse(parser);
        assertThat(testSection, notNullValue());
        assertThat(testSection.getName(), equalTo("node_info test"));
        assertThat(testSection.getExecutableSections().size(), equalTo(3));
    }

}
