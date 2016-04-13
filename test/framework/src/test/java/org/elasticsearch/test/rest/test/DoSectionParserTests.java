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

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.parser.DoSectionParser;
import org.elasticsearch.test.rest.parser.RestTestParseException;
import org.elasticsearch.test.rest.parser.RestTestSuiteParseContext;
import org.elasticsearch.test.rest.section.ApiCallSection;
import org.elasticsearch.test.rest.section.DoSection;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class DoSectionParserTests extends AbstractParserTestCase {
    public void testParseDoSectionNoBody() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "get:\n" +
                "    index:    test_index\n" +
                "    type:    test_type\n" +
                "    id:        1"
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("get"));
        assertThat(apiCallSection.getParams().size(), equalTo(3));
        assertThat(apiCallSection.getParams().get("index"), equalTo("test_index"));
        assertThat(apiCallSection.getParams().get("type"), equalTo("test_type"));
        assertThat(apiCallSection.getParams().get("id"), equalTo("1"));
        assertThat(apiCallSection.hasBody(), equalTo(false));
    }

    public void testParseDoSectionNoParamsNoBody() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "cluster.node_info: {}"
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("cluster.node_info"));
        assertThat(apiCallSection.getParams().size(), equalTo(0));
        assertThat(apiCallSection.hasBody(), equalTo(false));
    }

    public void testParseDoSectionWithJsonBody() throws Exception {
        String body = "{ \"include\": { \"field1\": \"v1\", \"field2\": \"v2\" }, \"count\": 1 }";
        parser = YamlXContent.yamlXContent.createParser(
                "index:\n" +
                "    index:  test_1\n" +
                "    type:   test\n" +
                "    id:     1\n" +
                "    body:   " + body
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("index"));
        assertThat(apiCallSection.getParams().size(), equalTo(3));
        assertThat(apiCallSection.getParams().get("index"), equalTo("test_1"));
        assertThat(apiCallSection.getParams().get("type"), equalTo("test"));
        assertThat(apiCallSection.getParams().get("id"), equalTo("1"));
        assertThat(apiCallSection.hasBody(), equalTo(true));

        assertJsonEquals(apiCallSection.getBodies().get(0), body);
    }

    public void testParseDoSectionWithJsonMultipleBodiesAsLongString() throws Exception {
        String bodies[] = new String[]{
                "{ \"index\": { \"_index\":\"test_index\", \"_type\":\"test_type\", \"_id\":\"test_id\" } }\n",
                "{ \"f1\":\"v1\", \"f2\":42 }\n",
                "{ \"index\": { \"_index\":\"test_index2\", \"_type\":\"test_type2\", \"_id\":\"test_id2\" } }\n",
                "{ \"f1\":\"v2\", \"f2\":47 }\n"
        };
        parser = YamlXContent.yamlXContent.createParser(
                "bulk:\n" +
                "    refresh: true\n" +
                "    body: |\n" +
                "        " + bodies[0] +
                "        " + bodies[1] +
                "        " + bodies[2] +
                "        " + bodies[3]
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("bulk"));
        assertThat(apiCallSection.getParams().size(), equalTo(1));
        assertThat(apiCallSection.getParams().get("refresh"), equalTo("true"));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(4));
    }

    public void testParseDoSectionWithJsonMultipleBodiesRepeatedProperty() throws Exception {
        String[] bodies = new String[] {
                "{ \"index\": { \"_index\":\"test_index\", \"_type\":\"test_type\", \"_id\":\"test_id\" } }",
                "{ \"f1\":\"v1\", \"f2\":42 }",
        };
        parser = YamlXContent.yamlXContent.createParser(
                "bulk:\n" +
                "    refresh: true\n" +
                "    body: \n" +
                "        " + bodies[0] + "\n" +
                "    body: \n" +
                "        " + bodies[1]
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("bulk"));
        assertThat(apiCallSection.getParams().size(), equalTo(1));
        assertThat(apiCallSection.getParams().get("refresh"), equalTo("true"));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(bodies.length));
        for (int i = 0; i < bodies.length; i++) {
            assertJsonEquals(apiCallSection.getBodies().get(i), bodies[i]);
        }
    }

    public void testParseDoSectionWithYamlBody() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "search:\n" +
                "    body:\n" +
                "        \"_source\": [ include.field1, include.field2 ]\n" +
                "        \"query\": { \"match_all\": {} }"
        );
        String body = "{ \"_source\": [ \"include.field1\", \"include.field2\" ], \"query\": { \"match_all\": {} }}";

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("search"));
        assertThat(apiCallSection.getParams().size(), equalTo(0));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(1));
        assertJsonEquals(apiCallSection.getBodies().get(0), body);
    }

    public void testParseDoSectionWithYamlMultipleBodies() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "bulk:\n" +
                "    refresh: true\n" +
                "    body:\n" +
                "        - index:\n" +
                "            _index: test_index\n" +
                "            _type:  test_type\n" +
                "            _id:    test_id\n" +
                "        - f1: v1\n" +
                "          f2: 42\n" +
                "        - index:\n" +
                "            _index: test_index2\n" +
                "            _type:  test_type2\n" +
                "            _id:    test_id2\n" +
                "        - f1: v2\n" +
                "          f2: 47"
        );
        String[] bodies = new String[4];
        bodies[0] = "{\"index\": {\"_index\": \"test_index\", \"_type\":  \"test_type\", \"_id\": \"test_id\"}}";
        bodies[1] = "{ \"f1\":\"v1\", \"f2\": 42 }";
        bodies[2] = "{\"index\": {\"_index\": \"test_index2\", \"_type\":  \"test_type2\", \"_id\": \"test_id2\"}}";
        bodies[3] = "{ \"f1\":\"v2\", \"f2\": 47 }";

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("bulk"));
        assertThat(apiCallSection.getParams().size(), equalTo(1));
        assertThat(apiCallSection.getParams().get("refresh"), equalTo("true"));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(bodies.length));

        for (int i = 0; i < bodies.length; i++) {
            assertJsonEquals(apiCallSection.getBodies().get(i), bodies[i]);
        }
    }

    public void testParseDoSectionWithYamlMultipleBodiesRepeatedProperty() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "bulk:\n" +
                "    refresh: true\n" +
                "    body:\n" +
                "        index:\n" +
                "            _index: test_index\n" +
                "            _type:  test_type\n" +
                "            _id:    test_id\n" +
                "    body:\n" +
                "        f1: v1\n" +
                "        f2: 42\n"
        );
        String[] bodies = new String[2];
        bodies[0] = "{\"index\": {\"_index\": \"test_index\", \"_type\":  \"test_type\", \"_id\": \"test_id\"}}";
        bodies[1] = "{ \"f1\":\"v1\", \"f2\": 42 }";

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("bulk"));
        assertThat(apiCallSection.getParams().size(), equalTo(1));
        assertThat(apiCallSection.getParams().get("refresh"), equalTo("true"));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(bodies.length));

        for (int i = 0; i < bodies.length; i++) {
            assertJsonEquals(apiCallSection.getBodies().get(i), bodies[i]);
        }
    }

    public void testParseDoSectionWithYamlBodyMultiGet() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "mget:\n" +
                "    body:\n" +
                "        docs:\n" +
                "            - { _index: test_2, _type: test, _id: 1}\n" +
                "            - { _index: test_1, _type: none, _id: 1}"
        );
        String body = "{ \"docs\": [ " +
                "{\"_index\": \"test_2\", \"_type\":\"test\", \"_id\":1}, " +
                "{\"_index\": \"test_1\", \"_type\":\"none\", \"_id\":1} " +
                "]}";

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("mget"));
        assertThat(apiCallSection.getParams().size(), equalTo(0));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(1));
        assertJsonEquals(apiCallSection.getBodies().get(0), body);
    }

    public void testParseDoSectionWithBodyStringified() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "index:\n" +
                "    index:  test_1\n" +
                "    type:   test\n" +
                "    id:     1\n" +
                "    body:   \"{ \\\"_source\\\": true, \\\"query\\\": { \\\"match_all\\\": {} } }\""
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("index"));
        assertThat(apiCallSection.getParams().size(), equalTo(3));
        assertThat(apiCallSection.getParams().get("index"), equalTo("test_1"));
        assertThat(apiCallSection.getParams().get("type"), equalTo("test"));
        assertThat(apiCallSection.getParams().get("id"), equalTo("1"));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(1));
        //stringified body is taken as is
        assertJsonEquals(apiCallSection.getBodies().get(0), "{ \"_source\": true, \"query\": { \"match_all\": {} } }");
    }

    public void testParseDoSectionWithBodiesStringifiedAndNot() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "index:\n" +
                "    body:\n" +
                "        - \"{ \\\"_source\\\": true, \\\"query\\\": { \\\"match_all\\\": {} } }\"\n" +
                "        - { size: 100, query: { match_all: {} } }"
        );

        String body = "{ \"size\": 100, \"query\": { \"match_all\": {} } }";

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection.getApi(), equalTo("index"));
        assertThat(apiCallSection.getParams().size(), equalTo(0));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(2));
        //stringified body is taken as is
        assertJsonEquals(apiCallSection.getBodies().get(0), "{ \"_source\": true, \"query\": { \"match_all\": {} } }");
        assertJsonEquals(apiCallSection.getBodies().get(1), body);
    }

    public void testParseDoSectionWithCatch() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "catch: missing\n" +
                "indices.get_warmer:\n" +
                "    index: test_index\n" +
                "    name: test_warmer"
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_warmer"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
    }

    public void testParseDoSectionWithHeaders() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "headers:\n" +
                        "    Authorization: \"thing one\"\n" +
                        "    Content-Type: \"application/json\"\n" +
                        "indices.get_warmer:\n" +
                        "    index: test_index\n" +
                        "    name: test_warmer"
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_warmer"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        assertThat(doSection.getApiCallSection().getHeaders(), notNullValue());
        assertThat(doSection.getApiCallSection().getHeaders().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().getHeaders().get("Authorization"), equalTo("thing one"));
        assertThat(doSection.getApiCallSection().getHeaders().get("Content-Type"), equalTo("application/json"));
    }

    public void testParseDoSectionWithoutClientCallSection() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "catch: missing\n"
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        try {
            doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));
            fail("Expected RestTestParseException");
        } catch (RestTestParseException e) {
            assertThat(e.getMessage(), is("client call section is mandatory within a do section"));
        }
    }

    public void testParseDoSectionMultivaluedField() throws Exception {
        parser = YamlXContent.yamlXContent.createParser(
                "indices.get_field_mapping:\n" +
                        "        index: test_index\n" +
                        "        type: test_type\n" +
                        "        field: [ text , text1 ]"
        );

        DoSectionParser doSectionParser = new DoSectionParser();
        DoSection doSection = doSectionParser.parse(new RestTestSuiteParseContext("api", "suite", parser));

        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_field_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(3));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        assertThat(doSection.getApiCallSection().getParams().get("type"), equalTo("test_type"));
        assertThat(doSection.getApiCallSection().getParams().get("field"), equalTo("text,text1"));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        assertThat(doSection.getApiCallSection().getBodies().size(), equalTo(0));
    }

    private static void assertJsonEquals(Map<String, Object> actual, String expected) throws IOException {
        Map<String,Object> expectedMap;
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(expected)) {
            expectedMap = parser.mapOrdered();
        }
        MatcherAssert.assertThat(actual, equalTo(expectedMap));
    }
}
