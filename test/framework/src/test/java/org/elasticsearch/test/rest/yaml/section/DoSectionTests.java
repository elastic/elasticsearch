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

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.NodeSelector;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.logging.HeaderWarning;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.yaml.YamlXContent;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;
import org.elasticsearch.test.rest.yaml.ClientYamlTestResponse;
import org.hamcrest.MatcherAssert;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DoSectionTests extends AbstractClientYamlTestFragmentParserTestCase {

    public void testWarningHeaders() {
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));

            // No warning headers doesn't throw an exception
            section.checkWarningHeaders(emptyList(), Version.CURRENT);
        }

        final String testHeader = HeaderWarning.formatWarning("test");
        final String anotherHeader = HeaderWarning.formatWarning("another");
        final String someMoreHeader = HeaderWarning.formatWarning("some more");
        final String catHeader = HeaderWarning.formatWarning("cat");
        // Any warning headers fail
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));

            final AssertionError one = expectThrows(AssertionError.class, () ->
                    section.checkWarningHeaders(singletonList(testHeader), Version.CURRENT));
            assertEquals("got unexpected warning header [\n\t" + testHeader + "\n]\n", one.getMessage());

            final AssertionError multiple = expectThrows(AssertionError.class, () ->
                    section.checkWarningHeaders(Arrays.asList(testHeader, anotherHeader, someMoreHeader), Version.CURRENT));
            assertEquals(
                    "got unexpected warning headers [\n\t" +
                            testHeader + "\n\t" +
                            anotherHeader + "\n\t" +
                            someMoreHeader + "\n]\n",
                    multiple.getMessage());
        }

        // But not when we expect them
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));
            section.setExpectedWarningHeaders(singletonList("test"));
            section.checkWarningHeaders(singletonList(testHeader), Version.CURRENT);
        }
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));
            section.setExpectedWarningHeaders(Arrays.asList("test", "another", "some more"));
            section.checkWarningHeaders(Arrays.asList(testHeader, anotherHeader, someMoreHeader), Version.CURRENT);
        }

        // But if you don't get some that you did expect, that is an error
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));
            section.setExpectedWarningHeaders(singletonList("test"));
            final AssertionError e = expectThrows(AssertionError.class, () -> section.checkWarningHeaders(emptyList(), Version.CURRENT));
            assertEquals("did not get expected warning header [\n\ttest\n]\n", e.getMessage());
        }
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));
            section.setExpectedWarningHeaders(Arrays.asList("test", "another", "some more"));

            final AssertionError multiple = expectThrows(AssertionError.class, () ->
                    section.checkWarningHeaders(emptyList(), Version.CURRENT));
            assertEquals("did not get expected warning headers [\n\ttest\n\tanother\n\tsome more\n]\n", multiple.getMessage());

            final AssertionError one = expectThrows(AssertionError.class, () ->
                    section.checkWarningHeaders(Arrays.asList(testHeader, someMoreHeader), Version.CURRENT));
            assertEquals("did not get expected warning header [\n\tanother\n]\n", one.getMessage());
        }

        // It is also an error if you get some warning you want and some you don't want
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));
            section.setExpectedWarningHeaders(Arrays.asList("test", "another", "some more"));
            final AssertionError e = expectThrows(AssertionError.class, () ->
                    section.checkWarningHeaders(Arrays.asList(testHeader, catHeader), Version.CURRENT));
            assertEquals("got unexpected warning header [\n\t" +
                            catHeader + "\n]\n" +
                            "did not get expected warning headers [\n\tanother\n\tsome more\n]\n",
                    e.getMessage());
        }

        // "allowed" warnings are fine
        {
            final DoSection section = new DoSection(new XContentLocation(1, 1));
            section.setAllowedWarningHeaders(singletonList("test"));
            section.checkWarningHeaders(singletonList(testHeader), Version.CURRENT);
            // and don't throw exceptions if we don't receive them
            section.checkWarningHeaders(emptyList(), Version.CURRENT);
        }
    }

    public void testParseDoSectionNoBody() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "get:\n" +
                "    index:    test_index\n" +
                "    type:    test_type\n" +
                "    id:        1"
        );

        DoSection doSection = DoSection.parse(parser);
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
        parser = createParser(YamlXContent.yamlXContent,
                "cluster.node_info: {}"
        );

        DoSection doSection = DoSection.parse(parser);
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("cluster.node_info"));
        assertThat(apiCallSection.getParams().size(), equalTo(0));
        assertThat(apiCallSection.hasBody(), equalTo(false));
    }

    public void testParseDoSectionWithJsonBody() throws Exception {
        String body = "{ \"include\": { \"field1\": \"v1\", \"field2\": \"v2\" }, \"count\": 1 }";
        parser = createParser(YamlXContent.yamlXContent,
                "index:\n" +
                "    index:  test_1\n" +
                "    type:   test\n" +
                "    id:     1\n" +
                "    body:   " + body
        );

        DoSection doSection = DoSection.parse(parser);
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
        parser = createParser(YamlXContent.yamlXContent,
                "bulk:\n" +
                "    refresh: true\n" +
                "    body: |\n" +
                "        " + bodies[0] +
                "        " + bodies[1] +
                "        " + bodies[2] +
                "        " + bodies[3]
        );

        DoSection doSection = DoSection.parse(parser);
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("bulk"));
        assertThat(apiCallSection.getParams().size(), equalTo(1));
        assertThat(apiCallSection.getParams().get("refresh"), equalTo("true"));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(4));
    }

    public void testParseDoSectionWithYamlBody() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "search:\n" +
                "    body:\n" +
                "        \"_source\": [ include.field1, include.field2 ]\n" +
                "        \"query\": { \"match_all\": {} }"
        );
        String body = "{ \"_source\": [ \"include.field1\", \"include.field2\" ], \"query\": { \"match_all\": {} }}";

        DoSection doSection = DoSection.parse(parser);
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("search"));
        assertThat(apiCallSection.getParams().size(), equalTo(0));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(1));
        assertJsonEquals(apiCallSection.getBodies().get(0), body);
    }

    public void testParseDoSectionWithYamlMultipleBodies() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
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

        DoSection doSection = DoSection.parse(parser);
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
        parser = createParser(YamlXContent.yamlXContent,
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

        DoSection doSection = DoSection.parse(parser);
        ApiCallSection apiCallSection = doSection.getApiCallSection();

        assertThat(apiCallSection, notNullValue());
        assertThat(apiCallSection.getApi(), equalTo("mget"));
        assertThat(apiCallSection.getParams().size(), equalTo(0));
        assertThat(apiCallSection.hasBody(), equalTo(true));
        assertThat(apiCallSection.getBodies().size(), equalTo(1));
        assertJsonEquals(apiCallSection.getBodies().get(0), body);
    }

    public void testParseDoSectionWithBodyStringified() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "index:\n" +
                "    index:  test_1\n" +
                "    type:   test\n" +
                "    id:     1\n" +
                "    body:   \"{ \\\"_source\\\": true, \\\"query\\\": { \\\"match_all\\\": {} } }\""
        );

        DoSection doSection = DoSection.parse(parser);
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
        parser = createParser(YamlXContent.yamlXContent,
                "index:\n" +
                "    body:\n" +
                "        - \"{ \\\"_source\\\": true, \\\"query\\\": { \\\"match_all\\\": {} } }\"\n" +
                "        - { size: 100, query: { match_all: {} } }"
        );

        String body = "{ \"size\": 100, \"query\": { \"match_all\": {} } }";

        DoSection doSection = DoSection.parse(parser);
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
        parser = createParser(YamlXContent.yamlXContent,
                "catch: missing\n" +
                "indices.get_warmer:\n" +
                "    index: test_index\n" +
                "    name: test_warmer"
        );

        DoSection doSection = DoSection.parse(parser);
        assertThat(doSection.getCatch(), equalTo("missing"));
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_warmer"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
    }

    public void testUnsupportedTopLevelField() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
            "max_concurrent_shard_requests: 1"
        );

        ParsingException e = expectThrows(ParsingException.class, () -> DoSection.parse(parser));
        assertThat(e.getMessage(), is("unsupported field [max_concurrent_shard_requests]"));
        parser.nextToken();
        parser.nextToken();
    }

    public void testParseDoSectionWithHeaders() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "headers:\n" +
                "    Authorization: \"thing one\"\n" +
                "    Content-Type: \"application/json\"\n" +
                "indices.get_warmer:\n" +
                "    index: test_index\n" +
                "    name: test_warmer"
        );

        DoSection doSection = DoSection.parse(parser);
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
        parser = createParser(YamlXContent.yamlXContent,
                "catch: missing\n"
        );

        Exception e = expectThrows(IllegalArgumentException.class, () -> DoSection.parse(parser));
        assertThat(e.getMessage(), is("client call section is mandatory within a do section"));
    }

    public void testParseDoSectionMultivaluedField() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "indices.get_field_mapping:\n" +
                "        index: test_index\n" +
                "        type: test_type\n" +
                "        field: [ text , text1 ]"
        );

        DoSection doSection = DoSection.parse(parser);
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

    public void testParseDoSectionExpectedWarnings() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "indices.get_field_mapping:\n" +
                "        index: test_index\n" +
                "        type: test_type\n" +
                "warnings:\n" +
                "    - some test warning they are typically pretty long\n" +
                "    - some other test warning sometimes they have [in] them"
        );

        DoSection doSection = DoSection.parse(parser);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_field_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        assertThat(doSection.getApiCallSection().getParams().get("type"), equalTo("test_type"));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        assertThat(doSection.getApiCallSection().getBodies().size(), equalTo(0));
        assertThat(doSection.getExpectedWarningHeaders(), equalTo(Arrays.asList(
                "some test warning they are typically pretty long",
                "some other test warning sometimes they have [in] them")));

        parser = createParser(YamlXContent.yamlXContent,
                "indices.get_field_mapping:\n" +
                "        index: test_index\n" +
                "warnings:\n" +
                "    - just one entry this time"
        );

        doSection = DoSection.parse(parser);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getExpectedWarningHeaders(), equalTo(singletonList(
                "just one entry this time")));
    }

    public void testParseDoSectionAllowedWarnings() throws Exception {
        parser = createParser(YamlXContent.yamlXContent,
                "indices.get_field_mapping:\n" +
                "        index: test_index\n" +
                "        type: test_type\n" +
                "allowed_warnings:\n" +
                "    - some test warning they are typically pretty long\n" +
                "    - some other test warning sometimes they have [in] them"
        );

        DoSection doSection = DoSection.parse(parser);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getApiCallSection().getApi(), equalTo("indices.get_field_mapping"));
        assertThat(doSection.getApiCallSection().getParams().size(), equalTo(2));
        assertThat(doSection.getApiCallSection().getParams().get("index"), equalTo("test_index"));
        assertThat(doSection.getApiCallSection().getParams().get("type"), equalTo("test_type"));
        assertThat(doSection.getApiCallSection().hasBody(), equalTo(false));
        assertThat(doSection.getApiCallSection().getBodies().size(), equalTo(0));
        assertThat(doSection.getAllowedWarningHeaders(), equalTo(Arrays.asList(
                "some test warning they are typically pretty long",
                "some other test warning sometimes they have [in] them")));

        parser = createParser(YamlXContent.yamlXContent,
                "indices.get_field_mapping:\n" +
                "        index: test_index\n" +
                "allowed_warnings:\n" +
                "    - just one entry this time"
        );

        doSection = DoSection.parse(parser);
        assertThat(doSection.getCatch(), nullValue());
        assertThat(doSection.getApiCallSection(), notNullValue());
        assertThat(doSection.getAllowedWarningHeaders(), equalTo(singletonList(
                "just one entry this time")));

        parser = createParser(YamlXContent.yamlXContent,
                "indices.get_field_mapping:\n" +
                "        index: test_index\n" +
                "warnings:\n" +
                "    - foo\n" +
                "allowed_warnings:\n" +
                "    - foo"
        );
        Exception e = expectThrows(IllegalArgumentException.class, () -> DoSection.parse(parser));
        assertThat(e.getMessage(), equalTo("the warning [foo] was both allowed and expected"));
    }

    public void testNodeSelectorByVersion() throws IOException {
        parser = createParser(YamlXContent.yamlXContent,
                "node_selector:\n" +
                "    version: 5.2.0-6.0.0\n" +
                "indices.get_field_mapping:\n" +
                "    index: test_index"
        );

        DoSection doSection = DoSection.parse(parser);
        assertNotSame(NodeSelector.ANY, doSection.getApiCallSection().getNodeSelector());
        Node v170 = nodeWithVersion("1.7.0");
        Node v521 = nodeWithVersion("5.2.1");
        Node v550 = nodeWithVersion("5.5.0");
        Node v612 = nodeWithVersion("6.1.2");
        List<Node> nodes = new ArrayList<>();
        nodes.add(v170);
        nodes.add(v521);
        nodes.add(v550);
        nodes.add(v612);
        doSection.getApiCallSection().getNodeSelector().select(nodes);
        assertEquals(Arrays.asList(v521, v550), nodes);
        ClientYamlTestExecutionContext context = mock(ClientYamlTestExecutionContext.class);
        ClientYamlTestResponse mockResponse = mock(ClientYamlTestResponse.class);
        when(context.callApi("indices.get_field_mapping", singletonMap("index", "test_index"),
                emptyList(), emptyMap(), doSection.getApiCallSection().getNodeSelector())).thenReturn(mockResponse);
        doSection.execute(context);
        verify(context).callApi("indices.get_field_mapping", singletonMap("index", "test_index"),
                emptyList(), emptyMap(), doSection.getApiCallSection().getNodeSelector());

        {
            List<Node> badNodes = new ArrayList<>();
            badNodes.add(new Node(new HttpHost("dummy")));
            Exception e = expectThrows(IllegalStateException.class, () ->
                    doSection.getApiCallSection().getNodeSelector().select(badNodes));
            assertEquals("expected [version] metadata to be set but got [host=http://dummy]",
                    e.getMessage());
        }
    }

    private static Node nodeWithVersion(String version) {
        return new Node(new HttpHost("dummy"), null, null, version, null, null);
    }

    public void testNodeSelectorByAttribute() throws IOException {
        parser = createParser(YamlXContent.yamlXContent,
                "node_selector:\n" +
                "    attribute:\n" +
                "        attr: val\n" +
                "indices.get_field_mapping:\n" +
                "    index: test_index"
        );

        DoSection doSection = DoSection.parse(parser);
        assertNotSame(NodeSelector.ANY, doSection.getApiCallSection().getNodeSelector());
        Node hasAttr = nodeWithAttributes(singletonMap("attr", singletonList("val")));
        Node hasAttrWrongValue = nodeWithAttributes(singletonMap("attr", singletonList("notval")));
        Node notHasAttr = nodeWithAttributes(singletonMap("notattr", singletonList("val")));
        {
            List<Node> nodes = new ArrayList<>();
            nodes.add(hasAttr);
            nodes.add(hasAttrWrongValue);
            nodes.add(notHasAttr);
            doSection.getApiCallSection().getNodeSelector().select(nodes);
            assertEquals(Arrays.asList(hasAttr), nodes);
        }
        {
            List<Node> badNodes = new ArrayList<>();
            badNodes.add(new Node(new HttpHost("dummy")));
            Exception e = expectThrows(IllegalStateException.class, () ->
                    doSection.getApiCallSection().getNodeSelector().select(badNodes));
            assertEquals("expected [attributes] metadata to be set but got [host=http://dummy]",
                    e.getMessage());
        }

        parser = createParser(YamlXContent.yamlXContent,
                "node_selector:\n" +
                "    attribute:\n" +
                "        attr: val\n" +
                "        attr2: val2\n" +
                "indices.get_field_mapping:\n" +
                "    index: test_index"
        );

        DoSection doSectionWithTwoAttributes = DoSection.parse(parser);
        assertNotSame(NodeSelector.ANY, doSection.getApiCallSection().getNodeSelector());
        Node hasAttr2 = nodeWithAttributes(singletonMap("attr2", singletonList("val2")));
        Map<String, List<String>> bothAttributes = new HashMap<>();
        bothAttributes.put("attr", singletonList("val"));
        bothAttributes.put("attr2", singletonList("val2"));
        Node hasBoth = nodeWithAttributes(bothAttributes);
        {
            List<Node> nodes = new ArrayList<>();
            nodes.add(hasAttr);
            nodes.add(hasAttrWrongValue);
            nodes.add(notHasAttr);
            nodes.add(hasAttr2);
            nodes.add(hasBoth);
            doSectionWithTwoAttributes.getApiCallSection().getNodeSelector().select(nodes);
            assertEquals(Arrays.asList(hasBoth), nodes);
        }
    }

    private static Node nodeWithAttributes(Map<String, List<String>> attributes) {
        return new Node(new HttpHost("dummy"), null, null, null, null, attributes);
    }

    public void testNodeSelectorByTwoThings() throws IOException {
        parser = createParser(YamlXContent.yamlXContent,
                "node_selector:\n" +
                "    version: 5.2.0-6.0.0\n" +
                "    attribute:\n" +
                "        attr: val\n" +
                "indices.get_field_mapping:\n" +
                "    index: test_index"
        );

        DoSection doSection = DoSection.parse(parser);
        assertNotSame(NodeSelector.ANY, doSection.getApiCallSection().getNodeSelector());
        Node both = nodeWithVersionAndAttributes("5.2.1", singletonMap("attr", singletonList("val")));
        Node badVersion = nodeWithVersionAndAttributes("5.1.1", singletonMap("attr", singletonList("val")));
        Node badAttr = nodeWithVersionAndAttributes("5.2.1", singletonMap("notattr", singletonList("val")));
        List<Node> nodes = new ArrayList<>();
        nodes.add(both);
        nodes.add(badVersion);
        nodes.add(badAttr);
        doSection.getApiCallSection().getNodeSelector().select(nodes);
        assertEquals(Arrays.asList(both), nodes);
    }

    private static Node nodeWithVersionAndAttributes(String version, Map<String, List<String>> attributes) {
        return new Node(new HttpHost("dummy"), null, null, version, null, attributes);
    }

    private void assertJsonEquals(Map<String, Object> actual, String expected) throws IOException {
        Map<String,Object> expectedMap;
        try (XContentParser parser = createParser(YamlXContent.yamlXContent, expected)) {
            expectedMap = parser.mapOrdered();
        }
        MatcherAssert.assertThat(actual, equalTo(expectedMap));
    }
}
