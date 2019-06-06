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

package org.elasticsearch.script.mustache;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParseException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;

public class SearchTemplateRequestXContentTests extends AbstractXContentTestCase<SearchTemplateRequest> {

    @Override
    public SearchTemplateRequest createTestInstance() {
        return SearchTemplateRequestTests.createRandomRequest();
    }

    @Override
    protected SearchTemplateRequest doParseInstance(XContentParser parser) throws IOException {
        return SearchTemplateRequest.fromXContent(parser);
    }

    /**
     * Note that when checking equality for xContent parsing, we omit two parts of the request:
     * - The 'simulate' option, since this parameter is not included in the
     *   request's xContent (it's instead used to determine the request endpoint).
     * - The random SearchRequest, since this component only affects the request
     *   parameters and also isn't captured in the request's xContent.
     */
    @Override
    protected void assertEqualInstances(SearchTemplateRequest expectedInstance, SearchTemplateRequest newInstance) {
        assertTrue(
            expectedInstance.isExplain() == newInstance.isExplain() &&
            expectedInstance.isProfile() == newInstance.isProfile() &&
            expectedInstance.getScriptType() == newInstance.getScriptType() &&
            Objects.equals(expectedInstance.getScript(), newInstance.getScript()) &&
            Objects.equals(expectedInstance.getScriptParams(), newInstance.getScriptParams()));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testToXContentWithInlineTemplate() throws IOException {
        SearchTemplateRequest request = new SearchTemplateRequest();

        request.setScriptType(ScriptType.INLINE);
        request.setScript("{\"query\": { \"match\" : { \"{{my_field}}\" : \"{{my_value}}\" } } }");
        request.setProfile(true);

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("my_field", "foo");
        scriptParams.put("my_value", "bar");
        request.setScriptParams(scriptParams);

        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
                .field("source", "{\"query\": { \"match\" : { \"{{my_field}}\" : \"{{my_value}}\" } } }")
                .startObject("params")
                    .field("my_field", "foo")
                    .field("my_value", "bar")
                .endObject()
                .field("explain", false)
                .field("profile", true)
            .endObject();

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        request.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedRequest),
            BytesReference.bytes(actualRequest),
            contentType);
    }

    public void testToXContentWithStoredTemplate() throws IOException {
        SearchTemplateRequest request = new SearchTemplateRequest();

        request.setScriptType(ScriptType.STORED);
        request.setScript("match_template");
        request.setExplain(true);

        Map<String, Object> params = new HashMap<>();
        params.put("my_field", "foo");
        params.put("my_value", "bar");
        request.setScriptParams(params);

        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedRequest = XContentFactory.contentBuilder(contentType)
            .startObject()
                .field("id", "match_template")
                .startObject("params")
                    .field("my_field", "foo")
                    .field("my_value", "bar")
                .endObject()
                .field("explain", true)
                .field("profile", false)
            .endObject();

        XContentBuilder actualRequest = XContentFactory.contentBuilder(contentType);
        request.toXContent(actualRequest, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(
            BytesReference.bytes(expectedRequest),
            BytesReference.bytes(actualRequest),
            contentType);
    }

    public void testFromXContentWithEmbeddedTemplate() throws Exception {
        String source = "{" +
                "    'source' : {\n" +
                "    'query': {\n" +
                "      'terms': {\n" +
                "        'status': [\n" +
                "          '{{#status}}',\n" +
                "          '{{.}}',\n" +
                "          '{{/status}}'\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  }" +
                "}";

        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(newParser(source));
        assertThat(request.getScript(), equalTo("{\"query\":{\"terms\":{\"status\":[\"{{#status}}\",\"{{.}}\",\"{{/status}}\"]}}}"));
        assertThat(request.getScriptType(), equalTo(ScriptType.INLINE));
        assertThat(request.getScriptParams(), nullValue());
    }

    public void testFromXContentWithEmbeddedTemplateAndParams() throws Exception {
        String source = "{" +
            "    'source' : {" +
            "      'query': { 'match' : { '{{my_field}}' : '{{my_value}}' } }," +
            "      'size' : '{{my_size}}'" +
            "    }," +
            "    'params' : {" +
            "        'my_field' : 'foo'," +
            "        'my_value' : 'bar'," +
            "        'my_size' : 5" +
            "    }" +
            "}";

        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(newParser(source));
        assertThat(request.getScript(), equalTo("{\"query\":{\"match\":{\"{{my_field}}\":\"{{my_value}}\"}},\"size\":\"{{my_size}}\"}"));
        assertThat(request.getScriptType(), equalTo(ScriptType.INLINE));
        assertThat(request.getScriptParams().size(), equalTo(3));
        assertThat(request.getScriptParams(), hasEntry("my_field", "foo"));
        assertThat(request.getScriptParams(), hasEntry("my_value", "bar"));
        assertThat(request.getScriptParams(), hasEntry("my_size", 5));
    }

    public void testFromXContentWithMalformedRequest() {
        // Unclosed template id
        expectThrows(XContentParseException.class, () -> SearchTemplateRequest.fromXContent(newParser("{'id' : 'another_temp }")));
    }

    /**
     * Creates a {@link XContentParser} with the given String while replacing single quote to double quotes.
     */
    private XContentParser newParser(String s) throws IOException {
        assertNotNull(s);
        return createParser(JsonXContent.jsonXContent, s.replace("'", "\""));
    }
}
