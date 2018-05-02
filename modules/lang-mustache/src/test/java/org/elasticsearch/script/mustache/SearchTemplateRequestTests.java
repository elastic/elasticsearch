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
import org.elasticsearch.search.RandomSearchRequestGenerator;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.nullValue;

public class SearchTemplateRequestTests extends AbstractStreamableTestCase<SearchTemplateRequest> {

    @Override
    protected SearchTemplateRequest createBlankInstance() {
        return new SearchTemplateRequest();
    }

    @Override
    protected SearchTemplateRequest createTestInstance() {
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setScriptType(randomFrom(ScriptType.values()));
        request.setScript(randomAlphaOfLength(50));

        Map<String, Object> scriptParams = new HashMap<>();
        for (int i = 0; i < randomInt(10); i++) {
            scriptParams.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        }
        request.setScriptParams(scriptParams);

        request.setExplain(randomBoolean());
        request.setProfile(randomBoolean());
        request.setSimulate(randomBoolean());

        request.setRequest(RandomSearchRequestGenerator.randomSearchRequest(
            SearchSourceBuilder::searchSource));
        return request;
    }

    @Override
    protected SearchTemplateRequest mutateInstance(SearchTemplateRequest instance) throws IOException {
        List<Consumer<SearchTemplateRequest>> mutators = new ArrayList<>();

        mutators.add(request -> request.setScriptType(
            randomValueOtherThan(request.getScriptType(), () -> randomFrom(ScriptType.values()))));
        mutators.add(request -> request.setScript(
            randomValueOtherThan(request.getScript(), () -> randomAlphaOfLength(50))));

        mutators.add(request -> {
            Map<String, Object> mutatedScriptParams = new HashMap<>(request.getScriptParams());
            String newField = randomValueOtherThanMany(mutatedScriptParams::containsKey, () -> randomAlphaOfLength(5));
            mutatedScriptParams.put(newField, randomAlphaOfLength(10));
            request.setScriptParams(mutatedScriptParams);
        });

        mutators.add(request -> request.setProfile(!request.isProfile()));
        mutators.add(request -> request.setExplain(!request.isExplain()));
        mutators.add(request -> request.setSimulate(!request.isSimulate()));

        mutators.add(request -> request.setRequest(
            RandomSearchRequestGenerator.randomSearchRequest(SearchSourceBuilder::searchSource)));

        SearchTemplateRequest mutatedInstance = copyInstance(instance);
        Consumer<SearchTemplateRequest> mutator = randomFrom(mutators);
        mutator.accept(mutatedInstance);
        return mutatedInstance;
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

    /**
     * Note that for xContent parsing, we omit two parts of the request:
     * - The 'simulate' option is always held constant, since this parameter is not included
     *   in the request's xContent (it's instead used to determine the request endpoint).
     * - We omit the random SearchRequest, since this component only affects the request
     *   parameters and also isn't captured in the request's xContent.
     */
    public void testFromXContent() throws IOException {
        AbstractXContentTestCase.testFromXContent(NUMBER_OF_TEST_RUNS,
            this::createTestInstanceForXContent,
            false,
            new String[]{}, field -> false,
            this::createParser,
            SearchTemplateRequest::fromXContent,
            this::assertEqualInstances, true);
    }

    private SearchTemplateRequest createTestInstanceForXContent() {
        SearchTemplateRequest request = createTestInstance();
        request.setSimulate(false);
        request.setRequest(null);
        return request;
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
