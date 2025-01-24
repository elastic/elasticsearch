/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchTemplateRequestTests extends ESTestCase {

    public void testParseRequest() throws Exception {
        byte[] data = StreamsUtils.copyToBytesFromClasspath("/org/elasticsearch/script/mustache/simple-msearch-template.json");
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(data), XContentType.JSON)
            .build();

        MultiSearchTemplateRequest request = RestMultiSearchTemplateAction.parseRequest(restRequest, true);

        assertThat(request.requests().size(), equalTo(3));
        assertThat(request.requests().get(0).getRequest().indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).getRequest().indices(), arrayContaining("test0", "test1"));
        assertThat(request.requests().get(0).getRequest().requestCache(), equalTo(true));
        assertThat(request.requests().get(0).getRequest().preference(), nullValue());
        assertThat(request.requests().get(1).getRequest().indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).getRequest().indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).getRequest().requestCache(), nullValue());
        assertThat(request.requests().get(1).getRequest().preference(), equalTo("_local"));
        assertThat(request.requests().get(2).getRequest().indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).getRequest().routing(), equalTo("123"));
        assertNotNull(request.requests().get(0).getScript());
        assertNotNull(request.requests().get(1).getScript());
        assertNotNull(request.requests().get(2).getScript());

        assertEquals(ScriptType.INLINE, request.requests().get(0).getScriptType());
        assertEquals(ScriptType.INLINE, request.requests().get(1).getScriptType());
        assertEquals(ScriptType.INLINE, request.requests().get(2).getScriptType());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(0).getScript());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(1).getScript());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(2).getScript());
        assertEquals(1, request.requests().get(0).getScriptParams().size());
        assertEquals(1, request.requests().get(1).getScriptParams().size());
        assertEquals(1, request.requests().get(2).getScriptParams().size());
    }

    public void testParseWithCarriageReturn() throws Exception {
        final String content = """
            {"index":["test0", "test1"], "request_cache": true}
            {"source": {"query" : {"match_{{template}}" :{}}}, "params": {"template": "all" } }
            """;
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(content), XContentType.JSON)
            .build();

        MultiSearchTemplateRequest request = RestMultiSearchTemplateAction.parseRequest(restRequest, true);

        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).getRequest().indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).getRequest().indices(), arrayContaining("test0", "test1"));
        assertThat(request.requests().get(0).getRequest().requestCache(), equalTo(true));
        assertThat(request.requests().get(0).getRequest().preference(), nullValue());
        assertNotNull(request.requests().get(0).getScript());
        assertEquals(ScriptType.INLINE, request.requests().get(0).getScriptType());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(0).getScript());
        assertEquals(1, request.requests().get(0).getScriptParams().size());
    }

    public void testMaxConcurrentSearchRequests() {
        MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
        request.maxConcurrentSearchRequests(randomIntBetween(1, Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> request.maxConcurrentSearchRequests(randomIntBetween(Integer.MIN_VALUE, 0)));
    }

    public void testMultiSearchTemplateToJson() throws Exception {
        final int numSearchRequests = randomIntBetween(1, 10);
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();
        for (int i = 0; i < numSearchRequests; i++) {
            // Create a random request.
            String[] indices = { "test" };
            SearchRequest searchRequest = new SearchRequest(indices);
            // scroll is not supported in the current msearch or msearchtemplate api, so unset it:
            searchRequest.scroll(null);
            // batched reduce size is currently not set-able on a per-request basis as it is a query string parameter only
            searchRequest.setBatchedReduceSize(SearchRequest.DEFAULT_BATCHED_REDUCE_SIZE);
            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(searchRequest);

            searchTemplateRequest.setScript("""
                {"query": { "match" : { "{{field}}" : "{{value}}" }}}""");
            searchTemplateRequest.setScriptType(ScriptType.INLINE);
            searchTemplateRequest.setProfile(randomBoolean());

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "name");
            scriptParams.put("value", randomAlphaOfLengthBetween(2, 5));
            searchTemplateRequest.setScriptParams(scriptParams);

            multiSearchTemplateRequest.add(searchTemplateRequest);
        }

        // Serialize the request
        String serialized = toJsonString(multiSearchTemplateRequest);

        // Deserialize the request
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(serialized), XContentType.JSON)
            .build();
        MultiSearchTemplateRequest deser = RestMultiSearchTemplateAction.parseRequest(restRequest, true);

        // For object equality purposes need to set the search requests' source to non-null
        for (SearchTemplateRequest str : deser.requests()) {
            SearchRequest sr = str.getRequest();
            if (sr.source() == null) {
                sr.source(new SearchSourceBuilder());
            }
        }
        // Compare the deserialized request object with the original request object
        assertEquals(multiSearchTemplateRequest, deser);

        // Finally, serialize the deserialized request to compare JSON equivalence (in case Object.equals() fails to reveal a discrepancy)
        assertEquals(serialized, toJsonString(deser));
    }

    protected String toJsonString(MultiSearchTemplateRequest multiSearchTemplateRequest) throws IOException {
        byte[] bytes = MultiSearchTemplateRequest.writeMultiLineFormat(multiSearchTemplateRequest, XContentType.JSON.xContent());
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
