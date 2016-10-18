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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.Script.StoredScriptSource;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Full integration test of the template query plugin.
 */
public class SearchTemplateIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(MustachePlugin.class);
    }

    @Before
    public void setup() throws IOException {
        createIndex("test");
        client().prepareIndex("test", "testtype", "1")
                .setSource(jsonBuilder().startObject().field("text", "value1").endObject())
                .get();
        client().prepareIndex("test", "testtype", "2")
                .setSource(jsonBuilder().startObject().field("text", "value2").endObject())
                .get();
        client().admin().indices().prepareRefresh().get();
    }

    // Relates to #6318
    public void testSearchRequestFail() throws Exception {
        String query = "{ \"query\": {\"match_all\": {}}, \"size\" : \"{{my_size}}\"  }";

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");

        expectThrows(Exception.class, () -> new SearchTemplateRequestBuilder(client())
                .setRequest(searchRequest)
                .setScript(query)
                .setScriptType(Script.ScriptType.INLINE)
                .setScriptParams(Collections.emptyMap())
                .get());

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(searchRequest)
                .setScript(query)
                .setScriptType(Script.ScriptType.INLINE)
                .setScriptParams(Collections.singletonMap("my_size", 1))
                .get();

        assertThat(searchResponse.getResponse().getHits().hits().length, equalTo(1));
    }

    /**
     * Test that template can be expressed as a single escaped string.
     */
    public void testTemplateQueryAsEscapedString() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String query = "{" + "  \"inline\" : \"{ \\\"size\\\": \\\"{{size}}\\\", \\\"query\\\":{\\\"match_all\\\":{}}}\","
                + "  \"params\":{" + "    \"size\": 1" + "  }" + "}";
        SearchTemplateRequest request = RestSearchTemplateAction.parse(new BytesArray(query));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().hits().length, equalTo(1));
    }

    /**
     * Test that template can contain conditional clause. In this case it is at
     * the beginning of the string.
     */
    public void testTemplateQueryAsEscapedStringStartingWithConditionalClause() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String templateString = "{"
                + "  \"inline\" : \"{ {{#use_size}} \\\"size\\\": \\\"{{size}}\\\", {{/use_size}} \\\"query\\\":{\\\"match_all\\\":{}}}\","
                + "  \"params\":{" + "    \"size\": 1," + "    \"use_size\": true" + "  }" + "}";
        SearchTemplateRequest request = RestSearchTemplateAction.parse(new BytesArray(templateString));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().hits().length, equalTo(1));
    }

    /**
     * Test that template can contain conditional clause. In this case it is at
     * the end of the string.
     */
    public void testTemplateQueryAsEscapedStringWithConditionalClauseAtEnd() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String templateString = "{"
                + "  \"inline\" : \"{ \\\"query\\\":{\\\"match_all\\\":{}} {{#use_size}}, \\\"size\\\": \\\"{{size}}\\\" {{/use_size}} }\","
                + "  \"params\":{" + "    \"size\": 1," + "    \"use_size\": true" + "  }" + "}";
        SearchTemplateRequest request = RestSearchTemplateAction.parse(new BytesArray(templateString));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().hits().length, equalTo(1));
    }

    public void testIndexedTemplateClient() throws Exception {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("testTemplate")
                .setSource(new StoredScriptSource(true, MustacheScriptEngineService.NAME,
                        "                \"query\":{" +
                        "                   \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}", Collections.emptyMap())));


        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("testTemplate").setSource(new StoredScriptSource(true, MustacheScriptEngineService.NAME,
                "{ \"query\":{ \"match\":{ \"theField\" : \"{{fieldParam}}\"}}}", Collections.emptyMap())));

        GetStoredScriptResponse getResponse = client().admin().cluster()
                .prepareGetStoredScript("testTemplate").get();
        assertNotNull(getResponse.getSource());

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}"));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest("test").types("type"))
                .setScript("testTemplate").setScriptType(Script.ScriptType.STORED).setScriptParams(templateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 4);

        assertAcked(client().admin().cluster().prepareDeleteStoredScript("testTemplate"));

        getResponse = client().admin().cluster()
                .prepareGetStoredScript("testTemplate").get();
        assertNull(getResponse.getSource());
    }

    public void testIndexedTemplate() throws Exception {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("1a")
                .setSource(new StoredScriptSource(true, MustacheScriptEngineService.NAME, "{" +
                        "                \"query\":{" +
                        "                   \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}", Collections.emptyMap()
                ))
        );
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("2")
                .setSource(new StoredScriptSource(true, MustacheScriptEngineService.NAME, "{" +
                        "                \"query\":{" +
                        "                   \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}", Collections.emptyMap()
                ))
        );
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("3")
                .setSource(new StoredScriptSource(true, MustacheScriptEngineService.NAME, "{" +
                        "             \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }", Collections.emptyMap()
                ))
        );

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}"));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest().indices("test").types("type"))
                .setScript("1a")
                .setScriptType(Script.ScriptType.STORED)
                .setScriptParams(templateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 4);

        expectThrows(ResourceNotFoundException.class, () -> new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest().indices("test").types("type"))
                .setScript("/template_index/mustache/1000")
                .setScriptType(Script.ScriptType.STORED)
                .setScriptParams(templateParams)
                .get());

        expectThrows(ResourceNotFoundException.class, () -> new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest().indices("test").types("type"))
                .setScript("/myindex/1")
                .setScriptType(Script.ScriptType.STORED)
                .setScriptParams(templateParams)
                .get());

        templateParams.put("fieldParam", "bar");
        searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest("test").types("type"))
                .setScript("2").setScriptType(Script.ScriptType.STORED).setScriptParams(templateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 1);

        Map<String, Object> vars = new HashMap<>();
        vars.put("fieldParam", "bar");

        TemplateQueryBuilder builder = new TemplateQueryBuilder("3", Script.ScriptType.STORED, vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 1);
    }

    // Relates to #10397
    public void testIndexedTemplateOverwrite() throws Exception {
        createIndex("testindex");
        ensureGreen("testindex");

        client().prepareIndex("testindex", "test", "1")
                .setSource(jsonBuilder().startObject().field("searchtext", "dev1").endObject())
                .get();
        client().admin().indices().prepareRefresh().get();

        int iterations = randomIntBetween(2, 11);
        for (int i = 1; i < iterations; i++) {
            assertAcked(client().admin().cluster().preparePutStoredScript()
                    .setId("git01")
                    .setSource(new StoredScriptSource(true, MustacheScriptEngineService.NAME,
                        "{\"query\": {\"match\": {\"searchtext\": {\"query\": \"{{P_Keyword1}}\"," +
                            " \"type\": \"ooophrase_prefix\"}}}}", Collections.emptyMap())));

            GetStoredScriptResponse getResponse = client().admin().cluster()
                    .prepareGetStoredScript("git01").get();
            assertNotNull(getResponse.getSource());

            Map<String, Object> templateParams = new HashMap<>();
            templateParams.put("P_Keyword1", "dev");

            ParsingException e = expectThrows(ParsingException.class, () -> new SearchTemplateRequestBuilder(client())
                    .setRequest(new SearchRequest("testindex").types("test"))
                    .setScript("git01").setScriptType(Script.ScriptType.STORED).setScriptParams(templateParams)
                    .get());
            assertThat(e.getMessage(), containsString("[match] query does not support type ooophrase_prefix"));

            assertAcked(client().admin().cluster().preparePutStoredScript()
                    .setId("git01")
                    .setSource(new StoredScriptSource(true, MustacheScriptEngineService.NAME,
                        "{\"query\": {\"match\": {\"searchtext\": {\"query\": \"{{P_Keyword1}}\"," +
                            "\"type\": \"phrase_prefix\"}}}}", Collections.emptyMap())));

            SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                    .setRequest(new SearchRequest("testindex").types("test"))
                    .setScript("git01").setScriptType(Script.ScriptType.STORED).setScriptParams(templateParams)
                    .get();
            assertHitCount(searchResponse.getResponse(), 1);
        }
    }

    public void testIndexedTemplateWithArray() throws Exception {
        String multiQuery = "{\"query\":{\"terms\":{\"theField\":[\"{{#fieldParam}}\",\"{{.}}\",\"{{/fieldParam}}\"]}}}";
        assertAcked(
                client().admin().cluster().preparePutStoredScript()
                        .setId("4")
                        .setSource(new StoredScriptSource(true, "mustache", multiQuery, Collections.emptyMap()))
        );
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}"));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}"));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> arrayTemplateParams = new HashMap<>();
        String[] fieldParams = {"foo", "bar"};
        arrayTemplateParams.put("fieldParam", fieldParams);

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest("test").types("type"))
                .setScript("4").setScriptType(Script.ScriptType.STORED).setScriptParams(arrayTemplateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 5);
    }

}
