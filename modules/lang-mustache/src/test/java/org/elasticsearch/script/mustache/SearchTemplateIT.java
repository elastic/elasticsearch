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
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptType;
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
        client().prepareIndex("test", "type", "1")
                .setSource(jsonBuilder().startObject().field("text", "value1").endObject())
                .get();
        client().prepareIndex("test", "type", "2")
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
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(randomBoolean() ? null : Collections.emptyMap())
                .get());

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(searchRequest)
                .setScript(query)
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(Collections.singletonMap("my_size", 1))
                .get();

        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    /**
     * Test that template can be expressed as a single escaped string.
     */
    public void testTemplateQueryAsEscapedString() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String query =
                  "{" + "  \"source\" : \"{ \\\"size\\\": \\\"{{size}}\\\", \\\"query\\\":{\\\"match_all\\\":{}}}\","
                + "  \"params\":{"
                + "    \"size\": 1"
                + "  }"
                + "}";
        SearchTemplateRequest request = RestSearchTemplateAction.parse(createParser(JsonXContent.jsonXContent, query));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    /**
     * Test that template can contain conditional clause. In this case it is at
     * the beginning of the string.
     */
    public void testTemplateQueryAsEscapedStringStartingWithConditionalClause() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String templateString =
                  "{"
                + "  \"source\" : \"{ {{#use_size}} \\\"size\\\": \\\"{{size}}\\\", {{/use_size}} \\\"query\\\":{\\\"match_all\\\":{}}}\","
                + "  \"params\":{"
                + "    \"size\": 1,"
                + "    \"use_size\": true"
                + "  }"
                + "}";
        SearchTemplateRequest request = RestSearchTemplateAction.parse(createParser(JsonXContent.jsonXContent, templateString));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    /**
     * Test that template can contain conditional clause. In this case it is at
     * the end of the string.
     */
    public void testTemplateQueryAsEscapedStringWithConditionalClauseAtEnd() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String templateString =
                  "{"
                + "  \"source\" : \"{ \\\"query\\\":{\\\"match_all\\\":{}} {{#use_size}}, \\\"size\\\": \\\"{{size}}\\\" {{/use_size}} }\","
                + "  \"params\":{"
                + "    \"size\": 1,"
                + "    \"use_size\": true"
                + "  }"
                + "}";
        SearchTemplateRequest request = RestSearchTemplateAction.parse(createParser(JsonXContent.jsonXContent, templateString));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    public void testIndexedTemplateClient() throws Exception {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("testTemplate")
                .setContent(new BytesArray("{" +
                        "\"template\":{" +
                        "                \"query\":{" +
                        "                   \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}" +
                        "}"), XContentType.JSON));


        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("testTemplate").setContent(new BytesArray("{" +
                        "\"template\":{" +
                        "                \"query\":{" +
                        "                   \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}" +
                        "}"), XContentType.JSON));

        GetStoredScriptResponse getResponse = client().admin().cluster()
                .prepareGetStoredScript("testTemplate").get();
        assertNotNull(getResponse.getSource());

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest("test").types("type"))
                .setScript("testTemplate").setScriptType(ScriptType.STORED).setScriptParams(templateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 4);

        assertAcked(client().admin().cluster().prepareDeleteStoredScript("testTemplate"));

        getResponse = client().admin().cluster().prepareGetStoredScript("testTemplate").get();
        assertNull(getResponse.getSource());
    }

    public void testIndexedTemplate() throws Exception {
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("1a")
                .setContent(new BytesArray("{" +
                        "\"template\":{" +
                        "                \"query\":{" +
                        "                   \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}" +
                        "}"
                ), XContentType.JSON)
        );
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("2")
                .setContent(new BytesArray("{" +
                        "\"template\":{" +
                        "                \"query\":{" +
                        "                   \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}" +
                        "}"), XContentType.JSON)
        );
        assertAcked(client().admin().cluster().preparePutStoredScript()
                .setId("3")
                .setContent(new BytesArray("{" +
                        "\"template\":{" +
                        "             \"match\":{" +
                        "                    \"theField\" : \"{{fieldParam}}\"}" +
                        "       }" +
                        "}"), XContentType.JSON)
        );

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest().indices("test").types("type"))
                .setScript("1a")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 4);

        expectThrows(ResourceNotFoundException.class, () -> new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest().indices("test").types("type"))
                .setScript("1000")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams)
                .get());

        templateParams.put("fieldParam", "bar");
        searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest("test").types("type"))
                .setScript("2").setScriptType(ScriptType.STORED).setScriptParams(templateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 1);
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
                    .setContent(new BytesArray(
                            "{\"template\":{\"query\": {\"match_phrase_prefix\": {\"searchtext\": {\"query\": \"{{P_Keyword1}}\","
                                    + "\"slop\": -1}}}}}"),
                            XContentType.JSON));

            GetStoredScriptResponse getResponse = client().admin().cluster().prepareGetStoredScript("git01").get();
            assertNotNull(getResponse.getSource());

            Map<String, Object> templateParams = new HashMap<>();
            templateParams.put("P_Keyword1", "dev");

            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new SearchTemplateRequestBuilder(client())
                    .setRequest(new SearchRequest("testindex").types("test"))
                    .setScript("git01").setScriptType(ScriptType.STORED).setScriptParams(templateParams)
                    .get());
            assertThat(e.getMessage(), containsString("No negative slop allowed"));

            assertAcked(client().admin().cluster().preparePutStoredScript()
                    .setId("git01")
                    .setContent(new BytesArray("{\"query\": {\"match_phrase_prefix\": {\"searchtext\": {\"query\": \"{{P_Keyword1}}\"," +
                            "\"slop\": 0}}}}"), XContentType.JSON));

            SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                    .setRequest(new SearchRequest("testindex").types("test"))
                    .setScript("git01").setScriptType(ScriptType.STORED).setScriptParams(templateParams)
                    .get();
            assertHitCount(searchResponse.getResponse(), 1);
        }
    }

    public void testIndexedTemplateWithArray() throws Exception {
        String multiQuery = "{\"query\":{\"terms\":{\"theField\":[\"{{#fieldParam}}\",\"{{.}}\",\"{{/fieldParam}}\"]}}}";
        assertAcked(
                client().admin().cluster().preparePutStoredScript()
                        .setId("4")
                        .setContent(jsonBuilder().startObject().field("template", multiQuery).endObject().bytes(), XContentType.JSON)
        );
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> arrayTemplateParams = new HashMap<>();
        String[] fieldParams = {"foo", "bar"};
        arrayTemplateParams.put("fieldParam", fieldParams);

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client())
                .setRequest(new SearchRequest("test").types("type"))
                .setScript("4").setScriptType(ScriptType.STORED).setScriptParams(arrayTemplateParams)
                .get();
        assertHitCount(searchResponse.getResponse(), 5);
    }

}
