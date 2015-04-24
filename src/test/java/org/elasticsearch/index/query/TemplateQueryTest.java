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
package org.elasticsearch.index.query;

import com.google.common.collect.Maps;

import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.indexedscripts.delete.DeleteIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.get.GetIndexedScriptResponse;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptRequestBuilder;
import org.elasticsearch.action.indexedscripts.put.PutIndexedScriptResponse;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.*;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

/**
 * Full integration test of the template query plugin.
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.SUITE)
public class TemplateQueryTest extends ElasticsearchIntegrationTest {

    @Before
    public void setup() throws IOException {
        createIndex("test");
        ensureGreen("test");

        index("test", "testtype", "1", jsonBuilder().startObject().field("text", "value1").endObject());
        index("test", "testtype", "2", jsonBuilder().startObject().field("text", "value2").endObject());
        refresh();
    }

    @Override
    public Settings nodeSettings(int nodeOrdinal) {
        return settingsBuilder().put(super.nodeSettings(nodeOrdinal))
                .put("path.conf", this.getDataPath("config")).build();
    }

    @Test
    public void testTemplateInBody() throws IOException {
        Map<String, Object> vars = new HashMap<>();
        vars.put("template", "all");

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "{\"match_{{template}}\": {}}\"", vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 2);
    }

    @Test
    public void testTemplateInBodyWithSize() throws IOException {
        String request = "{\n" +
                "    \"size\":0," +
                "    \"query\": {\n" +
                "        \"template\": {\n" +
                "            \"query\": {\"match_{{template}}\": {}},\n" +
                "            \"params\" : {\n" +
                "                \"template\" : \"all\"\n" +
                "            }\n" +
                "        }\n" +
                "    }\n" +
                "}";
        SearchResponse sr = client().prepareSearch().setSource(request)
                .execute().actionGet();
        assertNoFailures(sr);
        assertThat(sr.getHits().hits().length, equalTo(0));
        request = "{\n" +
                "    \"query\": {\n" +
                "        \"template\": {\n" +
                "            \"query\": {\"match_{{template}}\": {}},\n" +
                "            \"params\" : {\n" +
                "                \"template\" : \"all\"\n" +
                "            }\n" +
                "        }\n" +
                "    },\n" +
                "    \"size\":0" +
                "}";

        sr = client().prepareSearch().setSource(request)
                .execute().actionGet();
        assertNoFailures(sr);
        assertThat(sr.getHits().hits().length, equalTo(0));
    }

    @Test
    public void testTemplateWOReplacementInBody() throws IOException {
        Map<String, Object> vars = new HashMap<>();

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "{\"match_all\": {}}\"", vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 2);
    }

    @Test
    public void testTemplateInFile() {
        Map<String, Object> vars = new HashMap<>();
        vars.put("template", "all");

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "storedTemplate", ScriptService.ScriptType.FILE, vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 2);
    }

    @Test
    public void testRawEscapedTemplate() throws IOException {
        String query = "{\"template\": {\"query\": \"{\\\"match_{{template}}\\\": {}}\\\"\",\"params\" : {\"template\" : \"all\"}}}";

        SearchResponse sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 2);
    }

    @Test
    public void testRawTemplate() throws IOException {
        String query = "{\"template\": {\"query\": {\"match_{{template}}\": {}},\"params\" : {\"template\" : \"all\"}}}";
        SearchResponse sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 2);
    }

    @Test
    public void testRawFSTemplate() throws IOException {
        String query = "{\"template\": {\"file\": \"storedTemplate\",\"params\" : {\"template\" : \"all\"}}}";

        SearchResponse sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 2);
    }

    @Test
    public void testSearchRequestTemplateSource() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");

        String query = "{ \"template\" : { \"query\": {\"match_{{template}}\": {} } }, \"params\" : { \"template\":\"all\" } }";
        BytesReference bytesRef = new BytesArray(query);
        searchRequest.templateSource(bytesRef);

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertHitCount(searchResponse, 2);
    }

    @Test
    // Releates to #6318
    public void testSearchRequestFail() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        try {
            String query = "{ \"template\" : { \"query\": {\"match_all\": {}}, \"size\" : \"{{my_size}}\"  } }";
            BytesReference bytesRef = new BytesArray(query);
            searchRequest.templateSource(bytesRef);
            client().search(searchRequest).get();
            fail("expected exception");
        } catch (Exception ex) {
            // expected - no params
        }
        String query = "{ \"template\" : { \"query\": {\"match_all\": {}}, \"size\" : \"{{my_size}}\"  }, \"params\" : { \"my_size\": 1 } }";
        BytesReference bytesRef = new BytesArray(query);
        searchRequest.templateSource(bytesRef);

        SearchResponse searchResponse = client().search(searchRequest).get();
        assertThat(searchResponse.getHits().hits().length, equalTo(1));
    }

    @Test
    public void testThatParametersCanBeSet() throws Exception {
        index("test", "type", "1", jsonBuilder().startObject().field("theField", "foo").endObject());
        index("test", "type", "2", jsonBuilder().startObject().field("theField", "foo 2").endObject());
        index("test", "type", "3", jsonBuilder().startObject().field("theField", "foo 3").endObject());
        index("test", "type", "4", jsonBuilder().startObject().field("theField", "foo 4").endObject());
        index("test", "type", "5", jsonBuilder().startObject().field("otherField", "foo").endObject());
        refresh();

        Map<String, Object> templateParams = Maps.newHashMap();
        templateParams.put("mySize", "2");
        templateParams.put("myField", "theField");
        templateParams.put("myValue", "foo");

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type").setTemplateName("full-query-template")
                .setTemplateParams(templateParams).setTemplateType(ScriptService.ScriptType.FILE).get();
        assertHitCount(searchResponse, 4);
        // size kicks in here...
        assertThat(searchResponse.getHits().getHits().length, is(2));

        templateParams.put("myField", "otherField");
        searchResponse = client().prepareSearch("test").setTypes("type").setTemplateName("full-query-template")
                .setTemplateParams(templateParams).setTemplateType(ScriptService.ScriptType.FILE).get();
        assertHitCount(searchResponse, 1);
    }

    @Test(expected = SearchPhaseExecutionException.class)
    public void testIndexedTemplateClient() throws Exception {
        createIndex(ScriptService.SCRIPT_INDEX);
        ensureGreen(ScriptService.SCRIPT_INDEX);

        PutIndexedScriptResponse scriptResponse = client().preparePutIndexedScript(MustacheScriptEngineService.NAME, "testTemplate", "{" +
                "\"template\":{" +
                "                \"query\":{" +
                "                   \"match\":{" +
                "                    \"theField\" : \"{{fieldParam}}\"}" +
                "       }" +
                "}" +
                "}").get();

        assertTrue(scriptResponse.isCreated());

        scriptResponse = client().preparePutIndexedScript(MustacheScriptEngineService.NAME, "testTemplate", "{" +
                "\"template\":{" +
                "                \"query\":{" +
                "                   \"match\":{" +
                "                    \"theField\" : \"{{fieldParam}}\"}" +
                "       }" +
                "}" +
                "}").get();

        assertEquals(scriptResponse.getVersion(), 2);

        GetIndexedScriptResponse getResponse = client().prepareGetIndexedScript(MustacheScriptEngineService.NAME, "testTemplate").get();
        assertTrue(getResponse.isExists());

        List<IndexRequestBuilder> builders = new ArrayList<>();

        builders.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}"));
        builders.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}"));
        builders.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}"));
        builders.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}"));
        builders.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}"));

        indexRandom(true,builders);

        Map<String, Object> templateParams = Maps.newHashMap();
        templateParams.put("fieldParam", "foo");

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type").
                setTemplateName("testTemplate").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
        assertHitCount(searchResponse, 4);

        DeleteIndexedScriptResponse deleteResponse = client().prepareDeleteIndexedScript(MustacheScriptEngineService.NAME,"testTemplate").get();
        assertTrue(deleteResponse.isFound());

        getResponse = client().prepareGetIndexedScript(MustacheScriptEngineService.NAME, "testTemplate").get();
        assertFalse(getResponse.isExists());

        client().prepareSearch("test").setTypes("type").
                setTemplateName("/template_index/mustache/1000").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
    }

    @Test
    public void testIndexedTemplate() throws Exception {
        createIndex(ScriptService.SCRIPT_INDEX);
        ensureGreen(ScriptService.SCRIPT_INDEX);
        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex(ScriptService.SCRIPT_INDEX, MustacheScriptEngineService.NAME, "1a").setSource("{" +
                "\"template\":{"+
                "                \"query\":{" +
                "                   \"match\":{" +
                "                    \"theField\" : \"{{fieldParam}}\"}" +
                "       }" +
                    "}" +
                "}"));
        builders.add(client().prepareIndex(ScriptService.SCRIPT_INDEX, MustacheScriptEngineService.NAME, "2").setSource("{" +
                "\"template\":{"+
                "                \"query\":{" +
                "                   \"match\":{" +
                "                    \"theField\" : \"{{fieldParam}}\"}" +
                "       }" +
                    "}" +
                "}"));

        builders.add(client().prepareIndex(ScriptService.SCRIPT_INDEX, MustacheScriptEngineService.NAME, "3").setSource("{" +
                "\"template\":{"+
                "             \"match\":{" +
                "                    \"theField\" : \"{{fieldParam}}\"}" +
                "       }" +
                "}"));

        indexRandom(true, builders);

        builders.clear();

        builders.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}"));
        builders.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}"));
        builders.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}"));
        builders.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}"));
        builders.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}"));

        indexRandom(true,builders);

        Map<String, Object> templateParams = Maps.newHashMap();
        templateParams.put("fieldParam", "foo");

        SearchResponse searchResponse = client().prepareSearch("test").setTypes("type").
                setTemplateName("/mustache/1a").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
        assertHitCount(searchResponse, 4);

        try {
            client().prepareSearch("test").setTypes("type").
                    setTemplateName("/template_index/mustache/1000").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
            fail("shouldn't get here");
        } catch (SearchPhaseExecutionException spee) {
            //all good
        }

        try {
            searchResponse = client().prepareSearch("test").setTypes("type").
                    setTemplateName("/myindex/mustache/1").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
            assertFailures(searchResponse);
        } catch (SearchPhaseExecutionException spee) {
            //all good
        }

        searchResponse = client().prepareSearch("test").setTypes("type").
                setTemplateName("1a").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
        assertHitCount(searchResponse, 4);

        templateParams.put("fieldParam", "bar");
        searchResponse = client().prepareSearch("test").setTypes("type").
                setTemplateName("/mustache/2").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
        assertHitCount(searchResponse, 1);

        Map<String, Object> vars = new HashMap<>();
        vars.put("fieldParam", "bar");

        TemplateQueryBuilder builder = new TemplateQueryBuilder(
                "3", ScriptService.ScriptType.INDEXED, vars);
        SearchResponse sr = client().prepareSearch().setQuery(builder)
                .execute().actionGet();
        assertHitCount(sr, 1);

        String query = "{\"template\": {\"id\": \"3\",\"params\" : {\"fieldParam\" : \"foo\"}}}";
        sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 4);

        query = "{\"template\": {\"id\": \"/mustache/3\",\"params\" : {\"fieldParam\" : \"foo\"}}}";
        sr = client().prepareSearch().setQuery(query).get();
        assertHitCount(sr, 4);
    }

    // Relates to #10397
    @Test
    public void testIndexedTemplateOverwrite() throws Exception {
        createIndex("testindex");
        ensureGreen("testindex");

        index("testindex", "test", "1", jsonBuilder().startObject().field("searchtext", "dev1").endObject());
        refresh();

        int iterations = randomIntBetween(2, 11);
        for (int i = 1; i < iterations; i++) {
            PutIndexedScriptResponse scriptResponse = client().preparePutIndexedScript(MustacheScriptEngineService.NAME, "git01", 
                    "{\"query\": {\"match\": {\"searchtext\": {\"query\": \"{{P_Keyword1}}\",\"type\": \"ooophrase_prefix\"}}}}").get();
            assertEquals(i * 2 - 1, scriptResponse.getVersion());
    
            GetIndexedScriptResponse getResponse = client().prepareGetIndexedScript(MustacheScriptEngineService.NAME, "git01").get();
            assertTrue(getResponse.isExists());
    
            Map<String, Object> templateParams = Maps.newHashMap();
            templateParams.put("P_Keyword1", "dev");
    
            try {
                client().prepareSearch("testindex").setTypes("test").
                    setTemplateName("git01").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
                fail("Broken test template is parsing w/o error.");
            } catch (SearchPhaseExecutionException e) {
                // the above is expected to fail
            }
    
            PutIndexedScriptRequestBuilder builder = client()
                    .preparePutIndexedScript(MustacheScriptEngineService.NAME, "git01",
                            "{\"query\": {\"match\": {\"searchtext\": {\"query\": \"{{P_Keyword1}}\",\"type\": \"phrase_prefix\"}}}}")
                    .setOpType(OpType.INDEX);
            scriptResponse = builder.get();
            assertEquals(i * 2, scriptResponse.getVersion());
            SearchResponse searchResponse = client().prepareSearch("testindex").setTypes("test").
                    setTemplateName("git01").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(templateParams).get();
            assertHitCount(searchResponse, 1);
        }
    }

    
    @Test
    public void testIndexedTemplateWithArray() throws Exception {
      createIndex(ScriptService.SCRIPT_INDEX);
      ensureGreen(ScriptService.SCRIPT_INDEX);
      List<IndexRequestBuilder> builders = new ArrayList<>();

      String multiQuery = "{\"query\":{\"terms\":{\"theField\":[\"{{#fieldParam}}\",\"{{.}}\",\"{{/fieldParam}}\"]}}}";

      builders.add(client().prepareIndex(ScriptService.SCRIPT_INDEX, MustacheScriptEngineService.NAME, "4").setSource(jsonBuilder().startObject().field("template", multiQuery).endObject()));

      indexRandom(true,builders);

      builders.clear();

      builders.add(client().prepareIndex("test", "type", "1").setSource("{\"theField\":\"foo\"}"));
      builders.add(client().prepareIndex("test", "type", "2").setSource("{\"theField\":\"foo 2\"}"));
      builders.add(client().prepareIndex("test", "type", "3").setSource("{\"theField\":\"foo 3\"}"));
      builders.add(client().prepareIndex("test", "type", "4").setSource("{\"theField\":\"foo 4\"}"));
      builders.add(client().prepareIndex("test", "type", "5").setSource("{\"theField\":\"bar\"}"));

      indexRandom(true,builders);

      Map<String, Object> arrayTemplateParams = new HashMap<>();
      String[] fieldParams = {"foo","bar"};
      arrayTemplateParams.put("fieldParam", fieldParams);

      SearchResponse searchResponse = client().prepareSearch("test").setTypes("type").
              setTemplateName("/mustache/4").setTemplateType(ScriptService.ScriptType.INDEXED).setTemplateParams(arrayTemplateParams).get();
      assertHitCount(searchResponse, 5);
    }

}
