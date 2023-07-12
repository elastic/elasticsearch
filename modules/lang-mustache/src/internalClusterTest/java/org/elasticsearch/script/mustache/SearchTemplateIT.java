/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.script.mustache;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.DummyQueryParserPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Full integration test of the template query plugin.
 */
public class SearchTemplateIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(MustachePlugin.class, DummyQueryParserPlugin.class);
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(SearchService.CCS_VERSION_CHECK_SETTING.getKey(), "true").build();
    }

    @Before
    public void setup() throws IOException {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("text", "value1").endObject()).get();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("text", "value2").endObject()).get();
        indicesAdmin().prepareRefresh().get();
    }

    // Relates to #6318
    public void testSearchRequestFail() throws Exception {
        String query = """
            { "query": {"match_all": {}}, "size" : "{{my_size}}"  }""";

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");

        expectThrows(
            Exception.class,
            () -> new SearchTemplateRequestBuilder(client()).setRequest(searchRequest)
                .setScript(query)
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(randomBoolean() ? null : Collections.emptyMap())
                .get()
        );

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(searchRequest)
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
        String query = """
            {
              "source": "{ \\"size\\": \\"{{size}}\\", \\"query\\":{\\"match_all\\":{}}}",
              "params": {
                "size": 1
              }
            }""";
        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(createParser(JsonXContent.jsonXContent, query));
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
        String templateString = """
            {
              "source": "{ {{#use_size}} \\"size\\": \\"{{size}}\\", {{/use_size}} \\"query\\":{\\"match_all\\":{}}}",
              "params": {
                "size": 1,
                "use_size": true
              }
            }""";
        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(createParser(JsonXContent.jsonXContent, templateString));
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
        String templateString = """
            {
              "source": "{ \\"query\\":{\\"match_all\\":{}} {{#use_size}}, \\"size\\": \\"{{size}}\\" {{/use_size}} }",
              "params": {
                "size": 1,
                "use_size": true
              }
            }""";
        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(createParser(JsonXContent.jsonXContent, templateString));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    public void testIndexedTemplateClient() throws Exception {
        assertAcked(clusterAdmin().preparePutStoredScript().setId("testTemplate").setContent(new BytesArray("""
            {
              "script": {
                "lang": "mustache",
                "source": {
                  "query": {
                    "match": {
                      "theField": "{{fieldParam}}"
                    }
                  }
                }
              }
            }"""), XContentType.JSON));

        GetStoredScriptResponse getResponse = clusterAdmin().prepareGetStoredScript("testTemplate").get();
        assertNotNull(getResponse.getSource());

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        indicesAdmin().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
            .setScript("testTemplate")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(templateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 4);

        assertAcked(clusterAdmin().prepareDeleteStoredScript("testTemplate"));

        getResponse = clusterAdmin().prepareGetStoredScript("testTemplate").get();
        assertNull(getResponse.getSource());
    }

    public void testIndexedTemplate() throws Exception {

        String script = """
            {
              "script": {
                "lang": "mustache",
                "source": {
                  "query": {
                    "match": {
                      "theField": "{{fieldParam}}"
                    }
                  }
                }
              }
            }
            """;

        assertAcked(clusterAdmin().preparePutStoredScript().setId("1a").setContent(new BytesArray(script), XContentType.JSON));
        assertAcked(clusterAdmin().preparePutStoredScript().setId("2").setContent(new BytesArray(script), XContentType.JSON));
        assertAcked(clusterAdmin().preparePutStoredScript().setId("3").setContent(new BytesArray(script), XContentType.JSON));

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        indicesAdmin().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest().indices("test"))
            .setScript("1a")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(templateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 4);

        expectThrows(
            ResourceNotFoundException.class,
            () -> new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest().indices("test"))
                .setScript("1000")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams)
                .get()
        );

        templateParams.put("fieldParam", "bar");
        searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
            .setScript("2")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(templateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 1);
    }

    // Relates to #10397
    public void testIndexedTemplateOverwrite() throws Exception {
        createIndex("testindex");
        ensureGreen("testindex");

        client().prepareIndex("testindex").setId("1").setSource(jsonBuilder().startObject().field("searchtext", "dev1").endObject()).get();
        indicesAdmin().prepareRefresh().get();

        int iterations = randomIntBetween(2, 11);
        String query = """
            {
              "script": {
                "lang": "mustache",
                "source": {
                  "query": {
                    "match_phrase_prefix": {
                      "searchtext": {
                        "query": "{{P_Keyword1}}",
                        "slop": "{{slop}}"
                      }
                    }
                  }
                }
              }
            }""";
        for (int i = 1; i < iterations; i++) {
            assertAcked(
                clusterAdmin().preparePutStoredScript()
                    .setId("git01")
                    .setContent(new BytesArray(query.replace("{{slop}}", Integer.toString(-1))), XContentType.JSON)
            );

            GetStoredScriptResponse getResponse = clusterAdmin().prepareGetStoredScript("git01").get();
            assertNotNull(getResponse.getSource());

            Map<String, Object> templateParams = new HashMap<>();
            templateParams.put("P_Keyword1", "dev");

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("testindex"))
                    .setScript("git01")
                    .setScriptType(ScriptType.STORED)
                    .setScriptParams(templateParams)
                    .get()
            );
            assertThat(e.getMessage(), containsString("No negative slop allowed"));

            assertAcked(
                clusterAdmin().preparePutStoredScript()
                    .setId("git01")
                    .setContent(new BytesArray(query.replace("{{slop}}", Integer.toString(0))), XContentType.JSON)
            );

            SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("testindex"))
                .setScript("git01")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams)
                .get();
            assertHitCount(searchResponse.getResponse(), 1);
        }
    }

    public void testIndexedTemplateWithArray() throws Exception {
        String multiQuery = """
            {
              "script": {
                "lang": "mustache",
                "source": {
                  "query": {
                    "terms": {
                        "theField": [
                            "{{#fieldParam}}",
                            "{{.}}",
                            "{{/fieldParam}}"
                        ]
                    }
                  }
                }
              }
            }""";
        assertAcked(clusterAdmin().preparePutStoredScript().setId("4").setContent(new BytesArray(multiQuery), XContentType.JSON));
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        indicesAdmin().prepareRefresh().get();

        Map<String, Object> arrayTemplateParams = new HashMap<>();
        String[] fieldParams = { "foo", "bar" };
        arrayTemplateParams.put("fieldParam", fieldParams);

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
            .setScript("4")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(arrayTemplateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 5);
    }

    /**
     * Test that triggering the CCS compatibility check with a query that shouldn't go to the minor before Version.CURRENT works
     */
    public void testCCSCheckCompatibility() throws Exception {
        String templateString = """
            {
              "source": "{ \\"query\\":{\\"fail_before_current_version\\":{}} }"
            }""";
        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(createParser(JsonXContent.jsonXContent, templateString));
        request.setRequest(new SearchRequest());
        ExecutionException ex = expectThrows(
            ExecutionException.class,
            () -> client().execute(SearchTemplateAction.INSTANCE, request).get()
        );

        Throwable primary = ex.getCause();
        assertNotNull(primary);

        Throwable underlying = primary.getCause();
        assertNotNull(underlying);

        assertThat(
            primary.getMessage(),
            containsString("[class org.elasticsearch.action.search.SearchRequest] is not compatible with version")
        );
        assertThat(primary.getMessage(), containsString("'search.check_ccs_compatibility' setting is enabled."));

        String expectedCause = "[fail_before_current_version] was released first in version XXXXXXX, failed compatibility check trying to"
            + " send it to node with version XXXXXXX";
        String actualCause = underlying.getMessage().replaceAll("\\d{7,}", "XXXXXXX");
        assertEquals(expectedCause, actualCause);
    }
}
