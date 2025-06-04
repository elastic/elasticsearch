/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.script.mustache;

import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.action.admin.cluster.storedscripts.DeleteStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptRequest;
import org.elasticsearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportDeleteStoredScriptAction;
import org.elasticsearch.action.admin.cluster.storedscripts.TransportPutStoredScriptAction;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.DummyQueryParserPlugin;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentParseException;
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

import static org.elasticsearch.action.admin.cluster.storedscripts.StoredScriptIntegTestUtils.newPutStoredScriptTestRequest;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.matchesRegex;

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
        prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("text", "value1").endObject()).get();
        prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("text", "value2").endObject()).get();
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

        assertResponse(
            new SearchTemplateRequestBuilder(client()).setRequest(searchRequest)
                .setScript(query)
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(Collections.singletonMap("my_size", 1)),
            searchResponse -> assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1))
        );
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
        assertResponse(
            client().execute(MustachePlugin.SEARCH_TEMPLATE_ACTION, request),
            searchResponse -> assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1))
        );
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
        assertResponse(
            client().execute(MustachePlugin.SEARCH_TEMPLATE_ACTION, request),
            searchResponse -> assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1))
        );
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
        assertResponse(
            client().execute(MustachePlugin.SEARCH_TEMPLATE_ACTION, request),
            searchResponse -> assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1))
        );
    }

    public void testIndexedTemplateClient() {
        putJsonStoredScript("testTemplate", """
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
            }""");

        GetStoredScriptResponse getResponse = safeExecute(
            GetStoredScriptAction.INSTANCE,
            new GetStoredScriptRequest(TEST_REQUEST_TIMEOUT, "testTemplate")
        );
        assertNotNull(getResponse.getSource());

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        indicesAdmin().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        assertHitCount(
            new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
                .setScript("testTemplate")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams),
            4
        );

        assertAcked(
            safeExecute(
                TransportDeleteStoredScriptAction.TYPE,
                new DeleteStoredScriptRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "testTemplate")
            )
        );

        getResponse = safeExecute(GetStoredScriptAction.INSTANCE, new GetStoredScriptRequest(TEST_REQUEST_TIMEOUT, "testTemplate"));
        assertNull(getResponse.getSource());
    }

    public void testBadTemplate() {

        // This template will produce badly formed json if given a multi-valued `text_fields` parameter,
        // as it does not add commas between the entries. We test that it produces a 400 json parsing
        // error both when used directly and when used in a render template request.

        String script = """
            {
                "query": {
                    "multi_match": {
                      "query": "{{query_string}}",
                      "fields": [{{#text_fields}}"{{name}}^{{boost}}"{{/text_fields}}]
                    }
                },
                "from": "{{from}}",
                "size": "{{size}}"
            }""";

        Map<String, Object> params = Map.of(
            "text_fields",
            List.of(Map.of("name", "title", "boost", 10), Map.of("name", "description", "boost", 2)),
            "from",
            0,
            "size",
            0
        );

        {
            XContentParseException e = expectThrows(XContentParseException.class, () -> {
                new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest())
                    .setScript(script)
                    .setScriptParams(params)
                    .setScriptType(ScriptType.INLINE)
                    .get();
            });
            assertThat(e.getMessage(), containsString("Unexpected character"));
        }

        {
            XContentParseException e = expectThrows(XContentParseException.class, () -> {
                new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest())
                    .setScript(script)
                    .setScriptParams(params)
                    .setScriptType(ScriptType.INLINE)
                    .setSimulate(true)
                    .get();
            });
            assertThat(e.getMessage(), containsString("Unexpected character"));
        }
    }

    public void testIndexedTemplate() {

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

        putJsonStoredScript("1a", script);
        putJsonStoredScript("2", script);
        putJsonStoredScript("3", script);

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        indicesAdmin().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");
        assertHitCount(
            new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest().indices("test"))
                .setScript("1a")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams),
            4
        );

        expectThrows(
            ResourceNotFoundException.class,
            () -> new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest().indices("test"))
                .setScript("1000")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams)
                .get()
        );

        templateParams.put("fieldParam", "bar");
        assertHitCount(
            new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
                .setScript("2")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams),
            1
        );
    }

    // Relates to #10397
    public void testIndexedTemplateOverwrite() throws Exception {
        createIndex("testindex");
        ensureGreen("testindex");

        prepareIndex("testindex").setId("1").setSource(jsonBuilder().startObject().field("searchtext", "dev1").endObject()).get();
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
            putJsonStoredScript("git01", query.replace("{{slop}}", Integer.toString(-1)));

            GetStoredScriptResponse getResponse = safeExecute(
                GetStoredScriptAction.INSTANCE,
                new GetStoredScriptRequest(TEST_REQUEST_TIMEOUT, "git01")
            );
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

            putJsonStoredScript("git01", query.replace("{{slop}}", Integer.toString(0)));

            assertHitCount(
                new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("testindex"))
                    .setScript("git01")
                    .setScript("git01")
                    .setScriptType(ScriptType.STORED)
                    .setScriptParams(templateParams),
                1
            );
        }
    }

    public void testIndexedTemplateWithArray() {
        putJsonStoredScript("4", """
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
            }""");

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        indicesAdmin().prepareRefresh().get();

        Map<String, Object> arrayTemplateParams = new HashMap<>();
        String[] fieldParams = { "foo", "bar" };
        arrayTemplateParams.put("fieldParam", fieldParams);

        assertHitCount(
            new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
                .setScript("4")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(arrayTemplateParams),
            5
        );
    }

    /**
     * Test that triggering the CCS compatibility check with a query that shouldn't go to the minor before
     * TransportVersions.MINIMUM_CCS_VERSION works
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
            () -> client().execute(MustachePlugin.SEARCH_TEMPLATE_ACTION, request).get()
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

        assertThat(
            underlying.getMessage(),
            matchesRegex(
                "\\[fail_before_current_version] was released first in version .+,"
                    + " failed compatibility check trying to send it to node with version .+"
            )
        );
    }

    public static void assertHitCount(SearchTemplateRequestBuilder requestBuilder, long expectedHitCount) {
        assertResponse(requestBuilder, response -> ElasticsearchAssertions.assertHitCount(response.getResponse(), expectedHitCount));
    }

    private void putJsonStoredScript(String id, String jsonContent) {
        assertAcked(safeExecute(TransportPutStoredScriptAction.TYPE, newPutStoredScriptTestRequest(id, jsonContent)));
    }
}
