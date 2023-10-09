/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test.enrich;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class CommonEnrichRestTestCase extends ESRestTestCase {

    private List<String> cleanupPipelines = new ArrayList<>();

    /**
     * Registers a pipeline for subsequent post-test clean up (i.e. DELETE),
     * see {@link CommonEnrichRestTestCase#deletePipelinesAndPolicies()}.
     *
     * @param pipeline the name of the pipeline to clean up
     */
    public void cleanupPipelineAfterTest(String pipeline) {
        cleanupPipelines.add(pipeline);
    }

    @After
    public void deletePipelinesAndPolicies() throws Exception {
        // delete pipelines
        for (String pipeline : cleanupPipelines) {
            String endpoint = "/_ingest/pipeline/" + pipeline;
            assertOK(client().performRequest(new Request("DELETE", endpoint)));
        }
        cleanupPipelines.clear();

        // delete all policies
        Map<String, Object> responseMap = toMap(adminClient().performRequest(new Request("GET", "/_enrich/policy")));
        List<Map<?, ?>> policies = unsafeGetProperty(responseMap, "policies");

        for (Map<?, ?> entry : policies) {
            Map<?, Map<String, ?>> config = unsafeGetProperty(entry, "config");
            Map<String, ?> policy = config.values().iterator().next();
            String endpoint = "/_enrich/policy/" + policy.get("name");
            assertOK(client().performRequest(new Request("DELETE", endpoint)));

            List<?> sourceIndices = (List<?>) policy.get("indices");
            for (Object sourceIndex : sourceIndices) {
                try {
                    client().performRequest(new Request("DELETE", "/" + sourceIndex));
                } catch (ResponseException e) {
                    // and that is ok
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static <Property> Property unsafeGetProperty(Map<?, ?> map, String key) {
        return (Property) map.get(key);
    }

    private void setupGenericLifecycleTest(String field, String type, String value) throws Exception {
        // Create source index:
        createSourceIndex("my-source-index");

        // Create the policy:
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource("my-source-index", field, type));
        assertOK(client().performRequest(putPolicyRequest));

        // Add entry to source index and then refresh:
        Request indexRequest = new Request("PUT", "/my-source-index/_doc/elastic.co");
        indexRequest.setJsonEntity("""
            {
              "host": "elastic.co",
              "globalRank": 25,
              "tldRank": 7,
              "tld": "co",
              "date": {
                "gte": "2021-09-05",
                "lt": "2021-09-07"
              },
              "integer": {
                "gte": 40,
                "lt": 42
              },
              "long": {
                "gte": 8000000,
                "lt": 9000000
              },
              "double": {
                "gte": 10.1,
                "lt": 20.2
              },
              "float": {
                "gte": 10000.5,
                "lt": 10000.7
              },
              "ip": "100.0.0.0/4"
            }""");
        assertOK(client().performRequest(indexRequest));
        Request refreshRequest = new Request("POST", "/my-source-index/_refresh");
        assertOK(client().performRequest(refreshRequest));

        // Execute the policy:
        Request executePolicyRequest = new Request("POST", "/_enrich/policy/my_policy/_execute");
        assertOK(client().performRequest(executePolicyRequest));

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/my_pipeline");
        putPipelineRequest.setJsonEntity(String.format(Locale.ROOT, """
            {
              "processors": [ { "enrich": { "policy_name": "my_policy", "field": "%s", "target_field": "entry" } } ]
            }""", field));
        assertOK(client().performRequest(putPipelineRequest));
        cleanupPipelineAfterTest("my_pipeline");

        // Create index before indexing, so that we avoid a warning from being emitted that can fail this test.
        // (If during auto index creation, the creation of my-index index happens together with monitoring index then
        // a 'starts with a dot '.', in the next major version' warning from creating monitor index is also returned
        // in the index response, because both indices were created in the same cluster state update.)
        // (This workaround, specifically using create index api to pre-create the my-index index ensures that this
        // index is created in isolation and warnings of other indices that may be created will not be returned)
        // (Go to elastic/elasticsearch#85506 for more details)
        createIndex("my-index");
        // Index document using pipeline with enrich processor:
        indexRequest = new Request("PUT", "/my-index/_doc/1");
        indexRequest.addParameter("pipeline", "my_pipeline");
        indexRequest.setJsonEntity("{\"" + field + "\": \"" + value + "\"}");
        assertOK(client().performRequest(indexRequest));

        // Check if document has been enriched
        Request getRequest = new Request("GET", "/my-index/_doc/1");
        Map<String, Object> response = toMap(client().performRequest(getRequest));
        Map<?, ?> entry = (Map<?, ?>) ((Map<?, ?>) response.get("_source")).get("entry");
        assertThat(entry.size(), equalTo(4));
        assertThat(entry.get(field), notNullValue());
        assertThat(entry.get("tld"), equalTo("co"));
        assertThat(entry.get("globalRank"), equalTo(25));
        assertThat(entry.get("tldRank"), equalTo(7));
        Object originalMatchValue = ((Map<?, ?>) response.get("_source")).get(field);
        assertThat(originalMatchValue, equalTo(value));
    }

    public void testBasicFlowKeyword() throws Exception {
        setupGenericLifecycleTest("host", "match", "elastic.co");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowDate() throws Exception {
        setupGenericLifecycleTest("date", "range", "2021-09-06");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowInteger() throws Exception {
        setupGenericLifecycleTest("integer", "range", "41");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowLong() throws Exception {
        setupGenericLifecycleTest("long", "range", "8411017");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowDouble() throws Exception {
        setupGenericLifecycleTest("double", "range", "15.15");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowFloat() throws Exception {
        setupGenericLifecycleTest("float", "range", "10000.66666");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowIp() throws Exception {
        setupGenericLifecycleTest("ip", "range", "100.120.140.160");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testImmutablePolicy() throws IOException {
        createSourceIndex("my-source-index");
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource("my-source-index"));
        assertOK(client().performRequest(putPolicyRequest));

        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertTrue(exc.getMessage().contains("policy [my_policy] already exists"));
    }

    public void testDeleteIsCaseSensitive() throws Exception {
        createSourceIndex("my-source-index");
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource("my-source-index"));
        assertOK(client().performRequest(putPolicyRequest));

        ResponseException exc = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", "/_enrich/policy/MY_POLICY"))
        );
        assertTrue(exc.getMessage().contains("policy [MY_POLICY] not found"));
    }

    public void testDeleteExistingPipeline() throws Exception {
        setupGenericLifecycleTest("host", "match", "elastic.co");

        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/another_pipeline");
        putPipelineRequest.setJsonEntity("""
            {
              "processors": [ { "enrich": { "policy_name": "my_policy", "field": "host", "target_field": "entry" } } ]
            }""");
        assertOK(client().performRequest(putPipelineRequest));
        cleanupPipelineAfterTest("another_pipeline");

        ResponseException exc = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", "/_enrich/policy/my_policy"))
        );
        assertTrue(exc.getMessage().contains("Could not delete policy [my_policy] because a pipeline is referencing it ["));
        assertTrue(exc.getMessage().contains("another_pipeline"));
        assertTrue(exc.getMessage().contains("my_pipeline"));

        // verify the delete did not happen
        Request getRequest = new Request("GET", "/_enrich/policy/my_policy");
        assertOK(client().performRequest(getRequest));
    }

    public static String generatePolicySource(String index) throws IOException {
        return generatePolicySource(index, "host", "match");
    }

    public static String generatePolicySource(String index, String field, String type) throws IOException {
        XContentBuilder source = jsonBuilder().startObject().startObject(type);
        {
            source.field("indices", index);
            if (randomBoolean()) {
                source.field("query", QueryBuilders.matchAllQuery());
            }
            source.field("match_field", field);
            source.field("enrich_fields", new String[] { "globalRank", "tldRank", "tld" });
        }
        source.endObject().endObject();
        return Strings.toString(source);
    }

    public static void createSourceIndex(String index) throws IOException {
        String mapping = createSourceIndexMapping();
        createIndex(index, Settings.EMPTY, mapping);
    }

    public static String createSourceIndexMapping() {
        return String.format(Locale.ROOT, """
            "properties": {
                "host": {
                  "type": "keyword"
                },
                "globalRank": {
                  "type": "keyword"
                },
                "tldRank": {
                  "type": "keyword"
                },
                "tld": {
                  "type": "keyword"
                },
                "date": {
                  "type": "date_range"
                  %s
                },
                "integer": {
                  "type": "integer_range"
                },
                "long": {
                  "type": "long_range"
                },
                "double": {
                  "type": "double_range"
                },
                "float": {
                  "type": "float_range"
                },
                "ip": {
                  "type": "ip_range"
                }
              }
            """, randomBoolean() ? "" : ", \"format\": \"yyyy-MM-dd\"");
    }

    protected static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    protected static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    private static void verifyEnrichMonitoring() throws IOException {
        Request request = new Request("GET", "/.monitoring-*/_search");
        request.setJsonEntity("""
            {
              "query": {"term": {"type": "enrich_coordinator_stats"}},
              "sort": [{"timestamp": "desc"}],
              "size": 5
            }
            """);
        Map<String, ?> response;
        try {
            response = toMap(adminClient().performRequest(request));
        } catch (ResponseException e) {
            throw new AssertionError("error while searching", e);
        }

        int maxRemoteRequestsTotal = 0;
        int maxExecutedSearchesTotal = 0;

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), greaterThanOrEqualTo(1));

        for (Object o : hits) {
            Map<?, ?> hit = (Map<?, ?>) o;

            int foundRemoteRequestsTotal = (int) XContentMapValues.extractValue(
                "_source.enrich_coordinator_stats.remote_requests_total",
                hit
            );
            maxRemoteRequestsTotal = Math.max(maxRemoteRequestsTotal, foundRemoteRequestsTotal);
            int foundExecutedSearchesTotal = (int) XContentMapValues.extractValue(
                "_source.enrich_coordinator_stats.executed_searches_total",
                hit
            );
            maxExecutedSearchesTotal = Math.max(maxExecutedSearchesTotal, foundExecutedSearchesTotal);
        }

        assertThat("Maximum remote_requests_total was zero. Response: " + response, maxRemoteRequestsTotal, greaterThanOrEqualTo(1));
        assertThat("Maximum executed_searches_total was zero. Response: " + response, maxExecutedSearchesTotal, greaterThanOrEqualTo(1));
    }
}
