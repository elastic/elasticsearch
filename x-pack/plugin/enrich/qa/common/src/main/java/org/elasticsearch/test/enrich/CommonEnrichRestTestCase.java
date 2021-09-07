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
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public abstract class CommonEnrichRestTestCase extends ESRestTestCase {

    @After
    public void deletePolicies() throws Exception {
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
    private <Property> Property unsafeGetProperty(Map<?, ?> map, String key) {
        return (Property) map.get(key);
    }

    private void setupGenericLifecycleTest(boolean deletePipeilne, String field, String type, String value) throws Exception {
        // Create source index:
        createSourceIndex("my-source-index");

        // Create the policy:
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource("my-source-index", field, type));
        assertOK(client().performRequest(putPolicyRequest));

        // Add entry to source index and then refresh:
        Request indexRequest = new Request("PUT", "/my-source-index/_doc/elastic.co");
        indexRequest.setJsonEntity("{" +
            "\"host\": \"elastic.co\"," +
            "\"globalRank\": 25," +
            "\"tldRank\": 7," +
            "\"tld\": \"co\", " +
            "\"date\": {" +
                "\"gte\" : \"2021-09-05\"," +
                "\"lt\" : \"2021-09-07\"" +
            "}, " +
            "\"integer\": {" +
                "\"gte\" : 40," +
                "\"lt\" : 42" +
            "}, " +
            "\"long\": {" +
                "\"gte\" : 8000000," +
                "\"lt\" : 9000000" +
            "}, " +
            "\"double\": {" +
                "\"gte\" : 10.10," +
                "\"lt\" : 20.20" +
            "}, " +
            "\"float\": {" +
                "\"gte\" : 10000.5," +
                "\"lt\" : 10000.7" +
            "}, " +
            "\"ip\": \"100.0.0.0/4\"" +
            "}");
        assertOK(client().performRequest(indexRequest));
        Request refreshRequest = new Request("POST", "/my-source-index/_refresh");
        assertOK(client().performRequest(refreshRequest));

        // Execute the policy:
        Request executePolicyRequest = new Request("POST", "/_enrich/policy/my_policy/_execute");
        assertOK(client().performRequest(executePolicyRequest));

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/my_pipeline");
        putPipelineRequest.setJsonEntity(
            "{\"processors\":[" + "{\"enrich\":{\"policy_name\":\"my_policy\",\"field\":\""+field+"\",\"target_field\":\"entry\"}}" + "]}"
        );
        assertOK(client().performRequest(putPipelineRequest));

        // Index document using pipeline with enrich processor:
        indexRequest = new Request("PUT", "/my-index/_doc/1");
        indexRequest.addParameter("pipeline", "my_pipeline");
        indexRequest.setJsonEntity("{\""+field+"\": \""+value+"\"}");
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

        if (deletePipeilne) {
            // delete the pipeline so the policies can be deleted
            client().performRequest(new Request("DELETE", "/_ingest/pipeline/my_pipeline"));
        }
    }

    public void testBasicFlowKeyword() throws Exception {
        setupGenericLifecycleTest(true, "host", "match", "elastic.co");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowDate() throws Exception {
        setupGenericLifecycleTest(true, "date", "range", "2021-09-06");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowInteger() throws Exception {
        setupGenericLifecycleTest(true, "integer", "range", "41");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowLong() throws Exception {
        setupGenericLifecycleTest(true, "long", "range", "8411017");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowDouble() throws Exception {
        setupGenericLifecycleTest(true, "double", "range", "15.15");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowFloat() throws Exception {
        setupGenericLifecycleTest(true, "float", "range", "10000.66666");
        assertBusy(CommonEnrichRestTestCase::verifyEnrichMonitoring, 3, TimeUnit.MINUTES);
    }

    public void testBasicFlowIp() throws Exception {
        setupGenericLifecycleTest(true, "ip", "range", "100.120.140.160");
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
        // lets not delete the pipeline at first, to test the failure
        setupGenericLifecycleTest(false, "host", "match", "elastic.co");

        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/another_pipeline");
        putPipelineRequest.setJsonEntity(
            "{\"processors\":[" + "{\"enrich\":{\"policy_name\":\"my_policy\",\"field\":\"host\",\"target_field\":\"entry\"}}" + "]}"
        );
        assertOK(client().performRequest(putPipelineRequest));

        ResponseException exc = expectThrows(
            ResponseException.class,
            () -> client().performRequest(new Request("DELETE", "/_enrich/policy/my_policy"))
        );
        assertTrue(
            exc.getMessage()
                .contains("Could not delete policy [my_policy] because" + " a pipeline is referencing it [my_pipeline, another_pipeline]")
        );

        // delete the pipelines so the policies can be deleted
        client().performRequest(new Request("DELETE", "/_ingest/pipeline/my_pipeline"));
        client().performRequest(new Request("DELETE", "/_ingest/pipeline/another_pipeline"));

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
            source.field("enrich_fields", new String[]{"globalRank", "tldRank", "tld"});
        }
        source.endObject().endObject();
        return Strings.toString(source);
    }

    public static void createSourceIndex(String index) throws IOException {
        String mapping = createSourceIndexMapping();
        createIndex(index, Settings.EMPTY, mapping);
    }

    public static String createSourceIndexMapping() {
        return "\"properties\":"
            + "{\"host\": {\"type\":\"keyword\"},"
            + "\"globalRank\":{\"type\":\"keyword\"},"
            + "\"tldRank\":{\"type\":\"keyword\"},"
            + "\"tld\":{\"type\":\"keyword\"},"
            + "\"date\":{\"type\":\"date_range\"" + (randomBoolean() ? "" : ", \"format\": \"yyyy-MM-dd\"") + "},"
            + "\"integer\":{\"type\":\"integer_range\"},"
            + "\"long\":{\"type\":\"long_range\"},"
            + "\"double\":{\"type\":\"double_range\"},"
            + "\"float\":{\"type\":\"float_range\"},"
            + "\"ip\":{\"type\":\"ip_range\"}"
            + "}";
    }

    protected static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    protected static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    private static void verifyEnrichMonitoring() throws IOException {
        Request request = new Request("GET", "/.monitoring-*/_search");
        request.setJsonEntity("{\"query\": {\"term\": {\"type\": \"enrich_coordinator_stats\"}}}");
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

        assertThat(maxRemoteRequestsTotal, greaterThanOrEqualTo(1));
        assertThat(maxExecutedSearchesTotal, greaterThanOrEqualTo(1));
    }
}
