/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.test.enrich;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.After;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public abstract class CommonEnrichRestTestCase extends ESRestTestCase {

    @After
    private void deletePolicies() throws Exception {
        Map<String, Object> responseMap = toMap(adminClient().performRequest(new Request("GET", "/_enrich/policy")));
        @SuppressWarnings("unchecked")
        List<Map<?,?>> policies = (List<Map<?,?>>) responseMap.get("policies");

        for (Map<?, ?> entry: policies) {
            client().performRequest(new Request("DELETE", "/_enrich/policy/" + entry.get("name")));
        }
    }

    public void testBasicFlow() throws Exception {
        // Create the policy:
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity("{\"type\": \"exact_match\",\"indices\": [\"my-source-index\"], \"enrich_key\": \"host\", " +
            "\"enrich_values\": [\"globalRank\", \"tldRank\", \"tld\"]}");
        assertOK(client().performRequest(putPolicyRequest));

        // Add entry to source index and then refresh:
        Request indexRequest = new Request("PUT", "/my-source-index/_doc/elastic.co");
        indexRequest.setJsonEntity("{\"host\": \"elastic.co\",\"globalRank\": 25,\"tldRank\": 7,\"tld\": \"co\"}");
        assertOK(client().performRequest(indexRequest));
        Request refreshRequest = new Request("POST", "/my-source-index/_refresh");
        assertOK(client().performRequest(refreshRequest));

        // Execute the policy:
        Request executePolicyRequest = new Request("POST", "/_enrich/policy/my_policy/_execute");
        assertOK(client().performRequest(executePolicyRequest));

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/my_pipeline");
        putPipelineRequest.setJsonEntity("{\"processors\":[" +
            "{\"enrich\":{\"policy_name\":\"my_policy\",\"enrich_key\":\"host\",\"enrich_values\":[" +
            "{\"source\":\"globalRank\",\"target\":\"global_rank\"}," +
            "{\"source\":\"tldRank\",\"target\":\"tld_rank\"}" +
            "]}}" +
            "]}");
        assertOK(client().performRequest(putPipelineRequest));

        // Index document using pipeline with enrich processor:
        indexRequest = new Request("PUT", "/my-index/_doc/1");
        indexRequest.addParameter("pipeline", "my_pipeline");
        indexRequest.setJsonEntity("{\"host\": \"elastic.co\"}");
        assertOK(client().performRequest(indexRequest));

        // Check if document has been enriched
        Request getRequest = new Request("GET", "/my-index/_doc/1");
        Map<String, Object> response = toMap(client().performRequest(getRequest));
        Map<?, ?> _source = (Map<?, ?>) response.get("_source");
        assertThat(_source.size(), equalTo(3));
        assertThat(_source.get("host"), equalTo("elastic.co"));
        assertThat(_source.get("global_rank"), equalTo(25));
        assertThat(_source.get("tld_rank"), equalTo(7));
    }

    public void testImmutablePolicy() throws IOException {
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity("{\"type\": \"exact_match\",\"indices\": [\"my-source-index\"], \"enrich_key\": \"host\", " +
            "\"enrich_values\": [\"globalRank\", \"tldRank\", \"tld\"]}");
        assertOK(client().performRequest(putPolicyRequest));

        ResponseException exc = expectThrows(ResponseException.class, () -> client().performRequest(putPolicyRequest));
        assertTrue(exc.getMessage().contains("policy [my_policy] already exists"));
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }
}
