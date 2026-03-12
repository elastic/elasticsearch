/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.test.enrich;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichAdvancedSecurityIT extends CommonEnrichRestTestCase {

    public static final String ADMIN_USER = "test_admin";
    public static final String ENRICH_USER = "test_enrich";
    public static final String TEST_PASSWORD = "x-pack-test-password";

    @ClassRule
    public static ElasticsearchCluster cluster = enrichCluster("trial", true).rolesFile(Resource.fromClasspath("advanced_roles.yml"))
        .user(ADMIN_USER, TEST_PASSWORD, "superuser", true)
        .user(ENRICH_USER, TEST_PASSWORD, "integ_test_role", false)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        return authRequestHeaderSetting(ENRICH_USER, TEST_PASSWORD);
    }

    @Override
    protected Settings restAdminSettings() {
        return authRequestHeaderSetting(ADMIN_USER, TEST_PASSWORD);
    }

    public void testEnrichEnforcesDLS() throws IOException {
        // Create the index and policy using the admin client
        final String sourceIndexName = "dls-source-index";
        setupSourceIndexAndPolicy(sourceIndexName);

        // Add another doc that doesn't match the DLS filter
        Request sourceDocIndexReq = new Request("PUT", "/" + sourceIndexName + "/_doc/example.com");
        sourceDocIndexReq.setJsonEntity("{\"host\": \"example.com\",\"globalRank\": 42,\"tldRank\": 7,\"tld\": \"com\"}");
        assertOK(adminClient().performRequest(sourceDocIndexReq));
        Request refreshRequest = new Request("POST", "/" + sourceIndexName + "/_refresh");
        assertOK(adminClient().performRequest(refreshRequest));

        // Execute the policy:
        Request executePolicyRequest = new Request("POST", "/_enrich/policy/my_policy/_execute");
        assertOK(client().performRequest(executePolicyRequest));

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/my_pipeline");
        putPipelineRequest.setJsonEntity("""
            {
              "processors": [
                {
                  "enrich": {
                    "policy_name": "my_policy",
                    "field": "host",
                    "target_field": "entry"
                  }
                }
              ]
            }""");
        assertOK(client().performRequest(putPipelineRequest));

        // Verify that the pipeline works as expected for the source doc included in the DLS filter
        {
            // Index document using pipeline with enrich processor:
            Request indexRequest = new Request("PUT", "/my-index/_doc/1");
            indexRequest.addParameter("pipeline", "my_pipeline");
            indexRequest.setJsonEntity("{\"host\": \"elastic.co\"}");
            assertOK(client().performRequest(indexRequest));

            // Check if document has been enriched
            Request getRequest = new Request("GET", "/my-index/_doc/1");
            Map<String, Object> response = toMap(client().performRequest(getRequest));
            Map<?, ?> entry = (Map<?, ?>) ((Map<?, ?>) response.get("_source")).get("entry");
            assertThat(entry.size(), equalTo(4));
            assertThat(entry.get("host"), equalTo("elastic.co"));
            assertThat(entry.get("tld"), equalTo("co"));
            assertThat(entry.get("globalRank"), equalTo(25));
            assertThat(entry.get("tldRank"), equalTo(7));
        }

        // Verify that we don't leak the source doc that isn't included in the DLS filter
        {
            // Index document using pipeline with enrich processor:
            Request indexRequest = new Request("PUT", "/my-index/_doc/2");
            indexRequest.addParameter("pipeline", "my_pipeline");
            indexRequest.setJsonEntity("{\"host\": \"example.com\"}");
            assertOK(client().performRequest(indexRequest));

            // Check if document has been enriched
            Request getRequest = new Request("GET", "/my-index/_doc/2");
            Map<String, Object> response = toMap(client().performRequest(getRequest));
            Map<?, ?> entry = (Map<?, ?>) ((Map<?, ?>) response.get("_source")).get("entry");
            assertThat("the document should not have been enriched", entry, nullValue());
        }

        // delete the pipeline so the policies can be deleted
        client().performRequest(new Request("DELETE", "/_ingest/pipeline/my_pipeline"));
    }

    public void testEnrichEnforcesFLS() throws IOException {
        // Create the index and policy using the admin client
        setupSourceIndexAndPolicy("fls-source-index");

        // Execute the policy:
        Request executePolicyRequest = new Request("POST", "/_enrich/policy/my_policy/_execute");
        assertOK(client().performRequest(executePolicyRequest));

        // Create pipeline
        Request putPipelineRequest = new Request("PUT", "/_ingest/pipeline/my_pipeline");
        putPipelineRequest.setJsonEntity("""
            {
              "processors": [
                {
                  "enrich": {
                    "policy_name": "my_policy",
                    "field": "host",
                    "target_field": "entry"
                  }
                }
              ]
            }""");
        assertOK(client().performRequest(putPipelineRequest));

        // Index document using pipeline with enrich processor:
        Request indexRequest = new Request("PUT", "/my-index/_doc/1");
        indexRequest.addParameter("pipeline", "my_pipeline");
        indexRequest.setJsonEntity("{\"host\": \"elastic.co\"}");
        assertOK(client().performRequest(indexRequest));

        // Check if document has been enriched
        Request getRequest = new Request("GET", "/my-index/_doc/1");
        Map<String, Object> response = toMap(client().performRequest(getRequest));
        Map<?, ?> entry = (Map<?, ?>) ((Map<?, ?>) response.get("_source")).get("entry");
        assertThat(entry.size(), equalTo(3));
        assertThat(entry.get("host"), equalTo("elastic.co"));
        assertThat(entry.get("tld"), equalTo("co"));
        assertThat(entry.get("tldRank"), equalTo(7));
        assertThat("Field [globalRank] should not be present due to FLS restrictions", entry.get("globalRank"), nullValue());

        // delete the pipeline so the policies can be deleted
        client().performRequest(new Request("DELETE", "/_ingest/pipeline/my_pipeline"));
    }

    private void setupSourceIndexAndPolicy(String sourceIndexName) throws IOException {
        // Create source index:
        createSourceIndex(sourceIndexName);
        // Create the policy:
        Request putPolicyRequest = new Request("PUT", "/_enrich/policy/my_policy");
        putPolicyRequest.setJsonEntity(generatePolicySource(sourceIndexName));
        assertOK(adminClient().performRequest(putPolicyRequest));

        // Add entry to source index and then refresh:
        Request indexRequest = new Request("PUT", "/" + sourceIndexName + "/_doc/elastic.co");
        indexRequest.setJsonEntity("""
            {
              "host": "elastic.co",
              "globalRank": 25,
              "tldRank": 7,
              "tld": "co"
            }""");
        assertOK(adminClient().performRequest(indexRequest));
        Request refreshRequest = new Request("POST", "/" + sourceIndexName + "/_refresh");
        assertOK(adminClient().performRequest(refreshRequest));
    }
}
