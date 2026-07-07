/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.http.MockResponse;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class RegionPolicyAuthorizationRefreshIT extends ESRestTestCase {

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .feature(FeatureFlag.INFERENCE_REGION_POLICY)
        .setting("xpack.inference.elastic.url", mockEISServer::getUrl)
        .setting("xpack.inference.elastic.periodic_authorization_enabled", "false")
        // Disable the initial bootup authorization request so that each PUT/DELETE below triggers exactly one
        // authorization request via the refresh call, keeping the mock server's request count easy to assert on.
        .setting("xpack.inference.elastic.authorization.enabled", "false")
        .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "false")
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    // The reason we're doing this is to make sure the mock server is initialized first so we can get the address before communicating
    // it to the cluster as a setting.
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void clearMockServerRequests() {
        mockEISServer.getWebServer().clearRequests();
    }

    public void testPutRegionPolicy_WithAllowedRegions_TriggersAuthorizationRefresh() throws IOException {
        // The put region policy action makes two authorization requests: one to check whether the new policy would deny any
        // in-use inference endpoints, and one (the actual refresh) after the policy has been persisted.
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        mockEISServer.enqueueAuthorizeAllModelsResponse();

        var putRequest = new Request("PUT", "/_inference/_region_policy");
        putRequest.setJsonEntity("""
            {
              "region_policy": {
                "allowed_regions": [
                  { "csp": "aws", "region": "eu-west-1" }
                ]
              }
            }
            """);
        client().performRequest(putRequest);

        assertThat(mockEISServer.getWebServer().requests().size(), is(2));
        for (var request : mockEISServer.getWebServer().requests()) {
            assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER), is("aws:eu-west-1"));
            assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), nullValue());
        }
    }

    public void testPutRegionPolicy_WithAllowedGeos_TriggersAuthorizationRefresh() throws IOException {
        // The put region policy action makes two authorization requests: one to check whether the new policy would deny any
        // in-use inference endpoints, and one (the actual refresh) after the policy has been persisted.
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        mockEISServer.enqueueAuthorizeAllModelsResponse();

        var putRequest = new Request("PUT", "/_inference/_region_policy");
        putRequest.setJsonEntity("""
            {
              "region_policy": {
                "allowed_geos": ["eu", "us"]
              }
            }
            """);
        client().performRequest(putRequest);

        assertThat(mockEISServer.getWebServer().requests().size(), is(2));
        for (var request : mockEISServer.getWebServer().requests()) {
            assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), is("eu,us"));
            assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER), nullValue());
        }
    }

    public void testPutRegionPolicy_WithForceTrue_SkipsPreCheckAuthorizationRequest() throws IOException {
        // With force=true the pre-check is skipped entirely, so only the post-write refresh call is made.
        mockEISServer.enqueueAuthorizeAllModelsResponse();

        var putRequest = new Request("PUT", "/_inference/_region_policy");
        putRequest.addParameter("force", "true");
        putRequest.setJsonEntity("""
            {
              "region_policy": {
                "allowed_geos": ["eu"]
              }
            }
            """);
        client().performRequest(putRequest);

        assertThat(mockEISServer.getWebServer().requests().size(), is(1));
        var request = mockEISServer.getWebServer().requests().get(0);
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), is("eu"));
    }

    public void testDeleteRegionPolicy_TriggersAuthorizationRefresh() throws IOException {
        // Two authorization requests for the put (pre-check + refresh), then one for the delete (refresh only).
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        var putRequest = new Request("PUT", "/_inference/_region_policy");
        putRequest.setJsonEntity("""
            {
              "region_policy": {
                "allowed_geos": ["eu"]
              }
            }
            """);
        client().performRequest(putRequest);

        mockEISServer.enqueueAuthorizeAllModelsResponse();
        client().performRequest(new Request("DELETE", "/_inference/_region_policy"));

        assertThat(mockEISServer.getWebServer().requests().size(), is(3));
        var request = mockEISServer.getWebServer().requests().get(2);
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER), nullValue());
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testPutRegionPolicy_WhenDeniedEndpointReferencedByPipelineOnly_OmitsEmptyIndexesField() throws IOException {
        String endpointId = "denied-endpoint-pipeline-only";
        String pipelineId = "pipeline-referencing-" + endpointId;
        putPipeline(pipelineId, endpointId);

        try {
            mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(deniedEndpointAuthResponse(endpointId)));

            var exception = expectThrows(ResponseException.class, () -> putRegionPolicy("""
                { "region_policy": { "allowed_geos": ["eu"] } }
                """));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(409));

            var error = (Map<String, Object>) entityAsMap(exception.getResponse()).get("error");
            // A single denied endpoint / single reference serializes as a bare string, not a one-element array
            assertThat(error.get("denied_endpoint_ids"), is(endpointId));
            assertThat(error.get("referencing_pipelines"), is(endpointId + ":" + pipelineId));
            assertThat(error, not(hasKey("referencing_indexes")));
        } finally {
            deletePipeline(pipelineId);
        }
    }

    @SuppressWarnings("unchecked")
    public void testPutRegionPolicy_WhenDeniedEndpointReferencedByIndexOnly_OmitsEmptyPipelinesField() throws IOException {
        String endpointId = "denied-endpoint-index-only";
        String indexName = "index-referencing-" + endpointId;
        putTestServiceSparseEmbeddingModel(endpointId);
        putSemanticText(endpointId, indexName);

        try {
            mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(deniedEndpointAuthResponse(endpointId)));

            var exception = expectThrows(ResponseException.class, () -> putRegionPolicy("""
                { "region_policy": { "allowed_geos": ["eu"] } }
                """));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(409));

            var error = (Map<String, Object>) entityAsMap(exception.getResponse()).get("error");
            assertThat(error.get("denied_endpoint_ids"), is(endpointId));
            assertThat(error, not(hasKey("referencing_pipelines")));
            assertThat(error.get("referencing_indexes"), is(endpointId + ":" + indexName));
        } finally {
            client().performRequest(new Request("DELETE", indexName));
            client().performRequest(new Request("DELETE", "_inference/" + endpointId + "?force=true"));
        }
    }

    @SuppressWarnings("unchecked")
    public void testPutRegionPolicy_WhenDeniedEndpointReferencedByMultiplePipelines_SerializesAsArray() throws IOException {
        String endpointId = "denied-endpoint-multi-pipeline";
        String pipelineIdOne = "pipeline-one-referencing-" + endpointId;
        String pipelineIdTwo = "pipeline-two-referencing-" + endpointId;
        putPipeline(pipelineIdOne, endpointId);
        putPipeline(pipelineIdTwo, endpointId);

        try {
            mockEISServer.getWebServer().enqueue(new MockResponse().setResponseCode(200).setBody(deniedEndpointAuthResponse(endpointId)));

            var exception = expectThrows(ResponseException.class, () -> putRegionPolicy("""
                { "region_policy": { "allowed_geos": ["eu"] } }
                """));
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(409));

            var error = (Map<String, Object>) entityAsMap(exception.getResponse()).get("error");
            // A single denied endpoint still serializes as a bare string...
            assertThat(error.get("denied_endpoint_ids"), is(endpointId));
            // ...but multiple references for that endpoint serialize as an array
            assertThat(
                (List<String>) error.get("referencing_pipelines"),
                contains(endpointId + ":" + pipelineIdOne, endpointId + ":" + pipelineIdTwo)
            );
            assertThat(error, not(hasKey("referencing_indexes")));
        } finally {
            deletePipeline(pipelineIdOne);
            deletePipeline(pipelineIdTwo);
        }
    }

    private void putRegionPolicy(String body) throws IOException {
        var putRequest = new Request("PUT", "/_inference/_region_policy");
        putRequest.setJsonEntity(body);
        client().performRequest(putRequest);
    }

    private void putPipeline(String pipelineId, String modelId) throws IOException {
        var request = new Request("PUT", Strings.format("_ingest/pipeline/%s", pipelineId));
        request.setJsonEntity(Strings.format("""
            {
              "description": "Test pipeline",
              "processors": [
                {
                  "inference": {
                    "model_id": "%s"
                  }
                }
              ]
            }
            """, modelId));
        client().performRequest(request);
    }

    private void deletePipeline(String pipelineId) throws IOException {
        client().performRequest(new Request("DELETE", Strings.format("_ingest/pipeline/%s", pipelineId)));
    }

    private void putTestServiceSparseEmbeddingModel(String endpointId) throws IOException {
        var request = new Request("PUT", Strings.format("_inference/sparse_embedding/%s", endpointId));
        request.setJsonEntity("""
            {
              "service": "test_service",
              "service_settings": {
                "model": "my_model",
                "api_key": "abc64"
              },
              "task_settings": {
                "temperature": 3
              }
            }
            """);
        client().performRequest(request);
    }

    private void putSemanticText(String endpointId, String indexName) throws IOException {
        var request = new Request("PUT", indexName);
        request.setJsonEntity(Strings.format("""
            {
              "mappings": {
                "properties": {
                  "inference_field": {
                    "type": "semantic_text",
                    "inference_id": "%s"
                  }
                }
              }
            }
            """, endpointId));
        client().performRequest(request);
    }

    private static String deniedEndpointAuthResponse(String endpointId) {
        return Strings.format("""
            {
              "inference_endpoints": [
                {
                  "id": "%s",
                  "model_name": "test-model",
                  "task_types": {
                    "eis": "embed/text/sparse",
                    "elasticsearch": "sparse_embedding"
                  },
                  "status": "preview",
                  "release_date": "2024-05-01",
                  "denied_by_region_policy": true
                }
              ]
            }
            """, endpointId);
    }
}
