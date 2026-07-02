/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.http.MockRequest;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER;
import static org.hamcrest.Matchers.is;

/**
 * Verifies that putting or deleting a region policy immediately triggers a refresh of the EIS authorization
 * (rather than waiting for the next periodic authorization poll), and that the refresh request carries the
 * region/geo preference headers derived from the configured policy.
 */
public class RegionPolicyAuthorizationRefreshIT extends ESRestTestCase {

    private static final String REGION_POLICY_PATH = "/_inference/_region_policy";
    // See ElasticInferenceServiceAuthorizationRequest#AUTHORIZATION_PATH
    private static final String AUTHORIZATION_PATH = "/api/v2/authorizations";
    private static final String TEST_USERNAME = "x_pack_rest_user";
    private static final String TEST_PASSWORD = "x-pack-test-password";

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
        .setting(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        // Disables the periodic authorization poller entirely so the only authorization requests we observe
        // are the ones triggered explicitly by putting/deleting the region policy below.
        .setting(ElasticInferenceServiceSettings.AUTHORIZATION_ENABLED.getKey(), "false")
        .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "false")
        .feature(FeatureFlag.INFERENCE_REGION_POLICY)
        .plugin("inference-service-test")
        .user(TEST_USERNAME, TEST_PASSWORD)
        .build();

    // The reason we're doing this is to make sure the mock server is initialized first so we can get the address before communicating
    // it to the cluster as a setting.
    // Note: @ClassRule is executed once for the entire test class
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(TEST_USERNAME, new SecureString(TEST_PASSWORD.toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Before
    public void clearRecordedRequests() {
        mockEISServer.getWebServer().clearRequests();
    }

    public void testPutRegionPolicy_TriggersAuthorizationRefresh_WithAllowedRegionsHeader() throws IOException {
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        putRegionPolicy("""
            {
              "region_policy": {
                "allowed_regions": [ { "csp": "aws", "region": "us-east-1" } ]
              }
            }
            """);

        var authRequest = assertSingleAuthorizationRequest();
        assertThat(authRequest.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER), is("aws:us-east-1"));
        assertNull(authRequest.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER));

        mockEISServer.enqueueAuthorizeAllModelsResponse();
        deleteRegionPolicy();
    }

    public void testPutRegionPolicy_TriggersAuthorizationRefresh_WithAllowedGeosHeader() throws IOException {
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        putRegionPolicy("""
            {
              "region_policy": {
                "allowed_geos": [ "us", "eu" ]
              }
            }
            """);

        var authRequest = assertSingleAuthorizationRequest();
        assertThat(authRequest.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), is("us,eu"));
        assertNull(authRequest.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER));

        mockEISServer.enqueueAuthorizeAllModelsResponse();
        deleteRegionPolicy();
    }

    public void testDeleteRegionPolicy_TriggersAuthorizationRefresh() throws IOException {
        mockEISServer.enqueueAuthorizeAllModelsResponse();
        putRegionPolicy("""
            {
              "region_policy": {
                "allowed_geos": [ "us" ]
              }
            }
            """);
        mockEISServer.getWebServer().clearRequests();

        mockEISServer.enqueueAuthorizeAllModelsResponse();
        deleteRegionPolicy();

        assertSingleAuthorizationRequest();
    }

    private static MockRequest assertSingleAuthorizationRequest() {
        var requests = mockEISServer.getWebServer().requests();
        assertThat(requests.size(), is(1));
        var request = requests.get(0);
        assertThat(request.getUri().getPath(), is(AUTHORIZATION_PATH));
        assertThat(request.getMethod(), is("GET"));
        return request;
    }

    private static void putRegionPolicy(String body) throws IOException {
        var request = new Request("PUT", REGION_POLICY_PATH);
        request.setJsonEntity(body);
        assertOK(client().performRequest(request));
    }

    private static void deleteRegionPolicy() throws IOException {
        assertOK(client().performRequest(new Request("DELETE", REGION_POLICY_PATH)));
    }
}
