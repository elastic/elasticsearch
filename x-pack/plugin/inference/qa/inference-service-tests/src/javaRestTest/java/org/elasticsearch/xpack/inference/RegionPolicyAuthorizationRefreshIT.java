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
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;

import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER;
import static org.elasticsearch.xpack.inference.services.elastic.request.ElasticInferenceServiceRequest.X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER;
import static org.hamcrest.Matchers.is;
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

        assertThat(mockEISServer.getWebServer().requests().size(), is(1));
        var request = mockEISServer.getWebServer().requests().get(0);
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER), is("aws:eu-west-1"));
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), nullValue());
    }

    public void testPutRegionPolicy_WithAllowedGeos_TriggersAuthorizationRefresh() throws IOException {
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

        assertThat(mockEISServer.getWebServer().requests().size(), is(1));
        var request = mockEISServer.getWebServer().requests().get(0);
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), is("eu,us"));
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER), nullValue());
    }

    public void testDeleteRegionPolicy_TriggersAuthorizationRefresh() throws IOException {
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

        assertThat(mockEISServer.getWebServer().requests().size(), is(2));
        var request = mockEISServer.getWebServer().requests().get(1);
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_REGIONS_HEADER), nullValue());
        assertThat(request.getHeader(X_ELASTIC_INFERENCE_ALLOWED_GEOS_HEADER), nullValue());
    }
}
