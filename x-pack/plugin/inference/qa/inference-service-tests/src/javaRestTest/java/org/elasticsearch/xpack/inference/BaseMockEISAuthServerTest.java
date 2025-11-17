/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 *
 * this file has been contributed to by a Generative AI
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.RetryRule;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.inference.services.elastic.InternalPreconfiguredEndpoints;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getModel;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;

public class BaseMockEISAuthServerTest extends ESRestTestCase {

    protected static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    static {
        // Ensure that the mock EIS server has an authorized response prior to the cluster starting
        mockEISServer.enqueueAuthorizeAllModelsResponse();
    }

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        // Adding both settings unless one feature flag is disabled in a particular environment
        .setting("xpack.inference.elastic.url", mockEISServer::getUrl)
        // If we don't disable this there's a very small chance that the authorization code could attempt to make two
        // calls which would result in a test failure because the webserver is only expecting a single request
        // So to ensure we avoid that all together, this flag indicates that we'll only perform a single authorization request
        .setting("xpack.inference.elastic.periodic_authorization_enabled", "false")
        // Setting to false so that the CCM logic will be skipped when running the tests, the authorization logic skip trying to determine
        // if CCM is enabled
        .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "false")
        // This plugin is located in the inference/qa/test-service-plugin package, look for TestInferenceServicePlugin
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .build();

    // The reason we're doing this is to make sure the mock server is initialized first so we can get the address before communicating
    // it to the cluster as a setting.
    // Note: @ClassRule is executed once for the entire test class
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    // The reason we're retrying is there's a race condition between the node retrieving the
    // authorization response and running the test. Retrieving the authorization should be very fast since
    // we're hosting a local mock server but it's possible it could respond slower. So in the event of a test failure
    // we'll automatically retry after waiting a second.
    // Note: @Rule is executed for each test
    @Rule
    public RetryRule retry = new RetryRule(3, TimeValue.timeValueSeconds(1));

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("x_pack_rest_user", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // Keep the cluster around so the EIS preconfigured endpoints still exist between tests. Otherwise, the inference indices will
        // be removed when the cluster is wiped which causes the tests after the first one to fail.
        return true;
    }

    @Before
    public void ensureEisPreconfiguredEndpointsExist() throws Exception {
        // Ensure that the authorization logic has completed prior to running each test so we have the correct EIS preconfigured endpoints
        // available
        // Technically this only needs to be done before the suite runs but the underlying client is created in @Before and not statically
        // for the suite
        assertBusy(() -> getModel(InternalPreconfiguredEndpoints.DEFAULT_ELSER_ENDPOINT_ID_V2));
    }
}
