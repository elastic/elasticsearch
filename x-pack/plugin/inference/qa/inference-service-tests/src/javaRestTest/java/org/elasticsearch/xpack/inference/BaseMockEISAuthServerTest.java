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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

public class BaseMockEISAuthServerTest extends ESRestTestCase {

    // The reason we're retrying is there's a race condition between the node retrieving the
    // authorization response and running the test. Retrieving the authorization should be very fast since
    // we're hosting a local mock server but it's possible it could respond slower. So in the even of a test failure
    // we'll automatically retry after waiting a second.
    @Rule
    public RetryRule retry = new RetryRule(3, TimeValue.timeValueSeconds(1));

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer = MockElasticInferenceServiceAuthorizationServer
        .enabledWithRainbowSprinklesAndElser();

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        // Adding both settings unless one feature flag is disabled in a particular environment
        .setting("xpack.inference.elastic.url", mockEISServer::getUrl)
        // If we don't disable this there's a very small chance that the authorization code could attempt to make two
        // calls which would result in a test failure because the webserver is only expecting a single request
        // So to ensure we avoid that all together, this flag indicates that we'll only perform a single authorization request
        .setting("xpack.inference.elastic.periodic_authorization_enabled", "false")
        // This plugin is located in the inference/qa/test-service-plugin package, look for TestInferenceServicePlugin
        .plugin("inference-service-test")
        .user("x_pack_rest_user", "x-pack-test-password")
        .feature(FeatureFlag.INFERENCE_CUSTOM_SERVICE_ENABLED)
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
}
