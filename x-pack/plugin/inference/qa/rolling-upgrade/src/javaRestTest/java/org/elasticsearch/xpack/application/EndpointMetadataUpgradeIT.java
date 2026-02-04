/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.upgrades.ParameterizedRollingUpgradeTestCase;
import org.elasticsearch.xpack.inference.MockElasticInferenceServiceAuthorizationServer;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceService;
import org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.inference.ModelConfigurations.INFERENCE_ID_FIELD_NAME;
import static org.elasticsearch.xpack.application.AuthorizationTaskExecutorUpgradeIT.doesAuthPollingTaskExist;
import static org.elasticsearch.xpack.inference.CCMRestBaseIT.ENABLE_CCM_REQUEST;
import static org.elasticsearch.xpack.inference.CCMRestBaseIT.putCCMConfiguration;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.getAllModels;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED;
import static org.elasticsearch.xpack.inference.services.elastic.response.ElasticInferenceServiceAuthorizationResponseEntityTests.ELSER_V2_ENDPOINT_ID;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;

/**
 * Rolling upgrade test for the EndpointMetadata field. In a mixed cluster, storing a preconfigured
 * endpoint that includes EndpointMetadata causes a mapping exception because old nodes lack the
 * metadata field. This test verifies that no EIS endpoints are created in a mixed cluster when the
 * mock returns a preconfigured endpoint, and that the endpoint is created correctly after full upgrade.
 */
public class EndpointMetadataUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    static {
        // Don't enqueue any initial responses - we control responses per phase (empty until mixed, then preconfigured)
        mockEISServer.init(0);
    }

    private static final Logger logger = LogManager.getLogger(EndpointMetadataUpgradeIT.class);
    /**
     * Only run when old cluster lacks the endpoint metadata mapping (so we get mapping exception in mixed cluster).
     */
    private static final String ENDPOINT_METADATA_FEATURE = "inference_endpoint_metadata_fields_added";

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterVersion(), isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        .setting(ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
        .setting(CCMSettings.CCM_SUPPORTED_ENVIRONMENT.getKey(), "true")
        .build();

    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    private static final String GET_METHOD = "GET";

    private final AtomicBoolean initializedCcm = new AtomicBoolean(false);
    private final AtomicBoolean authRequestReceivedInUpgradedCluster = new AtomicBoolean(false);

    public EndpointMetadataUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    public void testEndpointMetadataMappingInRollingUpgrade() throws Exception {
        assumeTrue(
            "Only run when old cluster lacks endpoint metadata mapping (causes mapping exception in mixed cluster)",
            oldClusterHasFeature(ENDPOINT_METADATA_FEATURE) == false
        );

        final var isBeforeAnyNodesUpgraded = isOldCluster();
        final var isDuringNodeUpgrades = isMixedCluster();
        final var isFullyUpgraded = isUpgradedCluster();

        if (isBeforeAnyNodesUpgraded) {
            logger.info("Old cluster - CCM not enabled, no EIS endpoints expected");
            assertNoEisEndpoints();
        }

        if (isDuringNodeUpgrades) {
            logger.info(
                "Mixed cluster - enable CCM, mock returns preconfigured endpoint; store should fail, so no EIS endpoints should be created"
            );
            if (initializedCcm.compareAndSet(false, true)) {
                mockEISServer.enqueuePreconfiguredEndpointResponse();
                var response = putCCMConfiguration(ENABLE_CCM_REQUEST);
                assertTrue(response.isEnabled());
            } else {
                // Already enabled in a previous mixed run; enqueue empty so any subsequent auth poll has a response
                mockEISServer.enqueueEmptyResponse();
            }
            assertBusy(() -> assertTrue(doesAuthPollingTaskExist()));
            // Storing the endpoint with EndpointMetadata fails in mixed cluster (mapping exception), so no EIS endpoints
            assertNoEisEndpoints();
        }

        if (isFullyUpgraded) {
            logger.info("Fully upgraded - mock returns preconfigured endpoint with request callback; endpoint should be persisted");
            mockEISServer.enqueuePreconfiguredEndpointResponseWithRequestReceivedCallback(authRequestReceivedInUpgradedCluster);
            assertBusy(() -> assertTrue("Expected auth request to be received by mock server", authRequestReceivedInUpgradedCluster.get()));
            assertBusy(() -> {
                var endpoint = getEisEndpointViaRest(ELSER_V2_ENDPOINT_ID);
                assertNotNull("Expected EIS endpoint " + ELSER_V2_ENDPOINT_ID + " to be persisted", endpoint);
                assertThat(endpoint.get(INFERENCE_ID_FIELD_NAME), is(ELSER_V2_ENDPOINT_ID));
                assertThat(endpoint.get(TaskType.NAME), is(TaskType.SPARSE_EMBEDDING.toString()));
            });
        }
    }

    private static void assertNoEisEndpoints() throws IOException {
        var eisEndpoints = getEisEndpointsViaRest();
        assertThat("Expected no EIS preconfigured endpoints in mixed cluster (store fails with mapping exception)", eisEndpoints, empty());
    }

    private static List<Map<String, Object>> getEisEndpointsViaRest() throws IOException {
        var endpoints = getAllModels();
        if (endpoints == null) {
            return List.of();
        }
        return endpoints.stream().filter(e -> ElasticInferenceService.NAME.equals(e.get("service"))).toList();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getEisEndpointViaRest(String endpointId) throws IOException {
        var request = new Request(GET_METHOD, Strings.format("_inference/%s?error_trace", endpointId));
        var response = client().performRequest(request);
        assertStatusOkOrCreated(response);
        var responseAsMap = entityAsMap(response);
        var endpoints = (List<Map<String, Object>>) responseAsMap.get("endpoints");
        if (endpoints == null || endpoints.isEmpty()) {
            return null;
        }
        return endpoints.get(0);
    }
}
