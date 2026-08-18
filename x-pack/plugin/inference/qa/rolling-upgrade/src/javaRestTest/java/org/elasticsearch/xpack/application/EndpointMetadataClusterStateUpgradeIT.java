/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.ParameterizedRollingUpgradeTestCase;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.MockElasticInferenceServiceAuthorizationServer;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.CCMRestBaseIT.ENABLE_CCM_REQUEST;
import static org.elasticsearch.xpack.inference.CCMRestBaseIT.putCCMConfiguration;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL;
import static org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Rolling-upgrade test verifying that cluster state stores only the {@code heuristics} and {@code internal}
 * subset of {@code EndpointMetadata} after upgrading to 9.6+.
 *
 * <p>The test is gated on the old cluster supporting both endpoint metadata
 * ({@link InferenceFeatures#ENDPOINT_METADATA_FIELD}) and cloud-connected-mode
 * ({@link InferenceFeatures#INFERENCE_CCM_ENABLEMENT_SERVICE}).
 *
 * <ul>
 *   <li><b>Old cluster:</b> CCM is enabled and a mock EIS auth response populates endpoints in cluster state.</li>
 *   <li><b>Mixed cluster:</b> Endpoints remain accessible; cluster state is still parseable by both old and new nodes.</li>
 *   <li><b>Upgraded cluster:</b> Cluster state must not contain {@code display}, {@code regions}, or
 *       {@code denied_by_region_policy} in the {@code metadata} field, while
 *       {@code GET _inference/<id>} still returns the full metadata including {@code display}.</li>
 * </ul>
 */
public class EndpointMetadataClusterStateUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    static {
        // Queue one response per node to cover any auth requests made at startup (e.g. when an old node's
        // auth task runs before PERIODIC_AUTHORIZATION_ENABLED=false takes effect).
        mockEISServer.init(NODE_NUM);
    }

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterVersion(), isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
        // Allow the first auth call (triggered by CCM enable) but prevent background periodic polling
        // so the mock server queue is not exhausted by repeated requests.
        .setting(PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        .build();

    // Mock server must start before the cluster so its bound address is available for the setting.
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(mockEISServer).around(cluster);

    public EndpointMetadataClusterStateUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    /**
     * Verifies the cluster-state metadata subset invariant across a rolling upgrade.
     */
    @SuppressWarnings("unchecked")
    public void testClusterStateContainsOnlyMetadataSubsetAfterUpgrade() throws Exception {
        assumeTrue(
            "Old cluster must support endpoint metadata and CCM enablement service",
            oldClusterHasFeature(InferenceFeatures.ENDPOINT_METADATA_FIELD)
                && oldClusterHasFeature(InferenceFeatures.INFERENCE_CCM_ENABLEMENT_SERVICE)
        );

        if (isOldCluster()) {
            // Enqueue one auth response to service the initial auth call triggered by CCM enable.
            mockEISServer.enqueueAuthorizeAllModelsResponse();
            putCCMConfiguration(ENABLE_CCM_REQUEST);
            assertBusy(() -> {
                var models = getMinimalConfigs();
                assertNotNull("Expected EIS endpoints to appear in cluster state after CCM enable", models);
                assertFalse("Expected at least one EIS endpoint in cluster state after CCM enable", models.isEmpty());
            });
            // Verify that old-cluster nodes write the full EndpointMetadata (including display) to cluster state,
            // so that the post-upgrade assertion that display is absent is meaningful.
            var oldModels = getMinimalConfigs();
            assertTrue(
                "At least one EIS endpoint must have display in cluster state on the old cluster",
                oldModels.values().stream().anyMatch(endpoint -> {
                    var metadata = (Map<String, Object>) XContentMapValues.extractValue("metadata", endpoint);
                    return metadata != null && metadata.containsKey("display");
                })
            );
        }

        if (isMixedCluster() || isUpgradedCluster()) {
            // Endpoints must persist in cluster state across the upgrade.
            var models = getMinimalConfigs();
            assertNotNull("EIS endpoints must remain in cluster state during and after upgrade", models);
            assertFalse("EIS endpoints must remain in cluster state during and after upgrade", models.isEmpty());

            if (isUpgradedCluster()) {
                // After full upgrade, cluster state must store only heuristics+internal — never display/regions/denied_by_region_policy.
                var checkedCount = 0;
                for (var entry : models.entrySet()) {
                    var endpointId = entry.getKey();
                    var metadata = (Map<String, Object>) XContentMapValues.extractValue("metadata", entry.getValue());
                    if (metadata == null) {
                        continue;
                    }
                    checkedCount++;

                    assertTrue(
                        "Cluster state metadata for [" + endpointId + "] must contain heuristics",
                        metadata.containsKey("heuristics")
                    );
                    assertTrue("Cluster state metadata for [" + endpointId + "] must contain internal", metadata.containsKey("internal"));
                    assertFalse(
                        "display must not be stored in cluster state for endpoint [" + endpointId + "]",
                        metadata.containsKey("display")
                    );
                    assertFalse(
                        "regions must not be stored in cluster state for endpoint [" + endpointId + "]",
                        metadata.containsKey("regions")
                    );
                    assertFalse(
                        "denied_by_region_policy must not be stored in cluster state for endpoint [" + endpointId + "]",
                        metadata.containsKey("denied_by_region_policy")
                    );

                    // The full metadata — including display — must still be retrievable from the .inference system index.
                    var getResponse = entityAsMap(client().performRequest(new Request("GET", "_inference/" + endpointId)));
                    var getEndpoints = (List<Map<String, Object>>) getResponse.get("endpoints");
                    assertNotNull("GET _inference must return an endpoints array for [" + endpointId + "]", getEndpoints);
                    assertFalse("GET _inference must return at least one endpoint for [" + endpointId + "]", getEndpoints.isEmpty());
                    var getMetadata = (Map<String, Object>) getEndpoints.get(0).get("metadata");
                    assertNotNull("GET _inference must return metadata for endpoint [" + endpointId + "]", getMetadata);
                    assertTrue("GET _inference must return display for endpoint [" + endpointId + "]", getMetadata.containsKey("display"));
                }
                assertThat("At least one upgraded endpoint must have been fully verified", checkedCount, greaterThan(0));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Map<String, Object>> getMinimalConfigs() throws IOException {
        var request = new Request("GET", "_cluster/state?filter_path=metadata.model_registry");
        var response = client().performRequest(request);
        assertOK(response);
        return (Map<String, Map<String, Object>>) XContentMapValues.extractValue("metadata.model_registry.models", entityAsMap(response));
    }
}
