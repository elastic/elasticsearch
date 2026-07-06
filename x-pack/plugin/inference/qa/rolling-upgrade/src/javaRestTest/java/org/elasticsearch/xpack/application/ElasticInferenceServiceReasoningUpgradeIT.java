/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.UnifiedCompletionUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.upgrades.ParameterizedRollingUpgradeTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.MockElasticInferenceServiceAuthorizationServer;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Rolling-upgrade test for the {@code reasoning} task setting on the Elastic Inference Service
 * {@code chat_completion} endpoint.
 *
 * <p>The test is gated by the cluster feature {@link InferenceFeatures#INFERENCE_ELASTIC_REASONING_TASK_SETTINGS}:
 * when the old cluster does not yet have that feature, the test verifies that:
 * <ul>
 *   <li>In old/mixed phases, a PUT with {@code task_settings.reasoning} is rejected (HTTP 400): a
 *       current-node master rejects via {@code EnforcingEmptyTaskSettings} / {@code EnforceEmptyTaskSettingsStrategy}
 *       (since {@code CompletionCompatibilityService#getTaskSettingsStrategy} returns that strategy while the
 *       feature is not yet cluster-wide); an old-node master never parses {@code reasoning} into a settings
 *       object at all, so the field survives into the generic leftover-map check and is rejected there
 *       instead.</li>
 *   <li>Once the cluster is fully upgraded, the PUT is accepted, and {@code task_settings.reasoning}
 *       round-trips correctly on a subsequent GET.</li>
 * </ul>
 *
 * <p>The 3-node cluster is wired to {@link MockElasticInferenceServiceAuthorizationServer} for its URL
 * (so the EIS URL setting resolves to a bound address before any node starts) but with EIS authorization
 * fully disabled. In the upgraded phase, {@code xpack.inference.skip_validate_and_start} is applied as a
 * transient cluster setting (all nodes support it once fully upgraded) so the PUT makes no outgoing call
 * to the mock.
 *
 * <p>The whole class is skipped (before the cluster is ever started) unless the old cluster version is
 * {@code >= 9.3.1}, since the {@link ElasticInferenceServiceSettings#AUTHORIZATION_ENABLED} node setting
 * applied below was only added in 9.3.0 and older nodes reject it as unknown at startup.
 */
public class ElasticInferenceServiceReasoningUpgradeIT extends ParameterizedRollingUpgradeTestCase {

    private static final String SERVICE = "elastic";
    private static final String MODEL_ID = "eis-upgrade-test-model";
    private static final String EFFORT_MEDIUM = "medium";
    private static final String SUMMARY_DETAILED = "detailed";
    // AUTHORIZATION_ENABLED setting was added in 9.3.0
    private static final Version AUTHORIZATION_ENABLED_MIN_VERSION = Version.fromString("9.3.1");

    // URL provider only — no init() / enqueueAuthorizeAllModelsResponse() calls because
    // AUTHORIZATION_ENABLED=false suppresses all bootup auth traffic.
    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    // The AUTHORIZATION_ENABLED setting applied below is unknown to nodes older than 9.3.1, which would
    // otherwise fail settings validation and die at startup. Skip the whole class before the cluster
    // (and mock server) rules ever run when the old cluster predates that setting.
    private static final RunnableTestRuleAdapter versionLimit = new RunnableTestRuleAdapter(
        () -> assumeTrue(
            "Only run when upgrading from a version that has the AUTHORIZATION_ENABLED setting",
            Version.tryParse(getOldClusterVersion()).map(v -> v.onOrAfter(AUTHORIZATION_ENABLED_MIN_VERSION)).orElse(false)
        )
    );

    private static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .version(getOldClusterVersion(), isOldClusterDetachedVersion())
        .nodes(NODE_NUM)
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
        .setting(ElasticInferenceServiceSettings.AUTHORIZATION_ENABLED.getKey(), "false")
        .setting(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        .build();

    // The mock server must start before the cluster so its URL is bound when the cluster reads the setting.
    // versionLimit is outermost so the version assumption is checked before either the mock server or the
    // cluster is started.
    @ClassRule
    public static TestRule ruleChain = RuleChain.outerRule(versionLimit).around(mockEISServer).around(cluster);

    public ElasticInferenceServiceReasoningUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @Override
    protected ElasticsearchCluster getUpgradeCluster() {
        return cluster;
    }

    /**
     * Asserts that a {@code chat_completion} endpoint with {@code task_settings.reasoning} is:
     * <ul>
     *   <li>rejected (HTTP 400) in old/mixed-cluster phases, and</li>
     *   <li>accepted with a correct round-trip GET once the cluster is fully upgraded.</li>
     * </ul>
     *
     * <p>The test is skipped when the old cluster already has the
     * {@link InferenceFeatures#INFERENCE_ELASTIC_REASONING_TASK_SETTINGS} feature, since the gate would
     * never fire.
     */
    @SuppressWarnings("unchecked")
    public void testReasoningChatCompletionEndpoint_GatedUntilFullyUpgraded() throws Exception {
        assumeTrue(
            "Old cluster already supports reasoning task settings; skipping gate test",
            oldClusterHasFeature(InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS) == false
        );

        var inferenceId = "test-reasoning-gated";
        var config = chatCompletionConfig(EFFORT_MEDIUM, SUMMARY_DETAILED);

        if (isUpgradedCluster()) {
            // All nodes are upgraded and support the reasoning feature. Enable skip_validate_and_start as a
            // transient cluster setting so the PUT makes no outgoing call to the mock EIS server.
            updateClusterSettings(Settings.builder().put("xpack.inference.skip_validate_and_start", true).build());
            try {
                var putRequest = buildPutRequest(inferenceId, config, TaskType.CHAT_COMPLETION);
                assertStatusOkOrCreated(client().performRequest(putRequest));

                var getResponse = entityAsMap(client().performRequest(new Request("GET", "_inference/" + inferenceId)));
                var endpoints = (List<Map<String, Object>>) getResponse.get("endpoints");
                var taskSettings = (Map<String, Object>) endpoints.get(0).get(ModelConfigurations.TASK_SETTINGS);
                var reasoning = (Map<String, Object>) taskSettings.get(UnifiedCompletionUtils.REASONING_FIELD);
                assertThat(reasoning.get(UnifiedCompletionUtils.EFFORT_FIELD), is(EFFORT_MEDIUM));
                assertThat(reasoning.get(UnifiedCompletionUtils.SUMMARY_FIELD), is(SUMMARY_DETAILED));
            } finally {
                resetEndpoint(inferenceId);
            }
        } else {
            // Old or mixed cluster: a new-node master rejects via EnforceEmptyTaskSettingsStrategy /
            // EnforcingEmptyTaskSettings (unknown-settings-in-task_settings message); an old-node master
            // never parses reasoning into a settings object at all, so it survives into the generic
            // leftover-map check and is rejected as unknown to the service instead. Both produce a 400;
            // the exact message depends on which node is master, hence the broad anyOf.
            var putRequest = buildPutRequest(inferenceId, config, TaskType.CHAT_COMPLETION);
            var e = expectThrows(ResponseException.class, () -> client().performRequest(putRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(
                e.getMessage(),
                anyOf(containsString("unknown to the [elastic] service"), containsString("Configuration contains unknown settings"))
            );
        }
    }

    private static String chatCompletionConfig(String effort, String summary) {
        return Strings.format("""
            {
              "service": "%s",
              "service_settings": { "model_id": "%s" },
              "task_settings": { "reasoning": { "effort": "%s", "summary": "%s" } }
            }
            """, SERVICE, MODEL_ID, effort, summary);
    }

    private static Request buildPutRequest(String inferenceId, String config, TaskType taskType) throws IOException {
        var request = new Request("PUT", Strings.format("_inference/%s/%s", taskType, inferenceId));
        request.setJsonEntity(config);
        return request;
    }

    private void resetEndpoint(String inferenceId) throws IOException {
        try {
            client().performRequest(new Request("DELETE", "_inference/" + inferenceId));
        } finally {
            updateClusterSettings(Settings.builder().putNull("xpack.inference.skip_validate_and_start").build());
        }
    }
}
