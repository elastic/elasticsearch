/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.qa.mixed;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.inference.ModelConfigurations;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.inference.completion.UnifiedCompletionUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.inference.InferenceFeatures;
import org.elasticsearch.xpack.inference.MockElasticInferenceServiceAuthorizationServer;
import org.elasticsearch.xpack.inference.services.elastic.ElasticInferenceServiceSettings;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.inference.ElasticInferenceServiceCompletionTaskSettingsIT.chatCompletionConfig;
import static org.elasticsearch.xpack.inference.ElasticInferenceServiceCompletionTaskSettingsIT.configWithoutReasoning;
import static org.elasticsearch.xpack.inference.ElasticInferenceServiceCompletionTaskSettingsIT.reasoningTaskSettingsUpdate;
import static org.elasticsearch.xpack.inference.InferenceBaseRestTest.assertStatusOkOrCreated;
import static org.elasticsearch.xpack.inference.services.elastic.ccm.CCMSettings.CCM_SUPPORTED_ENVIRONMENT;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Mixed-cluster tests for the {@code reasoning} task setting on the Elastic Inference Service
 * {@code chat_completion} and {@code completion} endpoints.
 *
 * <p>Unlike {@code ElasticInferenceServiceReasoningUpgradeIT} (which walks through old, mixed, and fully
 * upgraded phases of a rolling upgrade), this suite pins a fixed old-node + current-node cluster, mirroring
 * every other test in this package. Because the {@code reasoning} feature is brand new, such a cluster is
 * permanently "mixed" with respect to it, so PUT requests carrying {@code task_settings.reasoning} are always
 * rejected, while requests with empty task settings always succeed on either task type.
 *
 * <p>PUT and {@code _update} are both master-node actions, so which node is elected master determines the
 * exact rejection path — as with every other mixed-cluster suite in this repo, master election here is not
 * pinned to either node (no suite does this; a node's version and its master eligibility are independent
 * things bootstrap does not let you decouple for a fresh old+new cluster). For PUT, a current-node master
 * rejects via {@code EnforcingEmptyTaskSettings} / {@code EnforceEmptyTaskSettingsStrategy} (since the current
 * {@code ElasticInferenceService} uses a task-settings parser); an old-node master never parses
 * {@code reasoning} into a settings object at all, so the field survives into the generic leftover-map check
 * and is rejected there instead — a different message, same 400 outcome.
 *
 * <p>For {@code _update} the two masters diverge further because the leftover-map check on the update path
 * ({@code validateConsumedUpdateSettings}) postdates 9.4.x. A current-node master still rejects during the
 * settings merge, via {@code EnforcingEmptyTaskSettings#updatedTaskSettings}. An old (9.4.x) node master has
 * no such check and {@code EmptyTaskSettings#updatedTaskSettings} does not inspect its input map, so
 * {@code reasoning} is silently swallowed rather than rejected; the update then proceeds to the mandatory
 * external service-validation call that {@code _update} always makes (this does not honor
 * {@code xpack.inference.skip_validate_and_start}, unlike PUT), which fails against the URL-only mock with no
 * queued response. Either way {@code reasoning} must never end up persisted, so the update tests below assert
 * that invariant, plus the closed set of 400 messages each master can actually produce, instead of a single
 * fixed message.
 *
 * <p>The 2-node cluster is wired to {@link MockElasticInferenceServiceAuthorizationServer} for its URL (so
 * the EIS URL setting resolves to a bound address before any node starts) but with EIS authorization fully
 * disabled, and {@code xpack.inference.skip_validate_and_start=true} so PUTs make no outgoing call to the
 * mock. The mock is never given a queued response, since PUT rejects before any outgoing call would be made
 * and the update scenarios only need to observe that the call fails, not what it returns.
 *
 * <p>The whole class is skipped (before the cluster is ever started) unless the old cluster version is
 * {@code >= 9.3.1}, since the {@link ElasticInferenceServiceSettings#AUTHORIZATION_ENABLED} node setting
 * applied below was only added in 9.3.0 and older nodes reject it as unknown at startup.
 */
public class ElasticInferenceServiceReasoningMixedIT extends ESRestTestCase {

    private static final String EFFORT_MEDIUM = "medium";
    private static final String SUMMARY_DETAILED = "detailed";
    // Thrown by EnforcingEmptyTaskSettings when a current-node master parses task_settings.
    private static final String UNKNOWN_REASONING_SETTING_MESSAGE = Strings.format(
        "[%s] Configuration contains unknown settings [%s]",
        ModelConfigurations.TASK_SETTINGS,
        UnifiedCompletionUtils.REASONING_FIELD
    );
    // Thrown by ServiceUtils#unknownSettingsError when a leftover-map check rejects reasoning because it was
    // never consumed by a task-settings parser (e.g. an old-node master on PUT).
    private static final String UNKNOWN_TO_SERVICE_MESSAGE = "unknown to the [elastic] service";
    // Thrown by ServiceIntegrationValidator when the mandatory update-time service-validation call fails; this
    // is what an old (9.4.x) node master produces, since it silently drops reasoning instead of rejecting it
    // and then falls through to that call, which the URL-only mock cannot answer.
    private static final String VALIDATION_CALL_FAILED_MESSAGE = "validation call to service threw an exception";
    // AUTHORIZATION_ENABLED setting was added in 9.3.0
    private static final Version AUTHORIZATION_ENABLED_MIN_VERSION = Version.fromString("9.3.1");

    private static final String OLD_CLUSTER_VERSION_STRING = System.getProperty("tests.old_cluster_version");
    private static final boolean IS_OLD_CLUSTER_DETACHED_VERSION = System.getProperty("tests.bwc.refspec.main") != null;
    private static final Version OLD_CLUSTER_VERSION = Version.fromString(OLD_CLUSTER_VERSION_STRING);

    // URL provider only — no init() / enqueueAuthorizeAllModelsResponse() calls because
    // AUTHORIZATION_ENABLED=false suppresses all bootup auth traffic.
    private static final MockElasticInferenceServiceAuthorizationServer mockEISServer =
        new MockElasticInferenceServiceAuthorizationServer();

    // The AUTHORIZATION_ENABLED setting applied below is unknown to nodes older than 9.3.1, which would
    // otherwise fail settings validation and die at startup. Skip the whole class before the cluster
    // (and mock server) rules ever run when the old cluster predates that setting.
    private static final RunnableTestRuleAdapter versionLimit = new RunnableTestRuleAdapter(
        () -> assumeTrue(
            "Only run when the old cluster has the AUTHORIZATION_ENABLED setting",
            OLD_CLUSTER_VERSION.onOrAfter(AUTHORIZATION_ENABLED_MIN_VERSION)
        )
    );

    private static final ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .withNode(node -> node.version(OLD_CLUSTER_VERSION_STRING, IS_OLD_CLUSTER_DETACHED_VERSION))
        .withNode(node -> node.version(Version.CURRENT))
        .setting("xpack.security.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting(CCM_SUPPORTED_ENVIRONMENT.getKey(), "false")
        .setting(ElasticInferenceServiceSettings.ELASTIC_INFERENCE_SERVICE_URL.getKey(), mockEISServer::getUrl)
        .setting(ElasticInferenceServiceSettings.AUTHORIZATION_ENABLED.getKey(), "false")
        .setting(ElasticInferenceServiceSettings.PERIODIC_AUTHORIZATION_ENABLED.getKey(), "false")
        .setting("xpack.inference.skip_validate_and_start", "true")
        .build();

    // The mock server must start before the cluster so its URL is bound when the cluster reads the setting.
    // versionLimit is outermost so the version assumption is checked before either the mock server or the
    // cluster is started.
    @ClassRule
    public static final TestRule ruleChain = RuleChain.outerRule(versionLimit).around(mockEISServer).around(cluster);

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testPutChatCompletionWithReasoning_Fails() throws IOException {
        assumeReasoningNotSupportedClusterWide();

        var config = chatCompletionConfig(EFFORT_MEDIUM, SUMMARY_DETAILED);
        var request = buildPutRequest("test-put-chat-completion-reasoning-mixed", config, TaskType.CHAT_COMPLETION);
        assertReasoningRejected(expectThrows(ResponseException.class, () -> client().performRequest(request)));
    }

    public void testPutCompletionWithReasoning_Fails() throws IOException {
        assumeReasoningNotSupportedClusterWide();

        var config = chatCompletionConfig(EFFORT_MEDIUM, SUMMARY_DETAILED);
        var request = buildPutRequest("test-put-completion-reasoning-mixed", config, TaskType.COMPLETION);
        assertReasoningRejected(expectThrows(ResponseException.class, () -> client().performRequest(request)));
    }

    public void testPutChatCompletionWithoutTaskSettings_Succeeds() throws IOException {
        var inferenceId = "test-put-chat-completion-empty-mixed";
        try {
            assertStatusOkOrCreated(
                client().performRequest(buildPutRequest(inferenceId, configWithoutReasoning(), TaskType.CHAT_COMPLETION))
            );
            assertNoTaskSettings(inferenceId);
        } finally {
            deleteModel(inferenceId);
        }
    }

    public void testPutCompletionWithoutTaskSettings_Succeeds() throws IOException {
        var inferenceId = "test-put-completion-empty-mixed";
        try {
            assertStatusOkOrCreated(client().performRequest(buildPutRequest(inferenceId, configWithoutReasoning(), TaskType.COMPLETION)));
            assertNoTaskSettings(inferenceId);
        } finally {
            deleteModel(inferenceId);
        }
    }

    public void testUpdateChatCompletionWithReasoning_NeverApplied() throws IOException {
        assumeReasoningNotSupportedClusterWide();

        var inferenceId = "test-update-chat-completion-reasoning-mixed";
        try {
            assertStatusOkOrCreated(
                client().performRequest(buildPutRequest(inferenceId, configWithoutReasoning(), TaskType.CHAT_COMPLETION))
            );

            var updateRequest = buildUpdateRequest(inferenceId, reasoningTaskSettingsUpdate(EFFORT_MEDIUM), TaskType.CHAT_COMPLETION);
            assertReasoningRejectedOrNeverApplied(inferenceId, updateRequest);
        } finally {
            deleteModel(inferenceId);
        }
    }

    public void testUpdateCompletionWithReasoning_NeverApplied() throws IOException {
        assumeReasoningNotSupportedClusterWide();

        var inferenceId = "test-update-completion-reasoning-mixed";
        try {
            assertStatusOkOrCreated(client().performRequest(buildPutRequest(inferenceId, configWithoutReasoning(), TaskType.COMPLETION)));

            var updateRequest = buildUpdateRequest(inferenceId, reasoningTaskSettingsUpdate(EFFORT_MEDIUM), TaskType.COMPLETION);
            assertReasoningRejectedOrNeverApplied(inferenceId, updateRequest);
        } finally {
            deleteModel(inferenceId);
        }
    }

    /**
     * Skips the calling test if the cluster fully supports the reasoning feature (e.g. a future BWC version
     * ships it on both nodes), since the rejection this suite asserts would no longer fire.
     */
    private static void assumeReasoningNotSupportedClusterWide() {
        assumeFalse(
            "Cluster fully supports reasoning task settings; the gate would not fire",
            clusterHasFeature(InferenceFeatures.INFERENCE_ELASTIC_REASONING_TASK_SETTINGS)
        );
    }

    /**
     * Asserts a 400 response rejecting the {@code reasoning} field. Which message appears depends on which
     * node is master for this master-node action: a current-node master rejects via
     * {@code EnforcingEmptyTaskSettings} / {@code EnforceEmptyTaskSettingsStrategy} (scoped to
     * {@code task_settings}); an old-node master never parses {@code reasoning} into a settings object, so it
     * survives into the generic leftover-map check and is rejected as an unknown top-level setting instead.
     */
    private static void assertReasoningRejected(ResponseException e) {
        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
        assertThat(
            e.getMessage(),
            anyOf(
                // current-node master: EnforcingEmptyTaskSettings (usesParserForTaskSettings() == true)
                containsString(UNKNOWN_REASONING_SETTING_MESSAGE),
                // old-node master: reasoning survives into SenderService's leftover-map check
                containsString(UNKNOWN_TO_SERVICE_MESSAGE)
            )
        );
    }

    /**
     * Asserts that {@code _update} never actually applies {@code task_settings.reasoning} while the cluster is
     * not fully upgraded, regardless of which node happens to be master for this master-node action:
     * <ul>
     *   <li>a current-node master rejects the request outright (400) during the settings merge, via
     *       {@code EnforcingEmptyTaskSettings#updatedTaskSettings};</li>
     *   <li>an old (9.4.x) node master predates the update-path leftover-map check
     *       ({@code validateConsumedUpdateSettings}) and {@code EmptyTaskSettings#updatedTaskSettings} does
     *       not inspect its input map, so {@code reasoning} is silently swallowed rather than rejected; the
     *       update then proceeds to the mandatory external service-validation call that {@code _update}
     *       always makes (unlike PUT, this ignores {@code xpack.inference.skip_validate_and_start}), which
     *       fails (400) against the URL-only mock with no queued response.</li>
     * </ul>
     * Either way {@code reasoning} must never end up persisted, so this asserts that invariant unconditionally,
     * plus the closed set of 400 messages each master can actually produce.
     */
    private static void assertReasoningRejectedOrNeverApplied(String inferenceId, Request updateRequest) throws IOException {
        try {
            client().performRequest(updateRequest);
        } catch (ResponseException e) {
            assertThat(e.getResponse().getStatusLine().getStatusCode(), is(400));
            assertThat(
                e.getMessage(),
                anyOf(
                    // current-node master: EnforcingEmptyTaskSettings rejects reasoning during the merge
                    containsString(UNKNOWN_REASONING_SETTING_MESSAGE),
                    // old-node master that rejects unknown update settings via a leftover-map check
                    // (unreachable on 9.4.x, which predates that check; kept for future old-cluster versions)
                    containsString(UNKNOWN_TO_SERVICE_MESSAGE),
                    // old (9.4.x) node master: reasoning is dropped, then the mandatory update-validation
                    // call fails against the URL-only mock
                    containsString(VALIDATION_CALL_FAILED_MESSAGE)
                )
            );
        }
        // In every case, reasoning must never be persisted.
        assertNoTaskSettings(inferenceId);
    }

    @SuppressWarnings("unchecked")
    private static void assertNoTaskSettings(String inferenceId) throws IOException {
        var response = entityAsMap(client().performRequest(new Request("GET", "_inference/" + inferenceId)));
        var endpoints = (List<Map<String, Object>>) response.get("endpoints");
        assertNull(endpoints.get(0).get(ModelConfigurations.TASK_SETTINGS));
    }

    private static Request buildPutRequest(String inferenceId, String config, TaskType taskType) {
        var request = new Request("PUT", Strings.format("_inference/%s/%s", taskType, inferenceId));
        request.setJsonEntity(config);
        return request;
    }

    private static Request buildUpdateRequest(String inferenceId, String config, TaskType taskType) {
        var request = new Request("PUT", Strings.format("_inference/%s/%s/_update", taskType, inferenceId));
        request.setJsonEntity(config);
        return request;
    }

    private static void deleteModel(String inferenceId) throws IOException {
        assertStatusOkOrCreated(client().performRequest(new Request("DELETE", "_inference/" + inferenceId)));
    }
}
