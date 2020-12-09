/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OperatorPrivilegesIT extends ESRestTestCase {

    private static final String OPERATOR_AUTH_HEADER = "Basic "
        + Base64.getEncoder().encodeToString("test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testNonOperatorSuperuserWillFailToCallOperatorOnlyApiWhenOperatorPrivilegesIsEnabled() throws IOException {
        final Request postVotingConfigExclusionsRequest = new Request("POST", "_cluster/voting_config_exclusions?node_names=foo");
        final ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(postVotingConfigExclusionsRequest)
        );
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(responseException.getMessage(), containsString("Operator privileges are required for action"));
    }

    public void testOperatorUserWillSucceedToCallOperatorOnlyApi() throws IOException {
        final Request postVotingConfigExclusionsRequest = new Request("POST", "_cluster/voting_config_exclusions?node_names=foo");
        postVotingConfigExclusionsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        client().performRequest(postVotingConfigExclusionsRequest);
    }

    public void testOperatorUserWillFailToCallOperatorOnlyApiIfRbacFails() throws IOException {
        final Request deleteVotingConfigExclusionsRequest = new Request("DELETE", "_cluster/voting_config_exclusions");
        deleteVotingConfigExclusionsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        final ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(deleteVotingConfigExclusionsRequest)
        );
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(responseException.getMessage(), containsString("is unauthorized for user"));
    }

    public void testOperatorUserCanCallNonOperatorOnlyApi() throws IOException {
        final Request mainRequest = new Request("GET", "/");
        mainRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        client().performRequest(mainRequest);
    }

    @SuppressWarnings("unchecked")
    public void testEveryActionIsEitherOperatorOnlyOrNonOperator() throws IOException {
        Set<String> doubleLabelled = Sets.intersection(Constants.NON_OPERATOR_ACTIONS, OperatorOnlyRegistry.SIMPLE_ACTIONS);
        assertTrue("Actions are both operator-only and non-operator: " + doubleLabelled, doubleLabelled.isEmpty());

        final Request request = new Request("GET", "/_test/get_actions");
        final Map<String, Object> response = responseAsMap(client().performRequest(request));
        Set<String> allActions = Set.copyOf((List<String>) response.get("actions"));
        final HashSet<String> labelledActions = new HashSet<>(OperatorOnlyRegistry.SIMPLE_ACTIONS);
        labelledActions.addAll(Constants.NON_OPERATOR_ACTIONS);

        final Set<String> unlabelled = Sets.difference(allActions, labelledActions);
        assertTrue("Actions are neither operator-only nor non-operator: " + unlabelled, unlabelled.isEmpty());

        final Set<String> redundant = Sets.difference(labelledActions, allActions);
        assertTrue("Actions may no longer be valid: " + redundant, redundant.isEmpty());
    }

    @SuppressWarnings("unchecked")
    public void testOperatorPrivilegesXpackUsage() throws IOException {
        final Request xpackRequest = new Request("GET", "/_xpack/usage");
        final Map<String, Object> response = entityAsMap(client().performRequest(xpackRequest));
        final Map<String, Object> features = (Map<String, Object>) response.get("security");
        final Map<String, Object> operatorPrivileges = (Map<String, Object>) features.get("operator_privileges");
        assertTrue((boolean) operatorPrivileges.get("available"));
        assertTrue((boolean) operatorPrivileges.get("enabled"));
    }

    // Make sure autoscaling polices are not restored by an operator user
    public void testAutoscalingPolicyWillNotBeRestored() throws IOException {
        final String repoName = "repo";
        final String snapshotName = "snap";
        final String policyName = "policy";
        createSnapshotRepo(repoName);
        createAutoscalingPolicy(policyName);
        takeSnapshot(repoName, snapshotName);
        deleteAutoscalingPolicy(policyName);
        restoreSnapshot(repoName, snapshotName);

        final Request getPolicyRequest = new Request("GET", "/_autoscaling/policy/" + policyName);
        getPolicyRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getPolicyRequest));
        assertThat(e.getMessage(), containsString("autoscaling policy with name [" + policyName + "] does not exist"));
    }

    private void createSnapshotRepo(String repoName) throws IOException {
        Request request = new Request("PUT", "/_snapshot/" + repoName);
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .field("type", "fs")
                    .startObject("settings")
                    .field("location", System.getProperty("tests.path.repo"))
                    .endObject()
                    .endObject()
            )
        );
        assertOK(client().performRequest(request));
    }

    private void createAutoscalingPolicy(String policyName) throws IOException {
        final Request request = new Request("PUT", "/_autoscaling/policy/" + policyName);
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder()
                    .startObject()
                    .array("roles", "master")
                    .startObject("deciders")
                    .startObject("fixed")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        assertOK(client().performRequest(request));
    }

    private void takeSnapshot(String repoName, String snapshotName) throws IOException {
        final Request request = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName);
        if (randomBoolean()) {
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        }
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().startObject().field("include_global_state", true).endObject())
        );
        assertOK(client().performRequest(request));
    }

    private void deleteAutoscalingPolicy(String policyName) throws IOException {
        final Request request = new Request("DELETE", "/_autoscaling/policy/" + policyName);
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        assertOK(client().performRequest(request));
    }

    private void restoreSnapshot(String repoName, String snapshotName) throws IOException {
        final Request request = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        if (randomBoolean()) {
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        }
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().startObject().field("include_global_state", true).endObject())
        );
        assertOK(client().performRequest(request));
    }

}
