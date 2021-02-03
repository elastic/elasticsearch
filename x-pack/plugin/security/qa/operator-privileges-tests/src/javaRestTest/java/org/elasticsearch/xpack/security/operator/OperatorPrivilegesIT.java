/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
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
        final String authHeader = "Basic "
            + Base64.getEncoder().encodeToString("test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        postVotingConfigExclusionsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        client().performRequest(postVotingConfigExclusionsRequest);
    }

    public void testOperatorUserWillFailToCallOperatorOnlyApiIfRbacFails() throws IOException {
        final Request deleteVotingConfigExclusionsRequest = new Request("DELETE", "_cluster/voting_config_exclusions");
        final String authHeader = "Basic "
            + Base64.getEncoder().encodeToString("test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        deleteVotingConfigExclusionsRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        final ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(deleteVotingConfigExclusionsRequest)
        );
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(responseException.getMessage(), containsString("is unauthorized for user"));
    }

    public void testOperatorUserCanCallNonOperatorOnlyApi() throws IOException {
        final Request mainRequest = new Request("GET", "/");
        final String authHeader = "Basic "
            + Base64.getEncoder().encodeToString("test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        mainRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
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
}
