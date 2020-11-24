/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OperatorPrivilegesIT extends ESRestTestCase {

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("test_admin", new SecureString("x-pack-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @SuppressWarnings("unchecked")
    public void testNonOperatorSuperuserWillFailToCallOperatorOnlyApiWhenOperatorPrivilegesIsEnabled() throws IOException {
        final Request getClusterSettingsRequest = new Request("GET",
            "_cluster/settings?flat_settings&include_defaults&filter_path=defaults.*operator_privileges*");
        final Map<String, Object> settingsMap = entityAsMap(client().performRequest(getClusterSettingsRequest));
        final Map<String, Object> defaults = (Map<String, Object>) settingsMap.get("defaults");
        final Object isOperatorPrivilegesEnabled = defaults.get("xpack.security.operator_privileges.enabled");

        final Request postVotingConfigExclusionsRequest = new Request(
            "POST", "_cluster/voting_config_exclusions?node_names=foo");
        if ("true".equals(isOperatorPrivilegesEnabled)) {
            final ResponseException responseException = expectThrows(
                ResponseException.class,
                () -> client().performRequest(postVotingConfigExclusionsRequest));
            assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(responseException.getMessage(), containsString("Operator privileges are required for action"));
        } else {
            client().performRequest(postVotingConfigExclusionsRequest);
        }
    }

    public void testOperatorUserWillSucceedToCallOperatorOnlyApi() throws IOException {
        final Request postVotingConfigExclusionsRequest = new Request(
            "POST", "_cluster/voting_config_exclusions?node_names=foo");
        final String authHeader = "Basic " + Base64.getEncoder().encodeToString(
            "test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        postVotingConfigExclusionsRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        client().performRequest(postVotingConfigExclusionsRequest);
    }

    public void testOperatorUserWillFailToCallOperatorOnlyApiIfRbacFails() throws IOException {
        final Request deleteVotingConfigExclusionsRequest = new Request(
            "DELETE", "_cluster/voting_config_exclusions");
        final String authHeader = "Basic " + Base64.getEncoder().encodeToString(
            "test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        deleteVotingConfigExclusionsRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        final ResponseException responseException = expectThrows(ResponseException.class,
            () -> client().performRequest(deleteVotingConfigExclusionsRequest));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(responseException.getMessage(), containsString("is unauthorized for user"));
    }

    public void testOperatorUserCanCallNonOperatorOnlyApi() throws IOException {
        final Request mainRequest = new Request("GET", "/");
        final String authHeader = "Basic " + Base64.getEncoder().encodeToString(
            "test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        mainRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        client().performRequest(mainRequest);
    }
}
