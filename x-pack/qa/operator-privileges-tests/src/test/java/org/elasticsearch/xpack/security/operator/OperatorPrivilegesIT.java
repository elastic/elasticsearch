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
import java.util.Base64;

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

    public void testNonOperatorSuperuserWillFailToCallOperatorOnlyApi() throws IOException {
        final Request postVotingConfigExclusionsRequest = new Request(
            "POST", "_cluster/voting_config_exclusions?node_names=foo");
        final ResponseException responseException = expectThrows(
            ResponseException.class,
            () -> client().performRequest(postVotingConfigExclusionsRequest));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(responseException.getMessage(), containsString("Operator privileges are required for action"));
    }

    public void testOperatorUserWillSucceedToCallOperatorOnlyApi() throws IOException {
        final Request postVotingConfigExclusionsRequest = new Request(
            "POST", "_cluster/voting_config_exclusions?node_names=foo");
        final String authHeader = "Basic " + Base64.getEncoder().encodeToString("test_operator:x-pack-test-password".getBytes());
        postVotingConfigExclusionsRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        client().performRequest(postVotingConfigExclusionsRequest);
    }

    public void testOperatorUserWillFailToCallOperatorOnlyApiIfRbacFails() throws IOException {
        final Request deleteVotingConfigExclusionsRequest = new Request(
            "DELETE", "_cluster/voting_config_exclusions");
        final String authHeader = "Basic " + Base64.getEncoder().encodeToString("test_operator:x-pack-test-password".getBytes());
        deleteVotingConfigExclusionsRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        final ResponseException responseException = expectThrows(ResponseException.class,
            () -> client().performRequest(deleteVotingConfigExclusionsRequest));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(responseException.getMessage(), containsString("is unauthorized for user"));
    }
}
