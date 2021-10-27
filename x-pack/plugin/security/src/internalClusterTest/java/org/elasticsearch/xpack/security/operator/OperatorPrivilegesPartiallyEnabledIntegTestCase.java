/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

import static org.elasticsearch.test.SecuritySettingsSource.TEST_PASSWORD_HASHED;
import static org.elasticsearch.test.SecuritySettingsSourceField.TEST_PASSWORD;
import static org.hamcrest.Matchers.equalTo;

// This test is to ensure that an operator can always successfully perform operator-only actions
// even when operator privileges are partially enabled for the cluster. This could happen
// for a cluster with operator privileges disabled wanting to enable operator privileges with rolling upgrade.
public class OperatorPrivilegesPartiallyEnabledIntegTestCase extends SecurityIntegTestCase {

    private static final String OPERATOR_USER_NAME = "test_operator";
    private static final String OPERATOR_AUTH_HEADER = "Basic "
        + Base64.getEncoder().encodeToString((OPERATOR_USER_NAME + ":" + TEST_PASSWORD).getBytes(StandardCharsets.UTF_8));

    @Override
    protected String configUsers() {
        return super.configUsers() + OPERATOR_USER_NAME + ":" + TEST_PASSWORD_HASHED + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + "superuser:" + OPERATOR_USER_NAME + "\n";
    }

    @Override
    protected String configOperatorUsers() {
        return super.configOperatorUsers() + "operator:\n" + "  - usernames: ['" + OPERATOR_USER_NAME + "']\n";
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        // randomly enable/disable operator privileges
        builder.put("xpack.security.operator_privileges.enabled", randomBoolean());
        return builder.build();
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testOperatorWillSucceedToPerformOperatorOnlyAction() throws IOException {
        final RestClient restClient = getRestClient();

        final Request clearVotingRequest = new Request("DELETE", "/_cluster/voting_config_exclusions");
        clearVotingRequest.setOptions(clearVotingRequest.getOptions().toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));
        final Request getShutdownRequest = new Request("GET", "/_nodes/shutdown");
        getShutdownRequest.setOptions(getShutdownRequest.getOptions().toBuilder().addHeader("Authorization", OPERATOR_AUTH_HEADER));

        // RestClient round-robin requests to each node, we run the requests in a loop so that each node
        // has a chance to handle at least one operator-only request (does not matter which one).
        // They must all be successful to prove that any node in a cluster with partially enabled operator privileges
        // can handle operator-only actions with no error.
        for (int i = 0; i < cluster().size(); i++) {
            final Response clearVotingResponse = restClient.performRequest(clearVotingRequest);
            assertThat(clearVotingResponse.getStatusLine().getStatusCode(), equalTo(200));
            final Response getShutdownResponse = restClient.performRequest(getShutdownRequest);
            assertThat(getShutdownResponse.getStatusLine().getStatusCode(), equalTo(200));
        }
    }
}
