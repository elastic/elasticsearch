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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class OperatorPrivilegesIT extends ESRestTestCase {

    private static final String OPERATOR_AUTH_HEADER = "Basic "
        + Base64.getEncoder().encodeToString("test_operator:x-pack-test-password".getBytes(StandardCharsets.UTF_8));;

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
        final String message = "An action should be declared to be either operator-only in ["
            + OperatorOnlyRegistry.class.getName()
            + "] or non-operator in ["
            + Constants.class.getName()
            + "]";

        Set<String> doubleLabelled = Sets.intersection(Constants.NON_OPERATOR_ACTIONS, OperatorOnlyRegistry.SIMPLE_ACTIONS);
        assertTrue("Actions are both operator-only and non-operator: [" + doubleLabelled + "]. " + message, doubleLabelled.isEmpty());

        final Request request = new Request("GET", "/_test/get_actions");
        final Map<String, Object> response = responseAsMap(client().performRequest(request));
        Set<String> allActions = Set.copyOf((List<String>) response.get("actions"));
        final HashSet<String> labelledActions = new HashSet<>(OperatorOnlyRegistry.SIMPLE_ACTIONS);
        labelledActions.addAll(Constants.NON_OPERATOR_ACTIONS);

        final Set<String> unlabelled = Sets.difference(allActions, labelledActions);
        assertTrue("Actions are neither operator-only nor non-operator: [" + unlabelled + "]. " + message, unlabelled.isEmpty());

        final Set<String> redundant = Sets.difference(labelledActions, allActions);
        assertTrue(
            "Actions may no longer be valid: ["
                + redundant
                + "]. They should be removed from either the operator-only action registry in ["
                + OperatorOnlyRegistry.class.getName()
                + "] or the non-operator action list in ["
                + Constants.class.getName()
                + "]",
            redundant.isEmpty()
        );
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

    public void testUpdateOperatorSettings() throws IOException {
        final Map<String, Object> settings = new HashMap<>(
            Map.of("xpack.security.http.filter.enabled", "false", "xpack.security.transport.filter.enabled", "false")
        );
        final boolean extraSettings = randomBoolean();
        if (extraSettings) {
            settings.put("search.allow_expensive_queries", false);
        }
        final ResponseException responseException = expectThrows(ResponseException.class, () -> updateSettings(settings, null));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(responseException.getMessage(), containsString("is unauthorized for user"));
        assertTrue(getPersistentSettings().isEmpty());

        updateSettings(settings, OPERATOR_AUTH_HEADER);

        Map<String, Object> persistentSettings = getPersistentSettings();
        assertThat(persistentSettings.get("xpack.security.http.filter.enabled"), equalTo("false"));
        assertThat(persistentSettings.get("xpack.security.transport.filter.enabled"), equalTo("false"));
        if (extraSettings) {
            assertThat(persistentSettings.get("search.allow_expensive_queries"), equalTo("false"));
        }
    }

    public void testSnapshotRestoreBehaviourOfOperatorSettings() throws IOException {
        final String repoName = "repo";
        final String snapshotName = "snap";
        createSnapshotRepo(repoName);
        // Initial values
        updateSettings(
            Map.of(
                "xpack.security.http.filter.enabled",
                "false",
                "xpack.security.http.filter.allow",
                "example.com",
                "search.default_keep_alive",
                "10m"
            ),
            OPERATOR_AUTH_HEADER
        );
        takeSnapshot(repoName, snapshotName);
        // change to different values
        deleteSettings(List.of("xpack.security.http.filter.enabled"), OPERATOR_AUTH_HEADER);
        updateSettings(
            Map.of(
                "xpack.security.transport.filter.enabled",
                "true",
                "xpack.security.http.filter.allow",
                "tutorial.com",
                "search.default_keep_alive",
                "1m",
                "search.allow_expensive_queries",
                "false"
            ),
            OPERATOR_AUTH_HEADER
        );

        // Restore with either operator or non-operator and the operator settings will not be touched
        restoreSnapshot(repoName, snapshotName, randomFrom(OPERATOR_AUTH_HEADER, null));
        Map<String, Object> persistentSettings = getPersistentSettings();
        assertNull(persistentSettings.get("xpack.security.http.filter.enabled"));
        assertThat(persistentSettings.get("xpack.security.transport.filter.enabled"), equalTo("true"));
        assertThat(persistentSettings.get("xpack.security.http.filter.allow"), equalTo("tutorial.com"));
        assertThat(persistentSettings.get("search.default_keep_alive"), equalTo("10m"));
        assertNull(persistentSettings.get("search.allow_expensive_queries"));
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

    private void updateSettings(Map<String, ?> settings, String authHeader) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        if (authHeader != null) {
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        }
        request.setJsonEntity(
            Strings.toString(
                JsonXContent.contentBuilder().startObject().startObject("persistent").mapContents(settings).endObject().endObject()
            )
        );
        assertOK(client().performRequest(request));
    }

    private void deleteSettings(Collection<String> settingKeys, String authHeader) throws IOException {
        final Request request = new Request("PUT", "/_cluster/settings");
        if (authHeader != null) {
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        }
        final XContentBuilder builder = JsonXContent.contentBuilder().startObject().startObject("persistent");
        for (String k : settingKeys) {
            builder.nullField(k);
        }
        builder.endObject().endObject();
        request.setJsonEntity(Strings.toString(builder));
        assertOK(client().performRequest(request));
    }

    private void takeSnapshot(String repoName, String snapshotName) throws IOException {
        final Request request = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName);
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().startObject().field("include_global_state", true).endObject())
        );
        assertOK(client().performRequest(request));
    }

    private void restoreSnapshot(String repoName, String snapshotName, String authHeader) throws IOException {
        final Request request = new Request("POST", "/_snapshot/" + repoName + "/" + snapshotName + "/_restore");
        if (authHeader != null) {
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
        }
        request.addParameter("wait_for_completion", "true");
        request.setJsonEntity(
            Strings.toString(JsonXContent.contentBuilder().startObject().field("include_global_state", true).endObject())
        );
        assertOK(client().performRequest(request));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getPersistentSettings() throws IOException {
        final Request getSettingsRequest = new Request("GET", "/_cluster/settings?flat_settings");
        Map<String, Object> response = entityAsMap(client().performRequest(getSettingsRequest));
        return (Map<String, Object>) response.get("persistent");
    }
}
