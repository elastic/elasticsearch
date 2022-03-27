/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.integration;

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;

import static org.elasticsearch.test.SecuritySettingsSource.ES_TEST_ROOT_USER;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class SecurityFeatureStateIntegTests extends AbstractPrivilegeTestCase {

    private static final String LOCAL_TEST_USER_NAME = "feature_state_user";
    private static final String LOCAL_TEST_USER_PASSWORD = "my_password";
    private static Path repositoryLocation;

    @BeforeClass
    public static void setupRepositoryPath() {
        repositoryLocation = createTempDir();
    }

    @AfterClass
    public static void cleanupRepositoryPath() {
        repositoryLocation = null;
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Settings nodeSettings() {
        return Settings.builder().put(super.nodeSettings()).put("path.repo", repositoryLocation).build();
    }

    /**
     * Test that, when the security system index is restored as a feature state,
     * the security plugin's listeners detect the state change and reload native
     * realm privileges.
     *
     * We use the admin client to handle snapshots and the rest API to manage
     * security roles and users. We use the native realm instead of the file
     * realm because this test relies on dynamically changing privileges.
     */
    public void testSecurityFeatureStateSnapshotAndRestore() throws Exception {
        // set up a snapshot repository
        final String repositoryName = "test-repo";
        client().admin()
            .cluster()
            .preparePutRepository(repositoryName)
            .setType("fs")
            .setSettings(Settings.builder().put("location", repositoryLocation))
            .get();

        // create a new role
        final String roleName = "extra_role";
        final Request createRoleRequest = new Request("PUT", "/_security/role/" + roleName);
        createRoleRequest.addParameter("refresh", "wait_for");
        createRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": [ "test_index" ],
                  "privileges": [ "create", "create_index", "create_doc" ]
                }
              ]
            }""");
        performSuperuserRequest(createRoleRequest);

        // create a test user
        final Request createUserRequest = new Request("PUT", "/_security/user/" + LOCAL_TEST_USER_NAME);
        createUserRequest.addParameter("refresh", "wait_for");
        createUserRequest.setJsonEntity("""
            {  "password": "%s",  "roles": [ "%s" ]}
            """.formatted(LOCAL_TEST_USER_PASSWORD, roleName));
        performSuperuserRequest(createUserRequest);

        // test user posts a document
        final Request postTestDocument1 = new Request("POST", "/test_index/_doc");
        postTestDocument1.setJsonEntity("""
            {"message": "before snapshot"}
            """);
        performTestUserRequest(postTestDocument1);

        // snapshot state
        final String snapshotName = "security-state";
        client().admin()
            .cluster()
            .prepareCreateSnapshot(repositoryName, snapshotName)
            .setIndices("test_index")
            .setFeatureStates("LocalStateSecurity")
            .get();
        waitForSnapshotToFinish(repositoryName, snapshotName);

        // modify user's roles
        final Request modifyUserRequest = new Request("PUT", "/_security/user/" + LOCAL_TEST_USER_NAME);
        modifyUserRequest.addParameter("refresh", "wait_for");
        modifyUserRequest.setJsonEntity("{\"roles\": [] }");
        performSuperuserRequest(modifyUserRequest);

        // new user has lost privileges and can't post a document
        final Request postDocumentRequest2 = new Request("POST", "/test_index/_doc");
        postDocumentRequest2.setJsonEntity("{\"message\": \"between snapshot and restore\"}");
        ResponseException exception = expectThrows(ResponseException.class, () -> performTestUserRequest(postDocumentRequest2));

        assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            exception.getMessage(),
            containsString("action [" + IndexAction.NAME + "] is unauthorized for user [" + LOCAL_TEST_USER_NAME + "]")
        );

        client().admin().indices().prepareClose("test_index").get();

        // restore state
        client().admin()
            .cluster()
            .prepareRestoreSnapshot(repositoryName, snapshotName)
            .setFeatureStates("LocalStateSecurity")
            .setIndices("test_index")
            .setWaitForCompletion(true)
            .get();

        // user has privileges again
        final Request postDocumentRequest3 = new Request("POST", "/test_index/_doc");
        postDocumentRequest3.setJsonEntity("{\"message\": \"after restore\"}");
        performTestUserRequest(postDocumentRequest3);
    }

    private Response performSuperuserRequest(Request request) throws Exception {
        String token = UsernamePasswordToken.basicAuthHeaderValue(
            ES_TEST_ROOT_USER,
            new SecureString(SecuritySettingsSourceField.TEST_PASSWORD.toCharArray())
        );
        return performAuthenticatedRequest(request, token);
    }

    private Response performTestUserRequest(Request request) throws Exception {
        String token = UsernamePasswordToken.basicAuthHeaderValue(
            LOCAL_TEST_USER_NAME,
            new SecureString(LOCAL_TEST_USER_PASSWORD.toCharArray())
        );
        return performAuthenticatedRequest(request, token);
    }

    private Response performAuthenticatedRequest(Request request, String token) throws Exception {
        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("Authorization", token);
        request.setOptions(options);
        return getRestClient().performRequest(request);
    }

    private void waitForSnapshotToFinish(String repo, String snapshot) throws Exception {
        assertBusy(() -> {
            SnapshotsStatusResponse response = client().admin().cluster().prepareSnapshotStatus(repo).setSnapshots(snapshot).get();
            assertThat(response.getSnapshots().get(0).getState(), is(SnapshotsInProgress.State.SUCCESS));
            // The status of the snapshot in the repository can become SUCCESS before it is fully finalized in the cluster state so wait for
            // it to disappear from the cluster state as well
            SnapshotsInProgress snapshotsInProgress = client().admin()
                .cluster()
                .state(new ClusterStateRequest())
                .get()
                .getState()
                .custom(SnapshotsInProgress.TYPE);
            assertTrue(snapshotsInProgress.isEmpty());
        });
    }
}
