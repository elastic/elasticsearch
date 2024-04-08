/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.integration;

import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecuritySettingsSourceField;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.is;

public class ClusterPrivilegeIntegrationTests extends AbstractPrivilegeTestCase {

    private static final String ROLES = """
        role_a:
          cluster: [ all ]

        role_b:
          cluster: [ monitor ]

        role_c:
          indices:
            - names: 'someindex'
              privileges: [ all ]
        role_d:
          cluster: [ create_snapshot ]

        role_e:
          cluster: [ monitor_snapshot]
        """;

    private static final String USERS_ROLES = """
        role_a:user_a
        role_b:user_b
        role_c:user_c
        role_d:user_d
        role_e:user_e
        """;

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

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + ROLES;
    }

    @Override
    protected String configUsers() {
        final Hasher passwdHasher = getFastStoredHashAlgoForTests();
        final String usersPasswdHashed = new String(passwdHasher.hash(SecuritySettingsSourceField.TEST_PASSWORD_SECURE_STRING));
        return super.configUsers()
            + "user_a:"
            + usersPasswdHashed
            + "\n"
            + "user_b:"
            + usersPasswdHashed
            + "\n"
            + "user_c:"
            + usersPasswdHashed
            + "\n"
            + "user_d:"
            + usersPasswdHashed
            + "\n"
            + "user_e:"
            + usersPasswdHashed
            + "\n";
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + USERS_ROLES;
    }

    public void testThatClusterPrivilegesWorkAsExpectedViaHttp() throws Exception {
        // user_a can do all the things
        assertAccessIsAllowed("user_a", "GET", "/_cluster/state");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/health");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/settings");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/stats");
        assertAccessIsAllowed("user_a", "GET", "/_cluster/pending_tasks");
        assertAccessIsAllowed("user_a", "GET", "/_nodes/stats");
        assertAccessIsAllowed("user_a", "GET", "/_nodes/hot_threads");
        assertAccessIsAllowed("user_a", "GET", "/_nodes/infos");
        assertAccessIsAllowed("user_a", "POST", "/_cluster/reroute");
        assertAccessIsAllowed("user_a", "PUT", "/_cluster/settings", "{ \"transient\" : { \"search.default_search_timeout\": \"1m\" } }");
        assertAccessIsAllowed("user_a", "PUT", "/_cluster/settings", "{ \"transient\" : { \"search.default_search_timeout\": null } }");

        // user_b can do monitoring
        assertAccessIsAllowed("user_b", "GET", "/_cluster/state");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/health");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/settings");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/stats");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/pending_tasks");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/stats");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/hot_threads");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/infos");
        // monitoring allows template retrieval (because it's implied by having read access to cluster state
        assertAccessIsAllowed("user_b", "GET", "/_cat/templates/" + (randomBoolean() ? "" : randomAlphaOfLengthBetween(2, 8)));
        assertAccessIsAllowed("user_b", "GET", "/_template/");
        assertAccessIsAllowed("user_b", "GET", "/_index_template/");
        assertAccessIsAllowed("user_b", "GET", "/_component_template/");
        // but no admin stuff
        assertAccessIsDenied("user_b", "POST", "/_cluster/reroute");
        assertAccessIsDenied("user_b", "PUT", "/_cluster/settings", "{ \"transient\" : { \"search.default_search_timeout\": \"1m\" } }");
        assertAccessIsDenied("user_b", "DELETE", "/_template/" + randomAlphaOfLengthBetween(2, 8));
        assertAccessIsDenied("user_b", "DELETE", "/_index_template/" + randomAlphaOfLengthBetween(2, 8));
        assertAccessIsDenied("user_b", "DELETE", "/_component_template/" + randomAlphaOfLengthBetween(2, 8));

        // sorry user_c, you are not allowed anything
        assertAccessIsDenied("user_c", "GET", "/_cluster/state");
        assertAccessIsDenied("user_c", "GET", "/_cluster/health");
        assertAccessIsDenied("user_c", "GET", "/_cluster/settings");
        assertAccessIsDenied("user_c", "GET", "/_cluster/stats");
        assertAccessIsDenied("user_c", "GET", "/_cluster/pending_tasks");
        assertAccessIsDenied("user_c", "GET", "/_nodes/stats");
        assertAccessIsDenied("user_c", "GET", "/_nodes/hot_threads");
        assertAccessIsDenied("user_c", "GET", "/_nodes/infos");
        assertAccessIsDenied("user_c", "POST", "/_cluster/reroute");
        assertAccessIsDenied("user_c", "PUT", "/_cluster/settings", "{ \"transient\" : { \"search.default_search_timeout\": \"1m\" } }");

        // user_d can view repos and create and view snapshots on existings repos, everything else is DENIED
        assertAccessIsDenied("user_d", "GET", "/_cluster/state");
        assertAccessIsDenied("user_d", "GET", "/_cluster/health");
        assertAccessIsDenied("user_d", "GET", "/_cluster/settings");
        assertAccessIsDenied("user_d", "GET", "/_cluster/stats");
        assertAccessIsDenied("user_d", "GET", "/_cluster/pending_tasks");
        assertAccessIsDenied("user_d", "GET", "/_nodes/stats");
        assertAccessIsDenied("user_d", "GET", "/_nodes/hot_threads");
        assertAccessIsDenied("user_d", "GET", "/_nodes/infos");
        assertAccessIsDenied("user_d", "POST", "/_cluster/reroute");
        assertAccessIsDenied("user_d", "PUT", "/_cluster/settings", "{ \"transient\" : { \"search.default_search_timeout\": \"1m\" } }");

        // user_e can view repos and snapshots on existing repos, everything else is DENIED
        assertAccessIsDenied("user_e", "GET", "/_cluster/state");
        assertAccessIsDenied("user_e", "GET", "/_cluster/health");
        assertAccessIsDenied("user_e", "GET", "/_cluster/settings");
        assertAccessIsDenied("user_e", "GET", "/_cluster/stats");
        assertAccessIsDenied("user_e", "GET", "/_cluster/pending_tasks");
        assertAccessIsDenied("user_e", "GET", "/_nodes/stats");
        assertAccessIsDenied("user_e", "GET", "/_nodes/hot_threads");
        assertAccessIsDenied("user_e", "GET", "/_nodes/infos");
        assertAccessIsDenied("user_e", "POST", "/_cluster/reroute");
        assertAccessIsDenied("user_e", "PUT", "/_cluster/settings", "{ \"transient\" : { \"search.default_search_timeout\": \"1m\" } }");

    }

    public void testThatSnapshotAndRestore() throws Exception {
        String repoJson = Strings.toString(
            jsonBuilder().startObject()
                .field("type", "fs")
                .startObject("settings")
                .field("location", repositoryLocation.toString())
                .endObject()
                .endObject()
        );
        assertAccessIsDenied("user_b", "PUT", "/_snapshot/my-repo", repoJson);
        assertAccessIsDenied("user_c", "PUT", "/_snapshot/my-repo", repoJson);
        assertAccessIsDenied("user_d", "PUT", "/_snapshot/my-repo", repoJson);
        assertAccessIsDenied("user_e", "PUT", "/_snapshot/my-repo", repoJson);
        assertAccessIsAllowed("user_a", "PUT", "/_snapshot/my-repo", repoJson);

        Request createBar = new Request("PUT", "/someindex/_doc/1");
        createBar.setJsonEntity("{ \"name\" : \"elasticsearch\" }");
        createBar.addParameter("refresh", "true");
        assertAccessIsDenied("user_a", createBar);
        assertAccessIsDenied("user_b", createBar);
        assertAccessIsDenied("user_d", createBar);
        assertAccessIsDenied("user_e", createBar);
        assertAccessIsAllowed("user_c", createBar);

        assertAccessIsDenied("user_b", "PUT", "/_snapshot/my-repo/my-snapshot", "{ \"indices\": \"someindex\" }");
        assertAccessIsDenied("user_c", "PUT", "/_snapshot/my-repo/my-snapshot", "{ \"indices\": \"someindex\" }");
        assertAccessIsDenied("user_e", "PUT", "/_snapshot/my-repo/my-snapshot", "{ \"indices\": \"someindex\" }");
        assertAccessIsAllowed("user_a", "PUT", "/_snapshot/my-repo/my-snapshot", "{ \"indices\": \"someindex\" }");

        assertAccessIsDenied("user_b", "GET", "/_snapshot/my-repo/my-snapshot/_status");
        assertAccessIsDenied("user_c", "GET", "/_snapshot/my-repo/my-snapshot/_status");
        assertAccessIsAllowed("user_a", "GET", "/_snapshot/my-repo/my-snapshot/_status");
        assertAccessIsAllowed("user_d", "GET", "/_snapshot/my-repo/my-snapshot/_status");
        assertAccessIsAllowed("user_e", "GET", "/_snapshot/my-repo/my-snapshot/_status");

        // This snapshot needs to be finished in order to be restored
        waitForSnapshotToFinish("my-repo", "my-snapshot");
        // user_d can create snapshots, but not concurrently
        assertAccessIsAllowed("user_d", "PUT", "/_snapshot/my-repo/my-snapshot-d", "{ \"indices\": \"someindex\" }");
        waitForSnapshotToFinish("my-repo", "my-snapshot-d");

        assertAccessIsDenied("user_a", "DELETE", "/someindex");
        assertAccessIsDenied("user_b", "DELETE", "/someindex");
        assertAccessIsDenied("user_d", "DELETE", "/someindex");
        assertAccessIsDenied("user_e", "DELETE", "/someindex");
        assertAccessIsAllowed("user_c", "DELETE", "/someindex");

        Request restoreSnapshotRequest = new Request("POST", "/_snapshot/my-repo/my-snapshot/_restore");
        restoreSnapshotRequest.addParameter("wait_for_completion", "true");
        assertAccessIsDenied("user_b", restoreSnapshotRequest);
        assertAccessIsDenied("user_c", restoreSnapshotRequest);
        assertAccessIsDenied("user_d", restoreSnapshotRequest);
        assertAccessIsDenied("user_e", restoreSnapshotRequest);
        assertAccessIsAllowed("user_a", restoreSnapshotRequest);

        assertAccessIsDenied("user_a", "GET", "/someindex/_doc/1");
        assertAccessIsDenied("user_b", "GET", "/someindex/_doc/1");
        assertAccessIsDenied("user_d", "GET", "/someindex/_doc/1");
        assertAccessIsDenied("user_e", "GET", "/someindex/_doc/1");
        assertAccessIsAllowed("user_c", "GET", "/someindex/_doc/1");

        assertAccessIsDenied("user_b", "DELETE", "/_snapshot/my-repo/my-snapshot");
        assertAccessIsDenied("user_c", "DELETE", "/_snapshot/my-repo/my-snapshot");
        assertAccessIsDenied("user_d", "DELETE", "/_snapshot/my-repo/my-snapshot");
        assertAccessIsDenied("user_e", "DELETE", "/_snapshot/my-repo/my-snapshot");
        assertAccessIsAllowed("user_a", "DELETE", "/_snapshot/my-repo/my-snapshot");

        assertAccessIsDenied("user_b", "DELETE", "/_snapshot/my-repo");
        assertAccessIsDenied("user_c", "DELETE", "/_snapshot/my-repo");
        assertAccessIsDenied("user_d", "DELETE", "/_snapshot/my-repo");
        assertAccessIsDenied("user_e", "DELETE", "/_snapshot/my-repo");
        assertAccessIsAllowed("user_a", "DELETE", "/_snapshot/my-repo");
    }

    private void waitForSnapshotToFinish(String repo, String snapshot) throws Exception {
        assertBusy(() -> {
            SnapshotsStatusResponse response = clusterAdmin().prepareSnapshotStatus(repo).setSnapshots(snapshot).get();
            assertThat(response.getSnapshots().get(0).getState(), is(SnapshotsInProgress.State.SUCCESS));
            // The status of the snapshot in the repository can become SUCCESS before it is fully finalized in the cluster state so wait for
            // it to disappear from the cluster state as well
            SnapshotsInProgress snapshotsInProgress = SnapshotsInProgress.get(
                clusterAdmin().state(new ClusterStateRequest()).get().getState()
            );
            assertTrue(snapshotsInProgress.isEmpty());
        });
    }
}
