/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.integration;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.snapshots.status.SnapshotsStatusResponse;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.nio.file.Path;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.TEST;

@ClusterScope(scope = TEST)
public class ClusterPrivilegeTests extends AbstractPrivilegeTestCase {

    public static final String ROLES =
                    "role_a:\n" +
                    "  cluster: [ all ]\n" +
                    "\n" +
                    "role_b:\n" +
                    "  cluster: [ monitor ]\n" +
                    "\n" +
                    "role_c:\n" +
                    "  indices:\n" +
                    "    - names: 'someindex'\n" +
                    "      privileges: [ all ]\n";

    public static final String USERS =
                    "user_a:" + USERS_PASSWD_HASHED + "\n" +
                    "user_b:" + USERS_PASSWD_HASHED + "\n" +
                    "user_c:" + USERS_PASSWD_HASHED + "\n";

    public static final String USERS_ROLES =
                    "role_a:user_a\n" +
                    "role_b:user_b\n" +
                    "role_c:user_c\n";

    static Path repositoryLocation;

    @BeforeClass
    public static void setupRepostoryPath() {
        repositoryLocation = createTempDir();
    }

    @AfterClass
    public static void cleanupRepositoryPath() {
        repositoryLocation = null;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal))
                .put(NetworkModule.HTTP_ENABLED.getKey(), true)
                .put("path.repo", repositoryLocation)
                .build();
    }

    @Override
    protected String configRoles() {
        return super.configRoles() + "\n" + ROLES;
    }

    @Override
    protected String configUsers() {
        return super.configUsers() + USERS;
    }

    @Override
    protected String configUsersRoles() {
        return super.configUsersRoles() + USERS_ROLES;
    }

    @TestLogging("org.elasticsearch.test.rest.client.http:TRACE")
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

        // user_b can do monitoring
        assertAccessIsAllowed("user_b", "GET", "/_cluster/state");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/health");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/settings");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/stats");
        assertAccessIsAllowed("user_b", "GET", "/_cluster/pending_tasks");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/stats");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/hot_threads");
        assertAccessIsAllowed("user_b", "GET", "/_nodes/infos");
        // but no admin stuff
        assertAccessIsDenied("user_b", "POST", "/_cluster/reroute");
        assertAccessIsDenied("user_b", "PUT", "/_cluster/settings", "{ \"transient\" : { \"search.default_search_timeout\": \"1m\" } }");

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
    }

    @TestLogging("org.elasticsearch.test.rest.client.http:TRACE")
    public void testThatSnapshotAndRestore() throws Exception {
        String repoJson = jsonBuilder().startObject().field("type", "fs").startObject("settings").field("location",
                repositoryLocation.toString()).endObject().endObject().string();
        assertAccessIsDenied("user_b", "PUT", "/_snapshot/my-repo", repoJson);
        assertAccessIsDenied("user_c", "PUT", "/_snapshot/my-repo", repoJson);
        assertAccessIsAllowed("user_a", "PUT", "/_snapshot/my-repo", repoJson);

        Map<String, String> params = singletonMap("refresh", "true");
        assertAccessIsDenied("user_a", "PUT", "/someindex/bar/1", "{ \"name\" : \"elasticsearch\" }", params);
        assertAccessIsDenied("user_b", "PUT", "/someindex/bar/1", "{ \"name\" : \"elasticsearch\" }", params);
        assertAccessIsAllowed("user_c", "PUT", "/someindex/bar/1", "{ \"name\" : \"elasticsearch\" }", params);

        assertAccessIsDenied("user_b", "PUT", "/_snapshot/my-repo/my-snapshot", "{ \"indices\": \"someindex\" }");
        assertAccessIsDenied("user_c", "PUT", "/_snapshot/my-repo/my-snapshot", "{ \"indices\": \"someindex\" }");
        assertAccessIsAllowed("user_a", "PUT", "/_snapshot/my-repo/my-snapshot", "{ \"indices\": \"someindex\" }");

        assertAccessIsDenied("user_b", "GET", "/_snapshot/my-repo/my-snapshot/_status");
        assertAccessIsDenied("user_c", "GET", "/_snapshot/my-repo/my-snapshot/_status");
        assertAccessIsAllowed("user_a", "GET", "/_snapshot/my-repo/my-snapshot/_status");

        // This snapshot needs to be finished in order to be restored
        waitForSnapshotToFinish("my-repo", "my-snapshot");

        assertAccessIsDenied("user_a", "DELETE", "/someindex");
        assertAccessIsDenied("user_b", "DELETE", "/someindex");
        assertAccessIsAllowed("user_c", "DELETE", "/someindex");

        params = singletonMap("wait_for_completion", "true");
        assertAccessIsDenied("user_b", "POST", "/_snapshot/my-repo/my-snapshot/_restore", null, params);
        assertAccessIsDenied("user_c", "POST", "/_snapshot/my-repo/my-snapshot/_restore", null, params);
        assertAccessIsAllowed("user_a", "POST", "/_snapshot/my-repo/my-snapshot/_restore", null, params);

        assertAccessIsDenied("user_a", "GET", "/someindex/bar/1");
        assertAccessIsDenied("user_b", "GET", "/someindex/bar/1");
        assertAccessIsAllowed("user_c", "GET", "/someindex/bar/1");

        assertAccessIsDenied("user_b", "DELETE", "/_snapshot/my-repo/my-snapshot");
        assertAccessIsDenied("user_c", "DELETE", "/_snapshot/my-repo/my-snapshot");
        assertAccessIsAllowed("user_a", "DELETE", "/_snapshot/my-repo/my-snapshot");

        assertAccessIsDenied("user_b", "DELETE", "/_snapshot/my-repo");
        assertAccessIsDenied("user_c", "DELETE", "/_snapshot/my-repo");
        assertAccessIsAllowed("user_a", "DELETE", "/_snapshot/my-repo");
    }

    private void waitForSnapshotToFinish(String repo, String snapshot) {
        SnapshotsStatusResponse snapshotsStatusResponse;
        int i = 0;
        do {
            snapshotsStatusResponse = client().admin().cluster().prepareSnapshotStatus(repo).setSnapshots(snapshot).get();
            try {
                Thread.sleep(200L);
            } catch (InterruptedException e) {}
            i++;
            if (i >= 20) { throw new ElasticsearchException("Snapshot should have been successfully created after four seconds, was " +
                    snapshotsStatusResponse.getSnapshots().get(0).getState()); }
        } while (snapshotsStatusResponse.getSnapshots().get(0).getState() != SnapshotsInProgress.State.SUCCESS);
    }
}
