/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.PutUserResponse;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SnapshotUserRoleIntegTests extends NativeRealmIntegTestCase {

    private Client client;
    private String ordinaryIndex;

    @Before
    public void setupClusterBeforeSnapshot() throws IOException {
        logger.info("-->  creating repository");
        assertAcked(client().admin().cluster().preparePutRepository("repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath())));

        logger.info("-->  creating ordinary index");
        final int shards = between(1, 10);
        ordinaryIndex = randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(ordinaryIndex, 0, Settings.builder().put("number_of_shards", shards).put("number_of_replicas", 0)));
        ensureGreen();

        logger.info("-->  creating snapshot_user user");
        final String user = "snapshot_user";
        final char[] password = new char[] {'p', 'a', 's', 's', 'w', 'o', 'r', 'd'};
        final String snapshotUserToken = basicAuthHeaderValue(user, new SecureString(password));
        client = client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken));
        PutUserResponse response = new TestRestHighLevelClient().security().putUser(
            PutUserRequest.withPassword(new User(user, List.of("snapshot_user")), password, true, RefreshPolicy.IMMEDIATE),
            SECURITY_REQUEST_OPTIONS);
        assertTrue(response.isCreated());
        ensureGreen(INTERNAL_SECURITY_MAIN_INDEX_7);
    }

    public void testSnapshotUserRoleCanSnapshotAndSeeAllIndices() {
        // view repositories
        final GetRepositoriesResponse getRepositoriesResponse = client.admin().cluster().prepareGetRepositories(randomFrom("*", "_all"))
                .get();
        assertThat(getRepositoriesResponse.repositories().size(), is(1));
        assertThat(getRepositoriesResponse.repositories().get(0).name(), is("repo"));
        // view all indices, including restricted ones
        final GetIndexResponse getIndexResponse = client.admin().indices().prepareGetIndex().setIndices(randomFrom("_all", "*")).get();
        assertThat(Arrays.asList(getIndexResponse.indices()), containsInAnyOrder(INTERNAL_SECURITY_MAIN_INDEX_7, ordinaryIndex));
        // create snapshot that includes restricted indices
        final CreateSnapshotResponse snapshotResponse = client.admin().cluster().prepareCreateSnapshot("repo", "snap")
                .setIndices(randomFrom("_all", "*")).setWaitForCompletion(true).get();
        assertThat(snapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotResponse.getSnapshotInfo().indices(), containsInAnyOrder(INTERNAL_SECURITY_MAIN_INDEX_7, ordinaryIndex));
        // view snapshots for repo
        final GetSnapshotsResponse getSnapshotResponse = client.admin().cluster().prepareGetSnapshots("repo").get();
        assertThat(getSnapshotResponse.getSnapshots().size(), is(1));
        assertThat(getSnapshotResponse.getSnapshots().get(0).snapshotId().getName(), is("snap"));
        assertThat(getSnapshotResponse.getSnapshots().get(0).indices(), containsInAnyOrder(INTERNAL_SECURITY_MAIN_INDEX_7,
                ordinaryIndex));
    }

    public void testSnapshotUserRoleIsReserved() {
        final RestHighLevelClient restClient = new TestRestHighLevelClient();
        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                () -> restClient.security().putRole(
                    new PutRoleRequest(Role.builder().name("snapshot_user").build(), RefreshPolicy.IMMEDIATE), SECURITY_REQUEST_OPTIONS));
        assertThat(e.getMessage(), containsString("role [snapshot_user] is reserved and cannot be modified"));
        e = expectThrows(ElasticsearchStatusException.class,
                () -> restClient.security().deleteRole(
                    new DeleteRoleRequest("snapshot_user", RefreshPolicy.IMMEDIATE), SECURITY_REQUEST_OPTIONS));
        assertThat(e.getMessage(), containsString("role [snapshot_user] is reserved and cannot be deleted"));
    }

    public void testSnapshotUserRoleUnathorizedForDestructiveActions() {
        // try search all
        assertThrowsAuthorizationException(() -> client.prepareSearch(randomFrom("_all", "*")).get(), "indices:data/read/search",
                "snapshot_user");
        // try create index
        assertThrowsAuthorizationException(() -> client.admin().indices().prepareCreate(ordinaryIndex + "2").get(), "indices:admin/create",
                "snapshot_user");
        // try create another repo
        assertThrowsAuthorizationException(
                () -> client.admin().cluster().preparePutRepository("some_other_repo").setType("fs")
                        .setSettings(Settings.builder().put("location", randomRepoPath())).get(),
                "cluster:admin/repository/put", "snapshot_user");
        // try delete repo
        assertThrowsAuthorizationException(() -> client.admin().cluster().prepareDeleteRepository("repo").get(),
                "cluster:admin/repository/delete", "snapshot_user");
        // try fumble with snapshots
        assertThrowsAuthorizationException(
                () -> client.admin().cluster().prepareRestoreSnapshot("repo", randomAlphaOfLength(4).toLowerCase(Locale.ROOT)).get(),
                "cluster:admin/snapshot/restore", "snapshot_user");
        assertThrowsAuthorizationException(
                () -> client.admin().cluster().prepareDeleteSnapshot("repo", randomAlphaOfLength(4).toLowerCase(Locale.ROOT)).get(),
                "cluster:admin/snapshot/delete", "snapshot_user");
        // try destructive/revealing actions on all indices
        for (final String indexToTest : Arrays.asList(INTERNAL_SECURITY_MAIN_INDEX_7, SECURITY_MAIN_ALIAS, ordinaryIndex)) {
            assertThrowsAuthorizationException(() -> client.prepareSearch(indexToTest).get(), "indices:data/read/search", "snapshot_user");
            assertThrowsAuthorizationException(() -> client.prepareGet(indexToTest, "1").get(), "indices:data/read/get",
                    "snapshot_user");
            assertThrowsAuthorizationException(() -> client.prepareIndex(indexToTest).setSource("term", "val").get(),
                    "indices:data/write/index", "snapshot_user");
            assertThrowsAuthorizationException(() -> client.prepareUpdate(indexToTest, "1").setDoc("term", "val").get(),
                    "indices:data/write/update", "snapshot_user");
            assertThrowsAuthorizationException(() -> client.prepareDelete(indexToTest, "1").get(), "indices:data/write/delete",
                    "snapshot_user");

            assertThrowsAuthorizationException(() -> client.admin().indices().prepareDelete(indexToTest).get(), "indices:admin/delete",
                    "snapshot_user");
        }
    }

}
