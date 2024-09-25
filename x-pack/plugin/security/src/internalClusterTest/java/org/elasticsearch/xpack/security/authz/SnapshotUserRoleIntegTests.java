/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

import static org.elasticsearch.test.SecuritySettingsSource.SECURITY_REQUEST_OPTIONS;
import static org.elasticsearch.test.SecurityTestsUtils.assertThrowsAuthorizationException;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SECURITY_MAIN_ALIAS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class SnapshotUserRoleIntegTests extends NativeRealmIntegTestCase {

    private Client client;
    private String ordinaryIndex;

    @Before
    public void setupClusterBeforeSnapshot() throws IOException {
        logger.info("-->  creating repository");
        assertAcked(
            clusterAdmin().preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
        );

        logger.info("-->  creating ordinary index");
        final int shards = between(1, 10);
        ordinaryIndex = randomAlphaOfLength(4).toLowerCase(Locale.ROOT);
        assertAcked(prepareCreate(ordinaryIndex, 0, indexSettings(shards, 0)));
        ensureGreen();

        logger.info("-->  creating snapshot_user user");
        final String user = "snapshot_user";
        final char[] password = new char[] { 'p', 'a', 's', 's', 'w', 'o', 'r', 'd' };
        final String snapshotUserToken = basicAuthHeaderValue(user, new SecureString(password));
        client = client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken));
        getSecurityClient().putUser(new org.elasticsearch.xpack.core.security.user.User(user, "snapshot_user"), new SecureString(password));
        ensureGreen(INTERNAL_SECURITY_MAIN_INDEX_7);
    }

    public void testSnapshotUserRoleCanSnapshotAndSeeAllIndices() {
        // view repositories
        final GetRepositoriesResponse getRepositoriesResponse = client.admin()
            .cluster()
            .prepareGetRepositories(TEST_REQUEST_TIMEOUT, randomFrom("*", "_all"))
            .get();
        assertThat(getRepositoriesResponse.repositories().size(), is(1));
        assertThat(getRepositoriesResponse.repositories().get(0).name(), is("repo"));
        // view all indices, including restricted ones
        final GetIndexResponse getIndexResponse = client.admin()
            .indices()
            .prepareGetIndex()
            .setIndices(randomFrom("_all", "*"))
            .setIndicesOptions(IndicesOptions.strictExpandHidden())
            .get();
        assertThat(Arrays.asList(getIndexResponse.indices()), containsInAnyOrder(INTERNAL_SECURITY_MAIN_INDEX_7, ordinaryIndex));
        // create snapshot that includes restricted indices
        final CreateSnapshotResponse snapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "repo", "snap")
            .setIndices(randomFrom("_all", "*"))
            .setIndicesOptions(IndicesOptions.strictExpandHidden())
            .setWaitForCompletion(true)
            .get();
        assertThat(snapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotResponse.getSnapshotInfo().indices(), containsInAnyOrder(INTERNAL_SECURITY_MAIN_INDEX_7, ordinaryIndex));
        // view snapshots for repo
        final GetSnapshotsResponse getSnapshotResponse = client.admin().cluster().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "repo").get();
        assertThat(getSnapshotResponse.getSnapshots().size(), is(1));
        assertThat(getSnapshotResponse.getSnapshots().get(0).snapshotId().getName(), is("snap"));
        assertThat(getSnapshotResponse.getSnapshots().get(0).indices(), containsInAnyOrder(INTERNAL_SECURITY_MAIN_INDEX_7, ordinaryIndex));
    }

    public void testSnapshotUserRoleIsReserved() {
        final TestSecurityClient securityClient = getSecurityClient(SECURITY_REQUEST_OPTIONS);

        ResponseException e = expectThrows(
            ResponseException.class,
            () -> securityClient.putRole(new RoleDescriptor("snapshot_user", new String[] { "all" }, null, new String[] { "*" }))
        );
        assertThat(e.getMessage(), containsString("Role [snapshot_user] is reserved and may not be used"));

        e = expectThrows(ResponseException.class, () -> securityClient.deleteRole("snapshot_user"));
        assertThat(e.getMessage(), containsString("role [snapshot_user] is reserved and cannot be deleted"));
    }

    public void testSnapshotUserRoleUnathorizedForDestructiveActions() {
        // try search all
        assertThrowsAuthorizationException(
            () -> client.prepareSearch(randomFrom("_all", "*")).get(),
            "indices:data/read/search",
            "snapshot_user"
        );
        // try create index
        assertThrowsAuthorizationException(
            () -> client.admin().indices().prepareCreate(ordinaryIndex + "2").get(),
            "indices:admin/create",
            "snapshot_user"
        );
        // try create another repo
        assertThrowsAuthorizationException(
            () -> client.admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "some_other_repo")
                .setType("fs")
                .setSettings(Settings.builder().put("location", randomRepoPath()))
                .get(),
            "cluster:admin/repository/put",
            "snapshot_user"
        );
        // try delete repo
        assertThrowsAuthorizationException(
            () -> client.admin().cluster().prepareDeleteRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "repo").get(),
            "cluster:admin/repository/delete",
            "snapshot_user"
        );
        // try fumble with snapshots
        assertThrowsAuthorizationException(
            () -> client.admin()
                .cluster()
                .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "repo", randomAlphaOfLength(4).toLowerCase(Locale.ROOT))
                .get(),
            "cluster:admin/snapshot/restore",
            "snapshot_user"
        );
        assertThrowsAuthorizationException(
            () -> client.admin()
                .cluster()
                .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "repo", randomAlphaOfLength(4).toLowerCase(Locale.ROOT))
                .get(),
            "cluster:admin/snapshot/delete",
            "snapshot_user"
        );
        // try destructive/revealing actions on all indices
        for (final String indexToTest : Arrays.asList(INTERNAL_SECURITY_MAIN_INDEX_7, SECURITY_MAIN_ALIAS, ordinaryIndex)) {
            assertThrowsAuthorizationException(() -> client.prepareSearch(indexToTest).get(), "indices:data/read/search", "snapshot_user");
            assertThrowsAuthorizationException(() -> client.prepareGet(indexToTest, "1").get(), "indices:data/read/get", "snapshot_user");
            assertThrowsAuthorizationException(
                () -> client.prepareIndex(indexToTest).setSource("term", "val").get(),
                "indices:data/write/index",
                "snapshot_user"
            );
            assertThrowsAuthorizationException(
                () -> client.prepareUpdate(indexToTest, "1").setDoc("term", "val").get(),
                "indices:data/write/update",
                "snapshot_user"
            );
            assertThrowsAuthorizationException(
                () -> client.prepareDelete(indexToTest, "1").get(),
                "indices:data/write/delete",
                "snapshot_user"
            );

            assertThrowsAuthorizationException(
                () -> client.admin().indices().prepareDelete(indexToTest).get(),
                "indices:admin/delete",
                "snapshot_user"
            );
        }
    }

}
