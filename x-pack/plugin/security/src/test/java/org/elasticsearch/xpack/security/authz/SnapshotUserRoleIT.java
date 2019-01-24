/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.repositories.get.GetRepositoriesResponse;
import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.indices.get.GetIndexResponse;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.NativeRealmIntegTestCase;
import org.elasticsearch.xpack.core.security.authc.support.Hasher;
import org.junit.Before;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.INTERNAL_SECURITY_INDEX;
import static org.elasticsearch.xpack.core.security.index.RestrictedIndicesNames.SECURITY_INDEX_NAME;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class SnapshotUserRoleIT extends NativeRealmIntegTestCase {

    private String snapshotUserToken; 
    private String ordinaryIndex;

    @Before
    public void setupClusterBeforeSnapshot() {
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
        snapshotUserToken = basicAuthHeaderValue(user, new SecureString(password));
        securityClient().preparePutUser(user, password, Hasher.BCRYPT, "snapshot_user").get();
        ensureGreen(INTERNAL_SECURITY_INDEX);
    }

    public void testSnapshotUserRoleCanSnapshotAndSeeAllIndices() {
        // view repositories
        final GetRepositoriesResponse getRepositoriesResponse = client()
                .filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).admin().cluster()
                .prepareGetRepositories(randomFrom("*", "_all")).get();
        assertThat(getRepositoriesResponse.repositories().size(), is(1));
        assertThat(getRepositoriesResponse.repositories().get(0).name(), is("repo"));
        // view all indices, including restricted ones
        final GetIndexResponse getIndexResponse = client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken))
                .admin().indices().prepareGetIndex().setIndices(randomFrom("_all", "*")).get();
        assertThat(Arrays.asList(getIndexResponse.indices()), containsInAnyOrder(INTERNAL_SECURITY_INDEX, ordinaryIndex));
        // create snapshot that includes restricted indices
        final CreateSnapshotResponse snapshotResponse = client()
                .filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).admin().cluster()
                .prepareCreateSnapshot("repo", "snap").setIndices(randomFrom("_all", "*")).setWaitForCompletion(true).get();
        assertThat(snapshotResponse.getSnapshotInfo().state(), is(SnapshotState.SUCCESS));
        assertThat(snapshotResponse.getSnapshotInfo().indices(), containsInAnyOrder(INTERNAL_SECURITY_INDEX, ordinaryIndex));
        // view snapshots for repo
        final GetSnapshotsResponse getSnapshotResponse = client()
                .filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).admin().cluster()
                .prepareGetSnapshots("repo").get();
        assertThat(getSnapshotResponse.getSnapshots().size(), is(1));
        assertThat(getSnapshotResponse.getSnapshots().get(0).snapshotId().getName(), is("snap"));
        assertThat(getSnapshotResponse.getSnapshots().get(0).indices(), containsInAnyOrder(INTERNAL_SECURITY_INDEX, ordinaryIndex));
    }

    public void testSnapshotUserRoleIsReserved() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
                () -> securityClient().preparePutRole("snapshot_user").get());
        assertThat(e.getMessage(), containsString("role [snapshot_user] is reserved and cannot be modified"));
        e = expectThrows(IllegalArgumentException.class,
                () -> securityClient().prepareDeleteRole("snapshot_user").get());
        assertThat(e.getMessage(), containsString("role [snapshot_user] is reserved and cannot be deleted"));
    }

    public void testSnapshotUserRoleUnathorizedForDestructiveActions() {
        // try search all
        ElasticsearchSecurityException unauthzException = expectThrows(ElasticsearchSecurityException.class,
                () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken))
                        .prepareSearch(randomFrom("_all", "*")).get());
        assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
        assertThat(unauthzException.getDetailedMessage(),
                containsString("action [indices:data/read/search] is unauthorized for user [snapshot_user]"));
        // try create index
        unauthzException = expectThrows(ElasticsearchSecurityException.class,
                () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).admin().indices().prepareCreate(ordinaryIndex + "2").get());
        assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
        assertThat(unauthzException.getDetailedMessage(),
                containsString("action [indices:admin/create] is unauthorized for user [snapshot_user]"));
        // try create another repo
        unauthzException = expectThrows(ElasticsearchSecurityException.class, () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken))
                .admin().cluster().preparePutRepository("some_other_repo").setType("fs").setSettings(Settings.builder().put("location", randomRepoPath())).get());
        assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
        assertThat(unauthzException.getDetailedMessage(),
                containsString("action [cluster:admin/repository/put] is unauthorized for user [snapshot_user]"));
        // try delete repo
        unauthzException = expectThrows(ElasticsearchSecurityException.class,
                () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).admin().cluster()
                        .prepareDeleteRepository("repo").get());
        assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
        assertThat(unauthzException.getDetailedMessage(),
                containsString("action [cluster:admin/repository/delete] is unauthorized for user [snapshot_user]"));
        // try fumble with snapshots
        unauthzException = expectThrows(ElasticsearchSecurityException.class, () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken))
                .admin().cluster().prepareRestoreSnapshot("repo", randomAlphaOfLength(4).toLowerCase(Locale.ROOT)).get());
        assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
        assertThat(unauthzException.getDetailedMessage(),
                containsString("action [cluster:admin/snapshot/restore] is unauthorized for user [snapshot_user]"));
        unauthzException = expectThrows(ElasticsearchSecurityException.class, () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken))
                .admin().cluster().prepareDeleteSnapshot("repo", randomAlphaOfLength(4).toLowerCase(Locale.ROOT)).get());
        assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
        assertThat(unauthzException.getDetailedMessage(),
                containsString("action [cluster:admin/snapshot/delete] is unauthorized for user [snapshot_user]"));
        // try destructive actions on all indices are unauthorized
        for (final String indexToTest : Arrays.asList(INTERNAL_SECURITY_INDEX, SECURITY_INDEX_NAME, ordinaryIndex)) {
            unauthzException = expectThrows(ElasticsearchSecurityException.class,
                    () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).prepareSearch(indexToTest).get());
            assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
            assertThat(unauthzException.getDetailedMessage(),
                    containsString("action [indices:data/read/search] is unauthorized for user [snapshot_user]"));

            unauthzException = expectThrows(ElasticsearchSecurityException.class,
                    () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).prepareGet(indexToTest, "doc", "1").get());
            assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
            assertThat(unauthzException.getDetailedMessage(),
                    containsString("action [indices:data/read/get] is unauthorized for user [snapshot_user]"));

            unauthzException = expectThrows(ElasticsearchSecurityException.class,
                    () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).prepareIndex(indexToTest, "doc").setSource("term", "val").get());
            assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
            assertThat(unauthzException.getDetailedMessage(),
                    containsString("action [indices:data/write/index] is unauthorized for user [snapshot_user]"));

            unauthzException = expectThrows(ElasticsearchSecurityException.class,
                    () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).prepareUpdate(indexToTest, "doc", "1").setDoc("term", "val").get());
            assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
            assertThat(unauthzException.getDetailedMessage(),
                    containsString("action [indices:data/write/update] is unauthorized for user [snapshot_user]"));

            unauthzException = expectThrows(ElasticsearchSecurityException.class,
                    () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).prepareDelete(indexToTest, "doc", "1").get());
            assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
            assertThat(unauthzException.getDetailedMessage(),
                    containsString("action [indices:data/write/delete] is unauthorized for user [snapshot_user]"));

            unauthzException = expectThrows(ElasticsearchSecurityException.class,
                    () -> client().filterWithHeader(Collections.singletonMap("Authorization", snapshotUserToken)).admin().indices().prepareDelete(indexToTest).get());
            assertThat(unauthzException.status(), is(RestStatus.FORBIDDEN));
            assertThat(unauthzException.getDetailedMessage(),
                    containsString("action [indices:admin/delete] is unauthorized for user [snapshot_user]"));
        }
    }

    // create repo
    // create another index
    // create user with snapshot_user role

    // test user can NOT get, search, index, change settings
    // test user can NOT put repositories
    // test user can NOT restore snapshot
    // test user can list all indices (including .security)
    // test user can snapshot all indices (including .security)
    
}
