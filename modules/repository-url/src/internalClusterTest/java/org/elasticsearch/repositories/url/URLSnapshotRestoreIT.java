/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.url;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsResponse;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.plugin.repository.url.URLRepositoryPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.ESIntegTestCase;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class URLSnapshotRestoreIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(URLRepositoryPlugin.class);
    }

    public void testUrlRepository() throws Exception {
        Client client = client();

        logger.info("-->  creating repository");
        Path repositoryLocation = randomRepoPath();
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "test-repo")
                .setType(FsRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(FsRepository.LOCATION_SETTING.getKey(), repositoryLocation)
                        .put(FsRepository.COMPRESS_SETTING.getKey(), randomBoolean())
                        .put(FsRepository.CHUNK_SIZE_SETTING.getKey(), randomIntBetween(100, 1000), ByteSizeUnit.BYTES)
                )
        );

        createIndex("test-idx");
        ensureGreen();

        logger.info("--> indexing some data");
        for (int i = 0; i < 100; i++) {
            indexDoc("test-idx", Integer.toString(i), "foo", "bar" + i);
        }
        refresh();
        assertHitCount(client.prepareSearch("test-idx").setSize(0), 100);

        logger.info("--> snapshot");
        CreateSnapshotResponse createSnapshotResponse = client.admin()
            .cluster()
            .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .get();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), greaterThan(0));
        int actualTotalShards = createSnapshotResponse.getSnapshotInfo().totalShards();
        assertThat(createSnapshotResponse.getSnapshotInfo().successfulShards(), equalTo(actualTotalShards));

        SnapshotState state = client.admin()
            .cluster()
            .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "test-repo")
            .setSnapshots("test-snap")
            .get()
            .getSnapshots()
            .get(0)
            .state();
        assertThat(state, equalTo(SnapshotState.SUCCESS));

        logger.info("--> delete index");
        cluster().wipeIndices("test-idx");

        logger.info("--> create read-only URL repository");
        assertAcked(
            client.admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "url-repo")
                .setType(URLRepository.TYPE)
                .setSettings(
                    Settings.builder()
                        .put(URLRepository.URL_SETTING.getKey(), repositoryLocation.toUri().toURL().toString())
                        .put("list_directories", randomBoolean())
                )
        );
        logger.info("--> restore index after deletion");
        RestoreSnapshotResponse restoreSnapshotResponse = client.admin()
            .cluster()
            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, "url-repo", "test-snap")
            .setWaitForCompletion(true)
            .setIndices("test-idx")
            .get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));

        assertHitCount(client.prepareSearch("test-idx").setSize(0), 100);

        logger.info("--> list available shapshots");
        GetSnapshotsResponse getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(1));

        logger.info("--> delete snapshot");
        AcknowledgedResponse deleteSnapshotResponse = client.admin()
            .cluster()
            .prepareDeleteSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .get();
        assertAcked(deleteSnapshotResponse);

        logger.info("--> list available shapshot again, no snapshots should be returned");
        getSnapshotsResponse = client.admin().cluster().prepareGetSnapshots(TEST_REQUEST_TIMEOUT, "url-repo").get();
        assertThat(getSnapshotsResponse.getSnapshots().size(), equalTo(0));
    }

    public void testUrlRepositoryPermitsShutdown() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutRepository(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT, "url-repo")
                .setType(URLRepository.TYPE)
                .setVerify(false)
                .setSettings(Settings.builder().put(URLRepository.URL_SETTING.getKey(), "http://localhost/"))
        );

        internalCluster().fullRestart(); // just checking that URL repositories don't block node shutdown
    }
}
