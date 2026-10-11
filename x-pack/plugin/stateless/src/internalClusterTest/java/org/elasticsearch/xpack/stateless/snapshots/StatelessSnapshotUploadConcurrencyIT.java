/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.UploadConcurrencyService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.snapshots.mockstore.MockRepository;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.indices.recovery.UploadConcurrencyService.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Snapshots and restores on stateless nodes, where adaptive upload concurrency applies: with it on, uploads run on their own thread
 * pool and are counted, and restores are not affected.
 */
public class StatelessSnapshotUploadConcurrencyIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(MockRepository.Plugin.class); // to block and fail uploads
        return List.copyOf(plugins);
    }

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    private static Settings nodeSettings(boolean adaptive) {
        return Settings.builder()
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), adaptive)
            .build();
    }

    private void startNodes(boolean adaptive) {
        startMasterOnlyNode(nodeSettings(adaptive));
        startIndexNodes(randomIntBetween(1, 3), nodeSettings(adaptive));
        ensureStableCluster(internalCluster().size());
    }

    private List<UploadConcurrencyService> uploadConcurrencyServices() {
        final List<UploadConcurrencyService> services = new ArrayList<>();
        for (String node : internalCluster().getNodeNames()) {
            services.add(internalCluster().getInstance(UploadConcurrencyService.class, node));
        }
        return services;
    }

    private List<MockRepository> mockRepositories(String repoName) {
        final List<MockRepository> repositories = new ArrayList<>();
        for (String node : internalCluster().getNodeNames()) {
            repositories.add(
                (MockRepository) internalCluster().getInstance(RepositoriesService.class, node).repository(ProjectId.DEFAULT, repoName)
            );
        }
        return repositories;
    }

    private String createIndexWithDocs(int docs) {
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(randomIntBetween(1, 3), 0).build());
        ensureGreen(indexName);
        indexDocs(indexName, docs);
        flush(indexName);
        return indexName;
    }

    private void snapshotAndRestore(boolean uploadsOnTheirOwnPool) {
        final String repoName = randomIdentifier();
        createRepository(repoName, "fs");
        final int docs = between(50, 100);
        final String indexName = createIndexWithDocs(docs);
        createSnapshot(repoName, "snap", List.of(indexName), List.of());

        final String restoredIndexName = "restored-" + indexName;
        final var restoreResponse = clusterAdmin().prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap")
            .setRenamePattern(indexName)
            .setRenameReplacement(restoredIndexName)
            .setWaitForCompletion(true)
            .get();
        assertThat(restoreResponse.getRestoreInfo().totalShards(), greaterThan(0));
        ensureGreen(restoredIndexName);
        assertThat(
            safeGet(indicesAdmin().prepareStats(restoredIndexName).setDocs(true).execute()).getTotal().getDocs().getCount(),
            equalTo((long) docs)
        );

        long uploads = 0;
        for (String node : internalCluster().getNodeNames()) {
            for (var stats : internalCluster().getInstance(ThreadPool.class, node).stats()) {
                if (stats.name().equals(ThreadPool.Names.SNAPSHOT_UPLOAD)) {
                    uploads += stats.completed();
                }
            }
        }
        if (uploadsOnTheirOwnPool) {
            assertThat(uploads, greaterThan(0L));
        } else {
            assertThat(uploads, equalTo(0L));
        }
    }

    public void testSnapshotAndRestoreWithAdaptiveOn() {
        startNodes(true);
        for (var service : uploadConcurrencyServices()) {
            assertTrue(service.isAdaptiveUploadConcurrencyEnabled());
        }
        snapshotAndRestore(true);
    }

    public void testSnapshotAndRestoreWithAdaptiveOff() {
        startNodes(false);
        for (var service : uploadConcurrencyServices()) {
            assertFalse(service.isAdaptiveUploadConcurrencyEnabled());
        }
        snapshotAndRestore(false);
    }

    public void testFailedUploadsAreCounted() {
        startNodes(true);
        final String repoName = randomIdentifier();
        createRepository(
            logger,
            repoName,
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                // every data file fails to write, as often as the repository allows
                .put("random_data_file_io_exception_rate", 1.0)
                .put("max_failure_number", Long.MAX_VALUE),
            true
        );
        final String indexName = createIndexWithDocs(between(50, 100));
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap").setIndices(indexName).setWaitForCompletion(true).get();

        long writeErrors = 0;
        long readErrors = 0;
        for (UploadConcurrencyService service : uploadConcurrencyServices()) {
            writeErrors += service.getUploadWriteErrors();
            readErrors += service.getUploadReadErrors();
        }
        assertThat(writeErrors, greaterThan(0L));
        assertThat(readErrors, equalTo(0L));
    }

    public void testSwitchingAdaptiveUploadConcurrencyWhileASnapshotRuns() throws Exception {
        startNodes(false);
        final String repoName = randomIdentifier();
        createRepository(repoName, "mock");
        final String indexName = createIndexWithDocs(between(50, 100));
        final var repositories = mockRepositories(repoName);
        repositories.forEach(MockRepository::blockOnDataFiles);
        final var snapshot = clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repoName, "snap")
            .setIndices(indexName)
            .setWaitForCompletion(true)
            .execute();
        assertBusy(() -> assertTrue(repositories.stream().anyMatch(MockRepository::blocked)));

        // while uploads are running, on and off again: they go on, and the snapshot is complete
        for (boolean adaptive : new boolean[] { true, false, true, false }) {
            updateClusterSettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), adaptive));
        }
        repositories.forEach(MockRepository::unblock);
        final CreateSnapshotResponse response = safeGet(snapshot);
        assertThat(response.getSnapshotInfo().state(), equalTo(SnapshotState.SUCCESS));

        // every task has given back what it took
        for (UploadConcurrencyService service : uploadConcurrencyServices()) {
            assertBusy(() -> assertThat(service.getRunningUploadTasks(), equalTo(0)));
        }
    }
}
