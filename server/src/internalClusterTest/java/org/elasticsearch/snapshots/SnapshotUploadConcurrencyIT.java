/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.env.Environment;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.indices.recovery.UploadConcurrencyService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.plugins.RepositoryPlugin;
import org.elasticsearch.repositories.RepositoriesMetrics;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotMetrics;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.indices.recovery.UploadConcurrencyService.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Snapshots and restores with adaptive upload concurrency on: snapshot uploads run on their own thread pool and are counted, and
 * restores are not affected. Adaptive upload concurrency only applies on stateless nodes, which these tests do not run on, so
 * {@link StatelessUploadConcurrencyPlugin} puts the service of a stateless node in the place of the node's own.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class SnapshotUploadConcurrencyIT extends AbstractSnapshotIntegTestCase {

    private static final Setting<Boolean> STATELESS_UPLOAD_CONCURRENCY_SETTING = Setting.boolSetting(
        "test.stateless_upload_concurrency",
        false,
        Setting.Property.NodeScope
    );

    /**
     * On a node with {@link #STATELESS_UPLOAD_CONCURRENCY_SETTING}, gives the repositories an {@link UploadConcurrencyService} that
     * treats the node as stateless, as repositories take it from the {@link RecoverySettings} that they get from the node.
     */
    public static class StatelessUploadConcurrencyPlugin extends Plugin implements RepositoryPlugin {
        private final List<UploadConcurrencyService> services = new ArrayList<>();

        @Override
        public List<Setting<?>> getSettings() {
            return List.of(STATELESS_UPLOAD_CONCURRENCY_SETTING);
        }

        @Override
        public Map<String, Repository.Factory> getRepositories(
            Environment env,
            NamedXContentRegistry namedXContentRegistry,
            ClusterService clusterService,
            BigArrays bigArrays,
            RecoverySettings recoverySettings,
            RepositoriesMetrics repositoriesMetrics,
            SnapshotMetrics snapshotMetrics
        ) {
            if (STATELESS_UPLOAD_CONCURRENCY_SETTING.get(env.settings())) {
                final var service = new UploadConcurrencyService(clusterService.getClusterSettings(), clusterService.threadPool(), true);
                service.start();
                services.add(service);
                recoverySettings.setUploadConcurrencyService(service);
            }
            return Collections.emptyMap();
        }

        @Override
        public void close() throws IOException {
            services.forEach(UploadConcurrencyService::close);
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(StatelessUploadConcurrencyPlugin.class);
        return plugins;
    }

    private static Settings nodeSettings(boolean adaptive, boolean stateless) {
        return Settings.builder()
            .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), adaptive)
            .put(STATELESS_UPLOAD_CONCURRENCY_SETTING.getKey(), stateless)
            .build();
    }

    private List<UploadConcurrencyService> uploadConcurrencyServices() {
        final List<UploadConcurrencyService> services = new ArrayList<>();
        for (PluginsService pluginsService : internalCluster().getDataNodeInstances(PluginsService.class)) {
            pluginsService.filterPlugins(StatelessUploadConcurrencyPlugin.class).forEach(plugin -> services.addAll(plugin.services));
        }
        return services;
    }

    private void snapshotAndRestore(boolean uploadsOnTheirOwnPool) {
        createRepository("test-repo", "fs");
        createIndexWithRandomDocs("test-idx", 50);
        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));
        final RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot(
            TEST_REQUEST_TIMEOUT,
            "test-repo",
            "test-snap"
        ).setRenamePattern("test-").setRenameReplacement("test2-").setWaitForCompletion(true).get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test2-idx", 50L);

        long uploads = 0;
        for (ThreadPool threadPool : internalCluster().getDataNodeInstances(ThreadPool.class)) {
            for (var stats : threadPool.stats()) {
                if (stats.name().equals(ThreadPool.Names.SNAPSHOT_UPLOAD)) {
                    uploads += stats.completed();
                }
            }
        }
        if (uploadsOnTheirOwnPool) {
            assertThat(uploads, greaterThan(0L));
        } else {
            assertEquals(0L, uploads);
        }
    }

    public void testSnapshotAndRestoreWithAdaptiveOn() {
        internalCluster().startNodes(randomIntBetween(1, 3), nodeSettings(true, true));
        snapshotAndRestore(true);
    }

    public void testFailedUploadsAreCounted() throws Exception {
        internalCluster().startNodes(randomIntBetween(1, 3), nodeSettings(true, true));
        createRepository(
            "test-repo",
            "mock",
            Settings.builder()
                .put("location", randomRepoPath())
                // every data file fails to write, as often as the repository allows
                .put("random_data_file_io_exception_rate", 1.0)
                .put("max_failure_number", Long.MAX_VALUE)
        );
        createIndexWithRandomDocs("test-idx", 50);
        clusterAdmin().prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, "test-repo", "test-snap")
            .setIndices("test-idx")
            .setWaitForCompletion(true)
            .get();

        long writeErrors = 0;
        long readErrors = 0;
        for (UploadConcurrencyService service : uploadConcurrencyServices()) {
            writeErrors += service.getUploadWriteErrors();
            readErrors += service.getUploadReadErrors();
        }
        assertThat(writeErrors, greaterThan(0L));
        assertEquals(0L, readErrors);
    }

    public void testSwitchingAdaptiveUploadConcurrencyWhileASnapshotRuns() throws Exception {
        internalCluster().startNodes(randomIntBetween(1, 3), nodeSettings(false, true));
        createRepository("test-repo", "mock");
        createIndexWithRandomDocs("test-idx", 50);
        blockAllDataNodes("test-repo");
        final var snapshot = startFullSnapshot("test-repo", "test-snap");
        waitForBlockOnAnyDataNode("test-repo");

        // while uploads are running, on and off again: they go on, and the snapshot is complete
        for (boolean adaptive : new boolean[] { true, false, true, false }) {
            updateClusterSettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), adaptive));
        }
        unblockAllDataNodes("test-repo");
        assertSuccessful(snapshot);

        // every task has given back what it took
        for (UploadConcurrencyService service : uploadConcurrencyServices()) {
            assertBusy(() -> assertEquals(0, service.getRunningUploadTasks()));
        }
    }

    public void testSnapshotAndRestoreWithAdaptiveOff() {
        internalCluster().startNodes(randomIntBetween(1, 3), nodeSettings(false, true));
        snapshotAndRestore(false);
    }

    public void testAdaptiveIsIgnoredOnStatefulNodes() {
        internalCluster().startNodes(randomIntBetween(1, 3), nodeSettings(true, false));
        for (UploadConcurrencyService service : internalCluster().getDataNodeInstances(UploadConcurrencyService.class)) {
            assertFalse(service.isAdaptiveUploadConcurrencyEnabled());
        }
        snapshotAndRestore(false);
    }
}
