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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.indices.recovery.BackgroundNetworkQos;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;

import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.BACKGROUND_QOS_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Snapshots and restores with both background QoS switches on: snapshot uploads run on their own thread pool and are counted, and
 * restores are not affected.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class SnapshotBackgroundQosIT extends AbstractSnapshotIntegTestCase {

    private static Settings qosSettings(boolean enabled) {
        return Settings.builder()
            .put(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey(), "1gb")
            .put(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey(), "2gb")
            .put(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey(), "2gb")
            .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), enabled)
            .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), enabled)
            .build();
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

    public void testSnapshotAndRestoreWithBothSwitchesOn() {
        internalCluster().startNodes(randomIntBetween(1, 3), qosSettings(true));
        snapshotAndRestore(true);
    }

    public void testFailedUploadsAreCounted() throws Exception {
        internalCluster().startNodes(randomIntBetween(1, 3), qosSettings(true));
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
        for (BackgroundNetworkQos qos : internalCluster().getDataNodeInstances(BackgroundNetworkQos.class)) {
            writeErrors += qos.getUploadWriteErrors();
            readErrors += qos.getUploadReadErrors();
        }
        assertThat(writeErrors, greaterThan(0L));
        assertEquals(0L, readErrors);
    }

    public void testSwitchingAdaptiveUploadConcurrencyWhileASnapshotRuns() throws Exception {
        internalCluster().startNodes(randomIntBetween(1, 3), qosSettings(false));
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
        for (BackgroundNetworkQos qos : internalCluster().getDataNodeInstances(BackgroundNetworkQos.class)) {
            assertBusy(() -> assertEquals(0, qos.getRunningUploadTasks()));
        }
    }

    public void testSnapshotAndRestoreWithBothSwitchesOff() {
        internalCluster().startNodes(randomIntBetween(1, 3), qosSettings(false));
        snapshotAndRestore(false);
    }
}
