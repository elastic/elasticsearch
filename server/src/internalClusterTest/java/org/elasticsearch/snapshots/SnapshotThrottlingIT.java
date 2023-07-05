/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.repositories.blobstore.BlobStoreRepository;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.Collections;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING;
import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class SnapshotThrottlingIT extends AbstractSnapshotIntegTestCase {

    private Tuple<Long, Long> testThrottledRepository(String maxSnapshotBytesPerSec, String maxRestoreBytesPerSec, boolean compressRepo) {
        logger.info(
            "--> testing throttled repository (maxSnapshotBytesPerSec=[{}], maxRestoreBytesPerSec=[{}], compressRepo=[{}])",
            maxSnapshotBytesPerSec,
            maxRestoreBytesPerSec,
            compressRepo
        );
        createRepository(
            "test-repo",
            "fs",
            Settings.builder()
                .put("location", randomRepoPath())
                .put("compress", compressRepo)
                .put("chunk_size", randomIntBetween(1000, 4000), ByteSizeUnit.BYTES)
                .put("max_snapshot_bytes_per_sec", maxSnapshotBytesPerSec)
                .put("max_restore_bytes_per_sec", maxRestoreBytesPerSec)
        );
        createSnapshot("test-repo", "test-snap", Collections.singletonList("test-idx"));
        RestoreSnapshotResponse restoreSnapshotResponse = clusterAdmin().prepareRestoreSnapshot("test-repo", "test-snap")
            .setRenamePattern("test-")
            .setRenameReplacement("test2-")
            .setWaitForCompletion(true)
            .execute()
            .actionGet();
        assertThat(restoreSnapshotResponse.getRestoreInfo().totalShards(), greaterThan(0));
        assertDocCount("test-idx", 50L);
        long snapshotPause = 0L;
        long restorePause = 0L;
        for (RepositoriesService repositoriesService : internalCluster().getDataNodeInstances(RepositoriesService.class)) {
            snapshotPause += repositoriesService.repository("test-repo").getSnapshotThrottleTimeInNanos();
            restorePause += repositoriesService.repository("test-repo").getRestoreThrottleTimeInNanos();
        }
        cluster().wipeIndices("test2-idx");
        logger.warn("--> tested throttled repository with snapshot pause [{}] and restore pause [{}]", snapshotPause, restorePause);
        return new Tuple<>(snapshotPause, restorePause);
    }

    public void testThrottling() throws Exception {
        boolean compressRepo = randomBoolean();
        boolean throttleSnapshotViaRecovery = randomBoolean();
        boolean throttleRestoreViaRecovery = throttleSnapshotViaRecovery || randomBoolean();

        Settings.Builder primaryNodeSettings = Settings.builder()
            .put(
                INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(),
                (throttleSnapshotViaRecovery || throttleRestoreViaRecovery) ? "25k" : "0"
            );

        if (throttleSnapshotViaRecovery) {
            primaryNodeSettings = primaryNodeSettings.put(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey(), "25k")
                .put(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey(), "25k")
                .put(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey(), "25k");
        }
        final String primaryNode = internalCluster().startNode(primaryNodeSettings);

        logger.info("--> create index");
        createIndexWithRandomDocs("test-idx", 50);

        long snapshotPauseViaRecovery = 0L;
        long restorePauseViaRecovery = 0L;

        // Throttle snapshot and/or restore only via recovery 25kb rate limit
        if (throttleSnapshotViaRecovery || throttleRestoreViaRecovery) {
            logger.info("--> testing throttling via recovery settings only");
            Tuple<Long, Long> pauses = testThrottledRepository("0", "0", compressRepo);
            snapshotPauseViaRecovery += pauses.v1();
            restorePauseViaRecovery += pauses.v2();
            if (throttleSnapshotViaRecovery) assertThat(snapshotPauseViaRecovery, greaterThan(0L));
            if (throttleRestoreViaRecovery) assertThat(restorePauseViaRecovery, greaterThan(0L));
        }

        // Throttle snapshot and/or restore separately with 5kb rate limit, which is much less than half of the potential recovery rate
        // limit. For this reason, we assert that the separately throttled speeds incur a pause time which is at least double of the
        // pause time detected in the recovery-only throttling run above.
        boolean throttleSnapshot = randomBoolean();
        boolean throttleRestore = randomBoolean();

        if (throttleSnapshot || throttleRestore) {
            Tuple<Long, Long> pauses = testThrottledRepository(throttleSnapshot ? "5k" : "0", throttleRestore ? "5k" : "0", compressRepo);
            long snapshotPause = pauses.v1();
            long restorePause = pauses.v2();
            if (throttleSnapshot) {
                assertThat(snapshotPause, greaterThan(0L));
                if (throttleSnapshotViaRecovery) assertThat(snapshotPause, greaterThan(snapshotPauseViaRecovery * 2));
            }
            if (throttleRestore) {
                assertThat(restorePause, greaterThan(0L));
                if (throttleRestoreViaRecovery) assertThat(restorePause, greaterThan(restorePauseViaRecovery * 2));
            }
        }
    }

    @TestLogging(
        reason = "testing warning that speed is over recovery speed",
        value = "org.elasticsearch.repositories.blobstore.BlobStoreRepository:WARN"
    )
    public void testWarningSpeedOverRecovery() throws Exception {
        boolean nodeBandwidthSettingsSet = randomBoolean();
        Settings.Builder primaryNodeSettings = Settings.builder().put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "100m");
        if (nodeBandwidthSettingsSet) {
            primaryNodeSettings = primaryNodeSettings.put(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey(), "100m")
                .put(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey(), "100m")
                .put(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey(), "100m");
        }
        final String primaryNode = internalCluster().startNode(primaryNodeSettings);

        final MockLogAppender mockLogAppender = new MockLogAppender();
        try {
            mockLogAppender.start();
            Loggers.addAppender(LogManager.getLogger(BlobStoreRepository.class), mockLogAppender);

            MockLogAppender.EventuallySeenEventExpectation snapshotExpectation = new MockLogAppender.EventuallySeenEventExpectation(
                "snapshot speed over recovery speed",
                "org.elasticsearch.repositories.blobstore.BlobStoreRepository",
                Level.WARN,
                "repository [test-repo] has a rate limit [max_snapshot_bytes_per_sec=1gb] per second which is above "
                    + "the effective recovery rate limit [indices.recovery.max_bytes_per_sec=100mb] per second, thus the repository "
                    + "rate limit will be superseded by the recovery rate limit"
            );
            if (nodeBandwidthSettingsSet) snapshotExpectation.setExpectSeen();
            mockLogAppender.addExpectation(snapshotExpectation);

            MockLogAppender.SeenEventExpectation restoreExpectation = new MockLogAppender.SeenEventExpectation(
                "snapshot restore speed over recovery speed",
                "org.elasticsearch.repositories.blobstore.BlobStoreRepository",
                Level.WARN,
                "repository [test-repo] has a rate limit [max_restore_bytes_per_sec=2gb] per second which is above "
                    + "the effective recovery rate limit [indices.recovery.max_bytes_per_sec=100mb] per second, thus the repository "
                    + "rate limit will be superseded by the recovery rate limit"
            );
            mockLogAppender.addExpectation(restoreExpectation);

            createRepository(
                "test-repo",
                "fs",
                Settings.builder()
                    .put("location", randomRepoPath())
                    .put("max_snapshot_bytes_per_sec", "1g")
                    .put("max_restore_bytes_per_sec", "2g")
            );

            deleteRepository("test-repo");
            mockLogAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(LogManager.getLogger(BlobStoreRepository.class), mockLogAppender);
            mockLogAppender.stop();
        }
    }

}
