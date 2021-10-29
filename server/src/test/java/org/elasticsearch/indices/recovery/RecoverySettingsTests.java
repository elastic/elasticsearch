/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_USE_SNAPSHOTS_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RecoverySettingsTests extends ESTestCase {
    public void testSnapshotDownloadPermitsAreNotGrantedWhenSnapshotsUseFlagIsFalse() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder()
                .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5)
                .put(INDICES_RECOVERY_USE_SNAPSHOTS_SETTING.getKey(), false)
                .build(),
            clusterSettings
        );

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));
    }

    public void testGrantsSnapshotDownloadPermitsUpToMaxPermits() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5).build(),
            clusterSettings
        );

        Releasable permit = recoverySettings.tryAcquireSnapshotDownloadPermits();
        assertThat(permit, is(notNullValue()));

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));

        permit.close();
        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(notNullValue()));
    }

    public void testSnapshotDownloadPermitCanBeDynamicallyUpdated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySettings recoverySettings = new RecoverySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5).build(),
            clusterSettings
        );

        Releasable permit = recoverySettings.tryAcquireSnapshotDownloadPermits();
        assertThat(permit, is(notNullValue()));

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 10).build()
        );

        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(notNullValue()));
        assertThat(recoverySettings.tryAcquireSnapshotDownloadPermits(), is(nullValue()));
        permit.close();
    }

    public void testMaxConcurrentSnapshotFileDownloadsPerNodeIsValidated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings settings = Settings.builder()
            .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS.getKey(), 10)
            .put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5)
            .build();
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> new RecoverySettings(settings, clusterSettings)
        );
        assertThat(
            exception.getMessage(),
            containsString(
                "[indices.recovery.max_concurrent_snapshot_file_downloads_per_node]=5 "
                    + "is less than [indices.recovery.max_concurrent_snapshot_file_downloads]=10"
            )
        );
    }
}
