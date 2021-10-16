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

import static org.elasticsearch.indices.recovery.RecoverySnapshotFileDownloadsThrottler.INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RecoverySnapshotFileDownloadsThrottlerTests extends ESTestCase {
    public void testGrantsPermitsUpToMaxPermits() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySnapshotFileDownloadsThrottler recoverySnapshotFileDownloadsThrottler = new RecoverySnapshotFileDownloadsThrottler(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5).build(),
            clusterSettings
        );

        Releasable permit = recoverySnapshotFileDownloadsThrottler.tryAcquire(5);
        assertThat(permit, notNullValue());

        assertThat(recoverySnapshotFileDownloadsThrottler.tryAcquire(5), is(nullValue()));

        permit.close();
        assertThat(recoverySnapshotFileDownloadsThrottler.tryAcquire(5), is(notNullValue()));
    }

    public void testMaxPermitsCanBeDynamicallyUpdated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        RecoverySnapshotFileDownloadsThrottler recoverySnapshotFileDownloadsThrottler = new RecoverySnapshotFileDownloadsThrottler(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 5).build(),
            clusterSettings
        );

        Releasable permit = recoverySnapshotFileDownloadsThrottler.tryAcquire(5);
        assertThat(permit, notNullValue());

        assertThat(recoverySnapshotFileDownloadsThrottler.tryAcquire(5), is(nullValue()));
        clusterSettings.applySettings(
            Settings.builder().put(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.getKey(), 10).build()
        );

        assertThat(recoverySnapshotFileDownloadsThrottler.tryAcquire(5), is(notNullValue()));
        assertThat(recoverySnapshotFileDownloadsThrottler.tryAcquire(5), is(nullValue()));
        permit.close();
    }
}
