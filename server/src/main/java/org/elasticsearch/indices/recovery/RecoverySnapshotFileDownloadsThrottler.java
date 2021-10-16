/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AdjustableSemaphore;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;

public class RecoverySnapshotFileDownloadsThrottler {
    public static final Setting<Integer> INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE =
        Setting.intSetting("indices.recovery.max_concurrent_snapshot_file_downloads_per_node",
            25,
            1,
            25,
            Setting.Property.Dynamic,
            Setting.Property.NodeScope
        );

    private final AdjustableSemaphore semaphore;

    public RecoverySnapshotFileDownloadsThrottler(Settings settings, ClusterSettings clusterSettings) {
        int maxSnapshotFileDownloadsPerNode = INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE.get(settings);
        this.semaphore = new AdjustableSemaphore(maxSnapshotFileDownloadsPerNode, true);
        clusterSettings.addSettingsUpdateConsumer(INDICES_RECOVERY_MAX_CONCURRENT_SNAPSHOT_FILE_DOWNLOADS_PER_NODE, this::updateMaxPermits);
    }

    @Nullable
    public Releasable tryAcquire(int count) {
        if (semaphore.tryAcquire(count)) {
            return Releasables.releaseOnce(() -> semaphore.release(count));
        }

        return null;
    }

    private void updateMaxPermits(int updatedMaxConcurrentSnapshotFileDownloadsPerNode) {
        semaphore.setMaxPermits(updatedMaxConcurrentSnapshotFileDownloadsPerNode);
    }
}
