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
import org.elasticsearch.indices.recovery.UploadConcurrencyService;
import org.elasticsearch.injection.guice.ConfigurationException;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;

import static org.elasticsearch.indices.recovery.UploadConcurrencyService.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Adaptive upload concurrency is for stateless nodes only. The nodes of these tests are stateful: whatever the setting says, there is
 * no upload concurrency service, and shard snapshots run on the {@code snapshot} pool, as they always have. The stateless nodes are
 * covered by {@code StatelessSnapshotUploadConcurrencyIT}.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class SnapshotUploadConcurrencyIT extends AbstractSnapshotIntegTestCase {

    public void testAdaptiveIsIgnoredOnStatefulNodes() {
        final var adaptive = Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true).build();
        internalCluster().startNodes(randomIntBetween(1, 3), adaptive);
        updateClusterSettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), randomBoolean()));

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

        // no node has the service that the repositories would have used, and nothing ran on the pool for it
        long uploads = 0;
        for (String node : internalCluster().getNodeNames()) {
            expectThrows(ConfigurationException.class, () -> internalCluster().getInstance(UploadConcurrencyService.class, node));
            for (var stats : internalCluster().getInstance(ThreadPool.class, node).stats()) {
                if (stats.name().equals(ThreadPool.Names.SNAPSHOT_UPLOAD)) {
                    uploads += stats.completed();
                }
            }
        }
        assertEquals(0L, uploads);
    }
}
