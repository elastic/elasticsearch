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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Adaptive upload concurrency is for stateless nodes only, where the stateless plugin adds its settings and its thread pool. The nodes
 * of this test are stateful: they have none of it, and shard snapshots run on the {@code snapshot} pool, as they always have. The
 * stateless nodes are covered by {@code StatelessSnapshotUploadConcurrencyIT}.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 0, scope = ESIntegTestCase.Scope.TEST)
public class SnapshotUploadConcurrencyIT extends AbstractSnapshotIntegTestCase {

    public void testStatefulNodesHaveNoUploadConcurrency() {
        internalCluster().startNodes(randomIntBetween(1, 3));

        final String key = ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey();
        final var e = expectThrows(IllegalArgumentException.class, () -> updateClusterSettings(Settings.builder().put(key, true)));
        assertThat(e.getMessage(), containsString("setting [" + key + "], not recognized"));

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

        // the uploads ran on the snapshot pool, and there is no pool or service for adaptive uploads on any node
        long uploadsOnSnapshotPool = 0;
        for (String node : internalCluster().getNodeNames()) {
            expectThrows(ConfigurationException.class, () -> internalCluster().getInstance(UploadConcurrencyService.class, node));
            final ThreadPool threadPool = internalCluster().getInstance(ThreadPool.class, node);
            assertNull(threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD));
            for (var stats : threadPool.stats()) {
                assertNotEquals(ThreadPool.Names.SNAPSHOT_UPLOAD, stats.name());
                if (stats.name().equals(ThreadPool.Names.SNAPSHOT)) {
                    uploadsOnSnapshotPool += stats.completed();
                }
            }
        }
        assertThat(uploadsOnSnapshotPool, greaterThan(0L));
    }
}
