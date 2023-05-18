/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.Matcher;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class AbortedRestoreIT extends AbstractSnapshotIntegTestCase {

    public void testAbortedRestoreAlsoAbortFileRestores() throws Exception {
        internalCluster().startMasterOnlyNode();
        // small pool so we are able to fully block all of its threads later
        final String dataNode = internalCluster().startDataOnlyNode(SMALL_SNAPSHOT_POOL_SETTINGS);

        final String indexName = "test-abort-restore";
        createIndex(indexName, 1, 0);
        indexRandomDocs(indexName, scaledRandomIntBetween(10, 1_000));
        ensureGreen();
        forceMerge();

        final String repositoryName = "repository";
        createRepository(repositoryName, "mock");

        final String snapshotName = "snapshot";
        createFullSnapshot(repositoryName, snapshotName);
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> blocking all data nodes for repository [{}]", repositoryName);
        blockAllDataNodes(repositoryName);
        failReadsAllDataNodes(repositoryName);

        logger.info("--> starting restore");
        final ActionFuture<RestoreSnapshotResponse> future = clusterAdmin().prepareRestoreSnapshot(repositoryName, snapshotName)
            .setWaitForCompletion(true)
            .setIndices(indexName)
            .execute();

        assertBusy(() -> {
            final RecoveryResponse recoveries = indicesAdmin().prepareRecoveries(indexName)
                .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN)
                .setActiveOnly(true)
                .get();
            assertThat(recoveries.hasRecoveries(), is(true));
            final List<RecoveryState> shardRecoveries = recoveries.shardRecoveryStates().get(indexName);
            assertThat(shardRecoveries, hasSize(1));
            assertThat(future.isDone(), is(false));

            for (RecoveryState shardRecovery : shardRecoveries) {
                assertThat(shardRecovery.getRecoverySource().getType(), equalTo(RecoverySource.Type.SNAPSHOT));
                assertThat(shardRecovery.getStage(), equalTo(RecoveryState.Stage.INDEX));
            }
        });

        final ThreadPool.Info snapshotThreadPoolInfo = internalCluster().getInstance(ThreadPool.class, dataNode)
            .info(ThreadPool.Names.SNAPSHOT);
        assertThat(snapshotThreadPoolInfo.getMax(), greaterThan(0));

        logger.info("--> waiting for snapshot thread [max={}] pool to be full", snapshotThreadPoolInfo.getMax());
        waitForMaxActiveSnapshotThreads(dataNode, equalTo(snapshotThreadPoolInfo.getMax()));

        logger.info("--> aborting restore by deleting the index");
        assertAcked(client().admin().indices().prepareDelete(indexName));

        logger.info("--> unblocking repository [{}]", repositoryName);
        unblockAllDataNodes(repositoryName);

        logger.info("--> restore should have failed");
        final RestoreSnapshotResponse restoreSnapshotResponse = future.get();
        assertThat(restoreSnapshotResponse.getRestoreInfo().failedShards(), equalTo(1));
        assertThat(restoreSnapshotResponse.getRestoreInfo().successfulShards(), equalTo(0));

        logger.info("--> waiting for snapshot thread pool to be empty");
        waitForMaxActiveSnapshotThreads(dataNode, equalTo(0));
    }

    private static void waitForMaxActiveSnapshotThreads(final String node, final Matcher<Integer> matcher) throws Exception {
        assertBusy(() -> assertThat(snapshotThreadPoolStats(node).getActive(), matcher), 30L, TimeUnit.SECONDS);
    }
}
