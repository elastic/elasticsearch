/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.snapshots.restore.RestoreSnapshotResponse;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshotsIntegritySuppressor;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.AbstractSnapshotIntegTestCase;
import org.elasticsearch.snapshots.SnapshotState;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;

public class BlobStoreCorruptionIT extends AbstractSnapshotIntegTestCase {

    private static final Logger logger = LogManager.getLogger(BlobStoreCorruptionIT.class);

    @Before
    public void suppressConsistencyCheck() {
        disableRepoConsistencyCheck("testing corruption detection involves breaking the repo");
    }

    public void testCorruptionDetection() throws Exception {
        final var repositoryName = randomIdentifier();
        final var indexName = randomIdentifier();
        final var snapshotName = randomIdentifier();
        final var repositoryRootPath = randomRepoPath();

        createRepository(repositoryName, FsRepository.TYPE, repositoryRootPath);
        createIndexWithRandomDocs(indexName, between(1, 100));
        flushAndRefresh(indexName);
        createSnapshot(repositoryName, snapshotName, List.of(indexName));

        final var corruptedFile = BlobStoreCorruptionUtils.corruptRandomFile(repositoryRootPath);
        final var corruptedFileType = RepositoryFileType.getRepositoryFileType(repositoryRootPath, corruptedFile);
        final var corruptionDetectors = new ArrayList<CheckedConsumer<ActionListener<Exception>, ?>>();

        // detect corruption by listing the snapshots
        if (corruptedFileType == RepositoryFileType.SNAPSHOT_INFO) {
            corruptionDetectors.add(exceptionListener -> {
                logger.info("--> listing snapshots");
                client().admin()
                    .cluster()
                    .prepareGetSnapshots(TEST_REQUEST_TIMEOUT, repositoryName)
                    .execute(ActionTestUtils.assertNoSuccessListener(exceptionListener::onResponse));
            });
        }

        // detect corruption by taking another snapshot
        if (corruptedFileType == RepositoryFileType.SHARD_GENERATION) {
            corruptionDetectors.add(exceptionListener -> {
                logger.info("--> taking another snapshot");
                client().admin()
                    .cluster()
                    .prepareCreateSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, randomIdentifier())
                    .setWaitForCompletion(true)
                    .execute(exceptionListener.map(createSnapshotResponse -> {
                        assertNotEquals(SnapshotState.SUCCESS, createSnapshotResponse.getSnapshotInfo().state());
                        return new ElasticsearchException("create-snapshot failed as expected");
                    }));
            });
        }

        // detect corruption by restoring the snapshot
        switch (corruptedFileType) {
            case SNAPSHOT_INFO, GLOBAL_METADATA, INDEX_METADATA -> corruptionDetectors.add(exceptionListener -> {
                logger.info("--> restoring snapshot");
                client().admin()
                    .cluster()
                    .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
                    .setRestoreGlobalState(corruptedFileType == RepositoryFileType.GLOBAL_METADATA || randomBoolean())
                    .setWaitForCompletion(true)
                    .execute(ActionTestUtils.assertNoSuccessListener(exceptionListener::onResponse));
            });
            case SHARD_SNAPSHOT_INFO, SHARD_DATA -> corruptionDetectors.add(exceptionListener -> {
                logger.info("--> restoring snapshot and checking for failed shards");
                SubscribableListener
                    // if shard-level data is corrupted then the overall restore succeeds but the shard recoveries fail
                    .<AcknowledgedResponse>newForked(l -> client().admin().indices().prepareDelete(indexName).execute(l))
                    .andThenAccept(ElasticsearchAssertions::assertAcked)

                    .<RestoreSnapshotResponse>andThen(
                        l -> client().admin()
                            .cluster()
                            .prepareRestoreSnapshot(TEST_REQUEST_TIMEOUT, repositoryName, snapshotName)
                            .setRestoreGlobalState(randomBoolean())
                            .setWaitForCompletion(true)
                            .execute(l)
                    )

                    .addListener(exceptionListener.map(restoreSnapshotResponse -> {
                        assertNotEquals(0, restoreSnapshotResponse.getRestoreInfo().failedShards());
                        return new ElasticsearchException("post-restore recoveries failed as expected");
                    }));
            });
        }

        try (var ignored = new BlobStoreIndexShardSnapshotsIntegritySuppressor()) {
            final var exception = safeAwait(randomFrom(corruptionDetectors));
            logger.info(Strings.format("--> corrupted [%s] and caught exception", corruptedFile), exception);
        }
    }
}
