/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.TestEnvironment;
import org.elasticsearch.index.engine.InternalEngineFactory;
import org.elasticsearch.index.seqno.RetentionLeaseSyncer;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotFailedException;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.indices.recovery.RecoverySettings;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.RepositoryData;
import org.elasticsearch.repositories.ShardGeneration;
import org.elasticsearch.repositories.ShardGenerations;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

/**
 * This class tests the behavior of {@link BlobStoreRepository} when it
 * restores a shard from a snapshot but some files with same names already
 * exist on disc.
 */
public class BlobStoreRepositoryRestoreTests extends IndexShardTestCase {

    /**
     * Restoring a snapshot that contains multiple files must succeed even when
     * some files already exist in the shard's store.
     */
    public void testRestoreSnapshotWithExistingFiles() throws IOException {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, "_doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            final Repository repository = createRepository();
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            snapshotShard(shard, snapshot, repository);

            // capture current store files
            final Store.MetadataSnapshot storeFiles = shard.snapshotStoreMetadata();
            assertFalse(storeFiles.fileMetadataMap().isEmpty());

            // close the shard
            closeShards(shard);

            // delete some random files in the store
            List<String> deletedFiles = randomSubsetOf(randomIntBetween(1, storeFiles.size() - 1), storeFiles.fileMetadataMap().keySet());
            for (String deletedFile : deletedFiles) {
                Files.delete(shard.shardPath().resolveIndex().resolve(deletedFile));
            }

            // build a new shard using the same store directory as the closed shard
            ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(
                shard.routingEntry(),
                RecoverySource.ExistingStoreRecoverySource.INSTANCE
            );
            shard = newShard(
                shardRouting,
                shard.shardPath(),
                shard.indexSettings().getIndexMetadata(),
                null,
                null,
                new InternalEngineFactory(),
                () -> {},
                RetentionLeaseSyncer.EMPTY,
                EMPTY_EVENT_LISTENER
            );

            // restore the shard
            recoverShardFromSnapshot(shard, snapshot, repository);

            // check that the shard is not corrupted
            TestUtil.checkIndex(shard.store().directory());

            // check that all files have been restored
            final Directory directory = shard.store().directory();
            final List<String> directoryFiles = Arrays.asList(directory.listAll());

            for (StoreFileMetadata storeFile : storeFiles) {
                String fileName = storeFile.name();
                assertTrue("File [" + fileName + "] does not exist in store directory", directoryFiles.contains(fileName));
                assertEquals(storeFile.length(), shard.store().directory().fileLength(fileName));
            }
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    shard.close("test", false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    public void testSnapshotWithConflictingName() throws Exception {
        final IndexId indexId = new IndexId(randomAlphaOfLength(10), UUIDs.randomBase64UUID());
        final ShardId shardId = new ShardId(indexId.getName(), indexId.getId(), 0);

        IndexShard shard = newShard(shardId, true);
        try {
            // index documents in the shards
            final int numDocs = scaledRandomIntBetween(1, 500);
            recoverShardFromStore(shard);
            for (int i = 0; i < numDocs; i++) {
                indexDoc(shard, "_doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            final Repository repository = createRepository();
            final Snapshot snapshot = new Snapshot(repository.getMetadata().name(), new SnapshotId(randomAlphaOfLength(10), "_uuid"));
            final ShardGeneration shardGen = snapshotShard(shard, snapshot, repository);
            assertNotNull(shardGen);
            final Snapshot snapshotWithSameName = new Snapshot(
                repository.getMetadata().name(),
                new SnapshotId(snapshot.getSnapshotId().getName(), "_uuid2")
            );
            final ShardGenerations shardGenerations = ShardGenerations.builder().put(indexId, 0, shardGen).build();
            PlainActionFuture.<Tuple<RepositoryData, SnapshotInfo>, Exception>get(
                f -> repository.finalizeSnapshot(
                    new FinalizeSnapshotContext(
                        shardGenerations,
                        RepositoryData.EMPTY_REPO_GEN,
                        Metadata.builder().put(shard.indexSettings().getIndexMetadata(), false).build(),
                        new SnapshotInfo(
                            snapshot,
                            shardGenerations.indices().stream().map(IndexId::getName).toList(),
                            Collections.emptyList(),
                            Collections.emptyList(),
                            null,
                            1L,
                            6,
                            Collections.emptyList(),
                            true,
                            Collections.emptyMap(),
                            0L,
                            Collections.emptyMap()
                        ),
                        Version.CURRENT,
                        f
                    )
                )
            );
            IndexShardSnapshotFailedException isfe = expectThrows(
                IndexShardSnapshotFailedException.class,
                () -> snapshotShard(shard, snapshotWithSameName, repository)
            );
            assertThat(isfe.getMessage(), containsString("Duplicate snapshot name"));
        } finally {
            if (shard != null && shard.state() != IndexShardState.CLOSED) {
                try {
                    shard.close("test", false);
                } finally {
                    IOUtils.close(shard.store());
                }
            }
        }
    }

    /** Create a {@link Repository} with a random name **/
    private Repository createRepository() {
        Settings settings = Settings.builder().put("location", randomAlphaOfLength(10)).build();
        RepositoryMetadata repositoryMetadata = new RepositoryMetadata(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final ClusterService clusterService = BlobStoreTestUtil.mockClusterService(repositoryMetadata);
        final FsRepository repository = new FsRepository(
            repositoryMetadata,
            createEnvironment(),
            xContentRegistry(),
            clusterService,
            MockBigArrays.NON_RECYCLING_INSTANCE,
            new RecoverySettings(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS))
        ) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        clusterService.addStateApplier(event -> repository.updateState(event.state()));
        // Apply state once to initialize repo properly like RepositoriesService would
        repository.updateState(clusterService.state());
        repository.start();
        return repository;
    }

    /** Create a {@link Environment} with random path.home and path.repo **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(
            Settings.builder()
                .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
                .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                .build()
        );
    }
}
