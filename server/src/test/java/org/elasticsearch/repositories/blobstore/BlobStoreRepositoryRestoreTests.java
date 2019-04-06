/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.repositories.blobstore;

import org.apache.lucene.store.Directory;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.fs.FsRepository;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
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
            assertFalse(storeFiles.asMap().isEmpty());

            // close the shard
            closeShards(shard);

            // delete some random files in the store
            List<String> deletedFiles = randomSubsetOf(randomIntBetween(1, storeFiles.size() - 1), storeFiles.asMap().keySet());
            for (String deletedFile : deletedFiles) {
                Files.delete(shard.shardPath().resolveIndex().resolve(deletedFile));
            }

            // build a new shard using the same store directory as the closed shard
            ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(shard.routingEntry(),
                RecoverySource.ExistingStoreRecoverySource.INSTANCE);
            shard = newShard(
                    shardRouting,
                    shard.shardPath(),
                    shard.indexSettings().getIndexMetaData(),
                    null,
                    null,
                    new InternalEngineFactory(),
                    () -> {},
                    RetentionLeaseSyncer.EMPTY,
                    EMPTY_EVENT_LISTENER);

            // restore the shard
            recoverShardFromSnapshot(shard, snapshot, repository);

            // check that the shard is not corrupted
            TestUtil.checkIndex(shard.store().directory());

            // check that all files have been restored
            final Directory directory = shard.store().directory();
            final List<String> directoryFiles = Arrays.asList(directory.listAll());

            for (StoreFileMetaData storeFile : storeFiles) {
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

    public void testSnapshotWithConflictingName() throws IOException {
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
            final Snapshot snapshotWithSameName = new Snapshot(repository.getMetadata().name(), new SnapshotId(
                snapshot.getSnapshotId().getName(), "_uuid2"));
            IndexShardSnapshotFailedException isfe = expectThrows(IndexShardSnapshotFailedException.class,
                () -> snapshotShard(shard, snapshotWithSameName, repository));
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
        RepositoryMetaData repositoryMetaData = new RepositoryMetaData(randomAlphaOfLength(10), FsRepository.TYPE, settings);
        final FsRepository repository = new FsRepository(repositoryMetaData, createEnvironment(), xContentRegistry(), threadPool) {
            @Override
            protected void assertSnapshotOrGenericThread() {
                // eliminate thread name check as we create repo manually
            }
        };
        repository.start();
        return repository;
    }

    /** Create a {@link Environment} with random path.home and path.repo **/
    private Environment createEnvironment() {
        Path home = createTempDir();
        return TestEnvironment.newEnvironment(Settings.builder()
                                                      .put(Environment.PATH_HOME_SETTING.getKey(), home.toAbsolutePath())
            .put(Environment.PATH_REPO_SETTING.getKey(), home.resolve("repo").toAbsolutePath())
                                                      .build());
    }
}
