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
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.BlobMetaData;
import org.elasticsearch.common.blobstore.BlobPath;
import org.elasticsearch.common.blobstore.BlobStore;
import org.elasticsearch.common.blobstore.support.PlainBlobMetaData;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.routing.RecoverySource.StoreRecoverySource.EXISTING_STORE_INSTANCE;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;

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
                indexDoc(shard, "doc", Integer.toString(i));
                if (rarely()) {
                    flushShard(shard, false);
                }
            }
            assertDocCount(shard, numDocs);

            // snapshot the shard
            final BlobStoreRepository repository = createRepository();
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
            ShardRouting shardRouting = ShardRoutingHelper.initWithSameId(shard.routingEntry(), EXISTING_STORE_INSTANCE);
            shard = newShard(shardRouting, shard.shardPath(), shard.indexSettings().getIndexMetaData(), null, null, () -> {});

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

    /** Recover a shard from a snapshot using a given repository **/
    private void recoverShardFromSnapshot(final IndexShard shard,
                                          final Snapshot snapshot,
                                          final BlobStoreRepository repository) throws IOException {
        final Version version = Version.CURRENT;
        final ShardId shardId = shard.shardId();
        final String index = shard.shardId().getIndexName();
        final IndexId indexId = new IndexId(index, UUIDs.randomBase64UUID());
        final DiscoveryNode node = new DiscoveryNode(randomAlphaOfLength(25), buildNewFakeTransportAddress(), version);
        final RecoverySource.SnapshotRecoverySource recoverySource = new RecoverySource.SnapshotRecoverySource(snapshot, version, index);
        final ShardRouting shardRouting = newShardRouting(shardId, node.getId(), true, recoverySource, ShardRoutingState.INITIALIZING);

        shard.markAsRecovering("from snapshot", new RecoveryState(shardRouting, node, null));
        repository.restoreShard(shard, snapshot.getSnapshotId(), version, indexId, shard.shardId(), shard.recoveryState());
    }

    /** Snapshot a shard using a given repository **/
    private void snapshotShard(final IndexShard shard,
                                                   final Snapshot snapshot,
                                                   final BlobStoreRepository repository) throws IOException {
        final IndexShardSnapshotStatus snapshotStatus = new IndexShardSnapshotStatus();
        try (Engine.IndexCommitRef indexCommitRef = shard.acquireIndexCommit(true)) {
            Index index = shard.shardId().getIndex();
            IndexId indexId = new IndexId(index.getName(), index.getUUID());

            repository.snapshotShard(shard, snapshot.getSnapshotId(), indexId, indexCommitRef.getIndexCommit(), snapshotStatus);
        }
        assertEquals(IndexShardSnapshotStatus.Stage.DONE, snapshotStatus.stage());
        assertEquals(shard.snapshotStoreMetadata().size(), snapshotStatus.numberOfFiles());
        assertNull(snapshotStatus.failure());
    }


    /**
     * A {@link BlobStoreRepository} implementation that works in memory.
     *
     * It implements only the methods required by the tests and is not thread safe.
     */
    class MemoryBlobStoreRepository extends BlobStoreRepository {

        private final Map<String, byte[]> files = new HashMap<>();

        MemoryBlobStoreRepository(final RepositoryMetaData metadata, final Settings settings, final NamedXContentRegistry registry) {
            super(metadata, settings, registry);
        }

        @Override
        protected BlobStore blobStore() {
            return new BlobStore() {
                @Override
                public BlobContainer blobContainer(BlobPath path) {
                    return new BlobContainer() {
                        @Override
                        public BlobPath path() {
                            return new BlobPath();
                        }

                        @Override
                        public boolean blobExists(String blobName) {
                            return files.containsKey(blobName);
                        }

                        @Override
                        public InputStream readBlob(String blobName) throws IOException {
                            if (blobExists(blobName) == false) {
                                throw new FileNotFoundException(blobName);
                            }
                            return new ByteArrayInputStream(files.get(blobName));
                        }

                        @Override
                        public void writeBlob(String blobName, InputStream in, long blobSize) throws IOException {
                            try (ByteArrayOutputStream out = new ByteArrayOutputStream((int) blobSize)) {
                                Streams.copy(in, out);
                                files.put(blobName, out.toByteArray());
                            }
                        }

                        @Override
                        public void deleteBlob(String blobName) throws IOException {
                            files.remove(blobName);
                        }

                        @Override
                        public Map<String, BlobMetaData> listBlobs() throws IOException {
                            final Map<String, BlobMetaData> blobs = new HashMap<>(files.size());
                            files.forEach((key, value) -> blobs.put(key, new PlainBlobMetaData(key, value.length)));
                            return blobs;
                        }

                        @Override
                        public Map<String, BlobMetaData> listBlobsByPrefix(String blobNamePrefix) throws IOException {
                            return listBlobs().entrySet().stream()
                                                         .filter(e -> e.getKey().startsWith(blobNamePrefix))
                                                         .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
                        }

                        @Override
                        public void move(String sourceBlobName, String targetBlobName) throws IOException {
                            byte[] bytes = files.remove(sourceBlobName);
                            if (bytes == null) {
                                throw new FileNotFoundException(sourceBlobName);
                            }
                            files.put(targetBlobName, bytes);
                        }
                    };
                }

                @Override
                public void delete(BlobPath path) throws IOException {
                    throw new UnsupportedOperationException("MemoryBlobStoreRepository does not support this method");
                }

                @Override
                public void close() throws IOException {
                    files.clear();
                }
            };
        }

        @Override
        protected BlobPath basePath() {
            return new BlobPath();
        }
    }

    /** Create a {@link BlobStoreRepository} with a random name **/
    private BlobStoreRepository createRepository() {
        String name = randomAlphaOfLength(10);
        return new MemoryBlobStoreRepository(new RepositoryMetaData(name, "in-memory", Settings.EMPTY), Settings.EMPTY, xContentRegistry());
    }
}
