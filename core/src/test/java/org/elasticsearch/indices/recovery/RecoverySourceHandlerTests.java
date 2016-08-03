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
package org.elasticsearch.indices.recovery;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.LocalTransportAddress;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.DirectoryService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecoverySourceHandlerTests extends ESTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    public void testSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
                put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        StartRecoveryRequest request = new StartRecoveryRequest(shardId,
                new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
                new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
            null, RecoveryState.Type.STORE, randomLong());
        Store store = newStore(createTempDir());
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request, () -> 0L, e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(), logger);
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot metadata = store.getMetadata(null);
        List<StoreFileMetaData> metas = new ArrayList<>();
        for (StoreFileMetaData md : metadata) {
            metas.add(md);
        }
        Store targetStore = newStore(createTempDir());
        handler.sendFiles(store, metas.toArray(new StoreFileMetaData[0]), (md) -> {
            try {
                return new IndexOutputOutputStream(targetStore.createVerifyingOutput(md.name(), md, IOContext.DEFAULT)) {
                    @Override
                    public void close() throws IOException {
                        super.close();
                        targetStore.directory().sync(Collections.singleton(md.name())); // sync otherwise MDW will mess with it
                    }
                };
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        Store.MetadataSnapshot targetStoreMetadata = targetStore.getMetadata(null);
        Store.RecoveryDiff recoveryDiff = targetStoreMetadata.recoveryDiff(metadata);
        assertEquals(metas.size(), recoveryDiff.identical.size());
        assertEquals(0, recoveryDiff.different.size());
        assertEquals(0, recoveryDiff.missing.size());
        IndexReader reader = DirectoryReader.open(targetStore.directory());
        assertEquals(numDocs, reader.maxDoc());
        IOUtils.close(reader, store, targetStore);
    }

    public void testHandleCorruptedIndexOnSendSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
                put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        StartRecoveryRequest request = new StartRecoveryRequest(shardId,
                new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
                new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
            null, RecoveryState.Type.STORE, randomLong());
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request, () -> 0L, e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(), logger) {
            @Override
            protected void failEngine(IOException cause) {
                assertFalse(failedEngine.get());
                failedEngine.set(true);
            }
        };
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot metadata = store.getMetadata(null);
        List<StoreFileMetaData> metas = new ArrayList<>();
        for (StoreFileMetaData md : metadata) {
            metas.add(md);
        }

        CorruptionUtils.corruptFile(random(), FileSystemUtils.files(tempDir, (p) ->
                (p.getFileName().toString().equals("write.lock") ||
                        p.getFileName().toString().startsWith("extra")) == false));
        Store targetStore = newStore(createTempDir(), false);
        try {
            handler.sendFiles(store, metas.toArray(new StoreFileMetaData[0]), (md) -> {
                try {
                    return new IndexOutputOutputStream(targetStore.createVerifyingOutput(md.name(), md, IOContext.DEFAULT)) {
                        @Override
                        public void close() throws IOException {
                            super.close();
                            store.directory().sync(Collections.singleton(md.name())); // sync otherwise MDW will mess with it
                        }
                    };
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            fail("corrupted index");
        } catch (IOException ex) {
            assertNotNull(ExceptionsHelper.unwrapCorruption(ex));
        }
        assertTrue(failedEngine.get());
        IOUtils.close(store, targetStore);
    }


    public void testHandleExceptinoOnSendSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
                put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        StartRecoveryRequest request = new StartRecoveryRequest(shardId,
                new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
                new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
            null, RecoveryState.Type.STORE, randomLong());
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request, () -> 0L, e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(), logger) {
            @Override
            protected void failEngine(IOException cause) {
                assertFalse(failedEngine.get());
                failedEngine.set(true);
            }
        };
        Directory dir = store.directory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), dir, newIndexWriterConfig());
        int numDocs = randomIntBetween(10, 100);
        for (int i = 0; i < numDocs; i++) {
            Document document = new Document();
            document.add(new StringField("id", Integer.toString(i), Field.Store.YES));
            document.add(newField("field", randomUnicodeOfCodepointLengthBetween(1, 10), TextField.TYPE_STORED));
            writer.addDocument(document);
        }
        writer.commit();
        writer.close();

        Store.MetadataSnapshot metadata = store.getMetadata(null);
        List<StoreFileMetaData> metas = new ArrayList<>();
        for (StoreFileMetaData md : metadata) {
            metas.add(md);
        }
        final boolean throwCorruptedIndexException = randomBoolean();
        Store targetStore = newStore(createTempDir(), false);
        try {
            handler.sendFiles(store, metas.toArray(new StoreFileMetaData[0]), (md) -> {
                if (throwCorruptedIndexException) {
                    throw new RuntimeException(new CorruptIndexException("foo", "bar"));
                } else {
                    throw new RuntimeException("boom");
                }
            });
            fail("exception index");
        } catch (RuntimeException ex) {
            assertNull(ExceptionsHelper.unwrapCorruption(ex));
            if (throwCorruptedIndexException) {
                assertEquals(ex.getMessage(), "[File corruption occurred on recovery but checksums are ok]");
            } else {
                assertEquals(ex.getMessage(), "boom");
            }
        } catch (CorruptIndexException ex) {
            fail("not expected here");
        }
        assertFalse(failedEngine.get());
        IOUtils.close(store, targetStore);
    }

    public void testThrowExceptionOnPrimaryRelocatedBeforePhase1Completed() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        StartRecoveryRequest request = new StartRecoveryRequest(shardId,
            new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
            null, RecoveryState.Type.REPLICA, randomLong());
        IndexShard shard = mock(IndexShard.class);
        Translog.View translogView = mock(Translog.View.class);
        when(shard.acquireTranslogView()).thenReturn(translogView);
        when(shard.state()).thenReturn(IndexShardState.RELOCATED);
        AtomicBoolean phase1Called = new AtomicBoolean();
        AtomicBoolean phase2Called = new AtomicBoolean();
        RecoverySourceHandler handler = new RecoverySourceHandler(shard, null, request, () -> 0L, e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(), logger) {

            @Override
            public void phase1(final IndexCommit snapshot, final Translog.View translogView) {
                phase1Called.set(true);
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) {
                phase2Called.set(true);
            }
        };
        expectThrows(IndexShardRelocatedException.class, () -> handler.recoverToTarget());
        assertTrue(phase1Called.get());
        assertFalse(phase2Called.get());
    }

    public void testWaitForClusterStateOnPrimaryRelocation() throws IOException, InterruptedException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        StartRecoveryRequest request = new StartRecoveryRequest(shardId,
            new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", LocalTransportAddress.buildUnique(), emptyMap(), emptySet(), Version.CURRENT),
            null, RecoveryState.Type.PRIMARY_RELOCATION, randomLong());
        AtomicBoolean phase1Called = new AtomicBoolean();
        AtomicBoolean phase2Called = new AtomicBoolean();
        AtomicBoolean ensureClusterStateVersionCalled = new AtomicBoolean();
        AtomicBoolean recoveriesDelayed = new AtomicBoolean();
        AtomicBoolean relocated = new AtomicBoolean();

        IndexShard shard = mock(IndexShard.class);
        Translog.View translogView = mock(Translog.View.class);
        when(shard.acquireTranslogView()).thenReturn(translogView);
        when(shard.state()).then(i -> relocated.get() ? IndexShardState.RELOCATED : IndexShardState.STARTED);
        doAnswer(i -> {
            relocated.set(true);
            assertTrue(recoveriesDelayed.get());
            return null;
        }).when(shard).relocated(any(String.class));

        RecoveryTargetHandler targetHandler = mock(RecoveryTargetHandler.class);

        final Supplier<Long> currentClusterStateVersionSupplier = () -> {
            assertFalse(ensureClusterStateVersionCalled.get());
            assertTrue(recoveriesDelayed.get());
            ensureClusterStateVersionCalled.set(true);
            return 0L;
        };
        final Function<String, Releasable> delayNewRecoveries = s -> {
            assertTrue(phase1Called.get());
            assertTrue(phase2Called.get());

            assertFalse(recoveriesDelayed.get());
            recoveriesDelayed.set(true);
            return () -> {
                assertTrue(recoveriesDelayed.get());
                recoveriesDelayed.set(false);
            };
        };

        RecoverySourceHandler handler = new RecoverySourceHandler(shard, targetHandler, request, currentClusterStateVersionSupplier,
            delayNewRecoveries, recoverySettings.getChunkSize().bytesAsInt(), logger) {

            @Override
            public void phase1(final IndexCommit snapshot, final Translog.View translogView) {
                phase1Called.set(true);
            }

            @Override
            public void phase2(Translog.Snapshot snapshot) {
                phase2Called.set(true);
            }
        };
        handler.recoverToTarget();
        assertTrue(ensureClusterStateVersionCalled.get());
        assertTrue(phase1Called.get());
        assertTrue(phase2Called.get());
        assertTrue(relocated.get());
        assertFalse(recoveriesDelayed.get());
    }

    private Store newStore(Path path) throws IOException {
        return newStore(path, true);
    }
    private Store newStore(Path path, boolean checkIndex) throws IOException {
        DirectoryService directoryService = new DirectoryService(shardId, INDEX_SETTINGS) {
            @Override
            public long throttleTimeInNanos() {
                return 0;
            }

            @Override
            public Directory newDirectory() throws IOException {
                BaseDirectoryWrapper baseDirectoryWrapper = RecoverySourceHandlerTests.newFSDirectory(path);
                if (checkIndex == false) {
                    baseDirectoryWrapper.setCheckIndexOnClose(false); // don't run checkindex we might corrupt the index in these tests
                }
                return baseDirectoryWrapper;
            }
        };
        return new Store(shardId,  INDEX_SETTINGS, directoryService, new DummyShardLock(shardId));
    }


}
