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
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.BaseDirectoryWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.Mapping;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbersService;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
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
        final StartRecoveryRequest request = new StartRecoveryRequest(
            shardId,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            null,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() ? SequenceNumbersService.UNASSIGNED_SEQ_NO : randomNonNegativeLong());
        Store store = newStore(createTempDir());
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request, () -> 0L, e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(), Settings.EMPTY);
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

    public void testSendSnapshotSendsOps() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final int fileChunkSizeInBytes = recoverySettings.getChunkSize().bytesAsInt();
        final long startingSeqNo = randomBoolean() ? SequenceNumbersService.UNASSIGNED_SEQ_NO : randomIntBetween(0, 16);
        final StartRecoveryRequest request = new StartRecoveryRequest(
            shardId,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            null,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() ? SequenceNumbersService.UNASSIGNED_SEQ_NO : randomNonNegativeLong());
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final RecoveryTargetHandler recoveryTarget = mock(RecoveryTargetHandler.class);
        final RecoverySourceHandler handler =
            new RecoverySourceHandler(shard, recoveryTarget, request, () -> 0L, e -> () -> {}, fileChunkSizeInBytes, Settings.EMPTY);
        final List<Translog.Operation> operations = new ArrayList<>();
        final int initialNumberOfDocs = randomIntBetween(16, 64);
        for (int i = 0; i < initialNumberOfDocs; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, SequenceNumbersService.UNASSIGNED_SEQ_NO, true)));
        }
        final int numberOfDocsWithValidSequenceNumbers = randomIntBetween(16, 64);
        for (int i = initialNumberOfDocs; i < initialNumberOfDocs + numberOfDocsWithValidSequenceNumbers; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, i - initialNumberOfDocs, true)));
        }
        operations.add(null);
        int totalOperations = handler.sendSnapshot(startingSeqNo, new Translog.Snapshot() {
            private int counter = 0;

            @Override
            public int totalOperations() {
                return operations.size() - 1;
            }

            @Override
            public Translog.Operation next() throws IOException {
                return operations.get(counter++);
            }
        });
        if (startingSeqNo == SequenceNumbersService.UNASSIGNED_SEQ_NO) {
            assertThat(totalOperations, equalTo(initialNumberOfDocs + numberOfDocsWithValidSequenceNumbers));
        } else {
            assertThat(totalOperations, equalTo(Math.toIntExact(numberOfDocsWithValidSequenceNumbers - startingSeqNo)));
        }
    }

    private Engine.Index getIndex(final String id) {
        final String type = "test";
        final ParseContext.Document document = new ParseContext.Document();
        document.add(new TextField("test", "test", Field.Store.YES));
        final Field uidField = new Field("_uid", Uid.createUid(type, id), UidFieldMapper.Defaults.FIELD_TYPE);
        final Field versionField = new NumericDocValuesField("_version", Versions.MATCH_ANY);
        final SeqNoFieldMapper.SequenceID seqID = SeqNoFieldMapper.SequenceID.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        final BytesReference source = new BytesArray(new byte[] { 1 });
        final ParsedDocument doc =
            new ParsedDocument(versionField, seqID, id, type, null, Arrays.asList(document), source, XContentType.JSON, null);
        return new Engine.Index(new Term("_uid", doc.uid()), doc);
    }

    public void testHandleCorruptedIndexOnSendSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
                put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        final StartRecoveryRequest request =
            new StartRecoveryRequest(
                shardId,
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                null,
                randomBoolean(),
                randomNonNegativeLong(),
                randomBoolean() ? SequenceNumbersService.UNASSIGNED_SEQ_NO : 0L);
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request, () -> 0L, e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(), Settings.EMPTY) {
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
        final StartRecoveryRequest request =
            new StartRecoveryRequest(
                shardId,
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                null,
                randomBoolean(),
                randomNonNegativeLong(),
                randomBoolean() ? SequenceNumbersService.UNASSIGNED_SEQ_NO : 0L);
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request, () -> 0L, e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(), Settings.EMPTY) {
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
        final boolean attemptSequenceNumberBasedRecovery = randomBoolean();
        final boolean isTranslogReadyForSequenceNumberBasedRecovery = attemptSequenceNumberBasedRecovery && randomBoolean();
        final StartRecoveryRequest request =
            new StartRecoveryRequest(
                shardId,
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                null,
                false,
                randomNonNegativeLong(),
                attemptSequenceNumberBasedRecovery ? randomNonNegativeLong() : SequenceNumbersService.UNASSIGNED_SEQ_NO);
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean())).thenReturn(mock(SegmentsStats.class));
        final Translog.View translogView = mock(Translog.View.class);
        when(shard.acquireTranslogView()).thenReturn(translogView);
        when(shard.state()).thenReturn(IndexShardState.RELOCATED);
        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new RecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            request,
            () -> 0L,
            e -> () -> {},
            recoverySettings.getChunkSize().bytesAsInt(),
            Settings.EMPTY) {

            @Override
            boolean isTranslogReadyForSequenceNumberBasedRecovery(final Translog.View translogView) {
                return isTranslogReadyForSequenceNumberBasedRecovery;
            }

            @Override
            public void phase1(final IndexCommit snapshot, final Translog.View translogView) {
                phase1Called.set(true);
            }

            @Override
            void prepareTargetForTranslog(final int totalTranslogOps, final long maxUnsafeAutoIdTimestamp) throws IOException {
                prepareTargetForTranslogCalled.set(true);
            }

            @Override
            void phase2(long startingSeqNo, Translog.Snapshot snapshot) throws IOException {
                phase2Called.set(true);
            }

        };
        expectThrows(IndexShardRelocatedException.class, handler::recoverToTarget);
        // phase1 should only be attempted if we are not doing a sequence-number-based recovery
        assertThat(phase1Called.get(), equalTo(!isTranslogReadyForSequenceNumberBasedRecovery));
        assertTrue(prepareTargetForTranslogCalled.get());
        assertFalse(phase2Called.get());
    }

    public void testWaitForClusterStateOnPrimaryRelocation() throws IOException, InterruptedException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final boolean attemptSequenceNumberBasedRecovery = randomBoolean();
        final boolean isTranslogReadyForSequenceNumberBasedRecovery = attemptSequenceNumberBasedRecovery && randomBoolean();
        final StartRecoveryRequest request =
            new StartRecoveryRequest(
                shardId,
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
                null,
                true,
                randomNonNegativeLong(),
                attemptSequenceNumberBasedRecovery ? randomNonNegativeLong(): SequenceNumbersService.UNASSIGNED_SEQ_NO);
        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final AtomicBoolean ensureClusterStateVersionCalled = new AtomicBoolean();
        final AtomicBoolean recoveriesDelayed = new AtomicBoolean();
        final AtomicBoolean relocated = new AtomicBoolean();

        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean())).thenReturn(mock(SegmentsStats.class));
        final Translog.View translogView = mock(Translog.View.class);
        when(shard.acquireTranslogView()).thenReturn(translogView);
        when(shard.state()).then(i -> relocated.get() ? IndexShardState.RELOCATED : IndexShardState.STARTED);
        doAnswer(i -> {
            relocated.set(true);
            assertTrue(recoveriesDelayed.get());
            return null;
        }).when(shard).relocated(any(String.class));

        final Supplier<Long> currentClusterStateVersionSupplier = () -> {
            assertFalse(ensureClusterStateVersionCalled.get());
            assertTrue(recoveriesDelayed.get());
            ensureClusterStateVersionCalled.set(true);
            return 0L;
        };

        final Function<String, Releasable> delayNewRecoveries = s -> {
            // phase1 should only be attempted if we are not doing a sequence-number-based recovery
            assertThat(phase1Called.get(), equalTo(!isTranslogReadyForSequenceNumberBasedRecovery));
            assertTrue(prepareTargetForTranslogCalled.get());
            assertTrue(phase2Called.get());

            assertFalse(recoveriesDelayed.get());
            recoveriesDelayed.set(true);
            return () -> {
                assertTrue(recoveriesDelayed.get());
                recoveriesDelayed.set(false);
            };
        };

        final RecoverySourceHandler handler = new RecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            request,
            currentClusterStateVersionSupplier,
            delayNewRecoveries,
            recoverySettings.getChunkSize().bytesAsInt(),
            Settings.EMPTY) {

            @Override
            boolean isTranslogReadyForSequenceNumberBasedRecovery(final Translog.View translogView) {
                return isTranslogReadyForSequenceNumberBasedRecovery;
            }

            @Override
            public void phase1(final IndexCommit snapshot, final Translog.View translogView) {
                phase1Called.set(true);
            }

            @Override
            void prepareTargetForTranslog(final int totalTranslogOps, final long maxUnsafeAutoIdTimestamp) throws IOException {
                prepareTargetForTranslogCalled.set(true);
            }

            @Override
            void phase2(long startingSeqNo, Translog.Snapshot snapshot) throws IOException {
                phase2Called.set(true);
            }

        };

        handler.recoverToTarget();
        assertTrue(ensureClusterStateVersionCalled.get());
        // phase1 should only be attempted if we are not doing a sequence-number-based recovery
        assertThat(phase1Called.get(), equalTo(!isTranslogReadyForSequenceNumberBasedRecovery));
        assertTrue(prepareTargetForTranslogCalled.get());
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
