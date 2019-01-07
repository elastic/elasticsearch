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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefIterator;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.BaseFuture;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetaData;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyList;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecoverySourceHandlerTests extends ESTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index",
        Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    public void testSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
            put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Store store = newStore(createTempDir());

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
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            IndexOutputOutputStream out;
            @Override
            public BaseFuture<Void> writeFileChunk(StoreFileMetaData md, long position, BytesReference content,
                                                   boolean lastChunk, int totalTranslogOps) throws IOException {
                if (position == 0) {
                    out = new IndexOutputOutputStream(targetStore.createVerifyingOutput(md.name(), md, IOContext.DEFAULT));
                }
                BytesRefIterator iterator = content.iterator();
                BytesRef scratch;
                while((scratch = iterator.next()) != null) { // we iterate over all pages - this is a 0-copy for all core impls
                    out.write(scratch.bytes, scratch.offset, scratch.length);
                }
                if (lastChunk) {
                    out.close();
                    targetStore.directory().sync(Collections.singleton(md.name())); // sync otherwise MDW will mess with it
                }
                return BaseFuture.completedFuture(null);
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(null, target, () -> 1, request,
            recoverySettings.getChunkSize().bytesAsInt());
        handler.sendFiles(store, metas.toArray(new StoreFileMetaData[0]), () -> 1);
        Store.MetadataSnapshot targetStoreMetadata = targetStore.getMetadata(null);
        Store.RecoveryDiff recoveryDiff = targetStoreMetadata.recoveryDiff(metadata);
        assertEquals(metas.size(), recoveryDiff.identical.size());
        assertEquals(0, recoveryDiff.different.size());
        assertEquals(0, recoveryDiff.missing.size());
        IndexReader reader = DirectoryReader.open(targetStore.directory());
        assertEquals(numDocs, reader.maxDoc());
        IOUtils.close(reader, store, targetStore);
    }

    public StartRecoveryRequest getStartRecoveryRequest() throws IOException {
        Store.MetadataSnapshot metadataSnapshot = randomBoolean() ? Store.MetadataSnapshot.EMPTY :
            new Store.MetadataSnapshot(Collections.emptyMap(),
                Collections.singletonMap(Engine.HISTORY_UUID_KEY, UUIDs.randomBase64UUID()), randomIntBetween(0, 100));
        return new StartRecoveryRequest(
            shardId,
            null,
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            new DiscoveryNode("b", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT),
            metadataSnapshot,
            randomBoolean(),
            randomNonNegativeLong(),
            randomBoolean() || metadataSnapshot.getHistoryUUID() == null ?
                SequenceNumbers.UNASSIGNED_SEQ_NO : randomNonNegativeLong());
    }

    public void testSendSnapshotSendsOps() throws Exception {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final int fileChunkSizeInBytes = recoverySettings.getChunkSize().bytesAsInt();
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final RecoveryTargetHandler recoveryTarget = mock(RecoveryTargetHandler.class);
        when(recoveryTarget.indexTranslogOperations(anyList(), anyInt(), anyLong(), anyLong()))
            .thenReturn(BaseFuture.completedFuture(randomNonNegativeLong()));
        final RecoverySourceHandler handler =
            new RecoverySourceHandler(shard, recoveryTarget, () -> 1, request, fileChunkSizeInBytes);
        final List<Translog.Operation> operations = new ArrayList<>();
        final int initialNumberOfDocs = randomIntBetween(16, 64);
        for (int i = 0; i < initialNumberOfDocs; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, 1, SequenceNumbers.UNASSIGNED_SEQ_NO, true)));
        }
        final int numberOfDocsWithValidSequenceNumbers = randomIntBetween(16, 64);
        for (int i = initialNumberOfDocs; i < initialNumberOfDocs + numberOfDocsWithValidSequenceNumbers; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, 1, i - initialNumberOfDocs, true)));
        }
        operations.add(null);
        final long startingSeqNo = randomIntBetween(0, numberOfDocsWithValidSequenceNumbers - 1);
        final long requiredStartingSeqNo = randomIntBetween((int) startingSeqNo, numberOfDocsWithValidSequenceNumbers - 1);
        final long endingSeqNo = randomIntBetween((int) requiredStartingSeqNo - 1, numberOfDocsWithValidSequenceNumbers - 1);
        BaseFuture<RecoverySourceHandler.SendSnapshotResult> result = handler.phase2(startingSeqNo, requiredStartingSeqNo,
            endingSeqNo, new Translog.Snapshot() {
                @Override
                public void close() {

                }

                private int counter = 0;

                @Override
                public int totalOperations() {
                    return operations.size() - 1;
                }

                @Override
                public Translog.Operation next() throws IOException {
                    if (counter < operations.size()) {
                        return operations.get(counter++);
                    }
                    return null;
                }
            }, randomNonNegativeLong(), randomNonNegativeLong());
        final int expectedOps = (int) (endingSeqNo - startingSeqNo + 1);
        assertThat(result.get().totalOperations, equalTo(expectedOps));
        final ArgumentCaptor<List> shippedOpsCaptor = ArgumentCaptor.forClass(List.class);
        verify(recoveryTarget).indexTranslogOperations(shippedOpsCaptor.capture(), ArgumentCaptor.forClass(Integer.class).capture(),
            ArgumentCaptor.forClass(Long.class).capture(), ArgumentCaptor.forClass(Long.class).capture());
        List<Translog.Operation> shippedOps = new ArrayList<>();
        for (List list: shippedOpsCaptor.getAllValues()) {
            shippedOps.addAll(list);
        }
        shippedOps.sort(Comparator.comparing(Translog.Operation::seqNo));
        assertThat(shippedOps.size(), equalTo(expectedOps));
        for (int i = 0; i < shippedOps.size(); i++) {
            assertThat(shippedOps.get(i), equalTo(operations.get(i + (int) startingSeqNo + initialNumberOfDocs)));
        }
        if (endingSeqNo >= requiredStartingSeqNo + 1) {
            // check that missing ops blows up
            List<Translog.Operation> requiredOps = operations.subList(0, operations.size() - 1).stream() // remove last null marker
                .filter(o -> o.seqNo() >= requiredStartingSeqNo && o.seqNo() <= endingSeqNo).collect(Collectors.toList());
            List<Translog.Operation> opsToSkip = randomSubsetOf(randomIntBetween(1, requiredOps.size()), requiredOps);
            expectThrows(ExecutionException.class, IllegalStateException.class, () ->
                handler.phase2(startingSeqNo, requiredStartingSeqNo,
                    endingSeqNo, new Translog.Snapshot() {
                        @Override
                        public void close() {

                        }

                        private int counter = 0;

                        @Override
                        public int totalOperations() {
                            return operations.size() - 1 - opsToSkip.size();
                        }

                        @Override
                        public Translog.Operation next() throws IOException {
                            Translog.Operation op;
                            do {
                                if (counter < operations.size()) {
                                    op = operations.get(counter++);
                                } else {
                                    op = null;
                                }
                            } while (op != null && opsToSkip.contains(op));
                            return op;
                        }
                    }, randomNonNegativeLong(), randomNonNegativeLong()).get());
        }
    }

    private Engine.Index getIndex(final String id) {
        final String type = "test";
        final ParseContext.Document document = new ParseContext.Document();
        document.add(new TextField("test", "test", Field.Store.YES));
        final Field idField = new Field("_id", Uid.encodeId(id), IdFieldMapper.Defaults.FIELD_TYPE);
        final Field versionField = new NumericDocValuesField("_version", Versions.MATCH_ANY);
        final SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(idField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        final BytesReference source = new BytesArray(new byte[] { 1 });
        final ParsedDocument doc =
            new ParsedDocument(versionField, seqID, id, type, null, Arrays.asList(document), source, XContentType.JSON, null);
        return new Engine.Index(new Term("_id", Uid.encodeId(doc.id())), randomNonNegativeLong(), doc);
    }

    public void testHandleCorruptedIndexOnSendSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
            put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
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
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            IndexOutputOutputStream out;
            @Override
            public BaseFuture<Void> writeFileChunk(StoreFileMetaData md, long position, BytesReference content,
                                                   boolean lastChunk, int totalTranslogOps) {
                try {
                    if (position == 0) {
                        out = new IndexOutputOutputStream(targetStore.createVerifyingOutput(md.name(), md, IOContext.DEFAULT));
                    }
                    BytesRefIterator iterator = content.iterator();
                    BytesRef scratch;
                    while ((scratch = iterator.next()) != null) { // we iterate over all pages - this is a 0-copy for all core impls
                        out.write(scratch.bytes, scratch.offset, scratch.length);
                    }
                    if (lastChunk) {
                        out.close();
                        out = null;
                        targetStore.directory().sync(Collections.singleton(md.name())); // sync otherwise MDW will mess with it
                    }
                    return BaseFuture.completedFuture(null);
                } catch (IOException e) {
                    IOUtils.closeWhileHandlingException(out);
                    return BaseFuture.failedFuture(e);
                }
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(null, target, () -> 1, request,
            recoverySettings.getChunkSize().bytesAsInt()) {
            @Override
            protected void failEngine(IOException cause) {
                assertFalse(failedEngine.get());
                failedEngine.set(true);
            }
        };
        try {
            handler.sendFiles(store, metas.toArray(new StoreFileMetaData[0]), () -> between(1, 1000)).get();
            fail("corrupted index");
        } catch (Exception ex) {
            assertNotNull(ExceptionsHelper.unwrapCorruption(ex));
        }
        assertTrue(failedEngine.get());
        IOUtils.close(store, targetStore);
    }


    public void testHandleExceptionOnSendFiles() throws Throwable {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
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
        RecoveryTargetHandler target = new TestRecoveryTargetHandler(){
            @Override
            public BaseFuture<Void> writeFileChunk(StoreFileMetaData fileMetaData, long position,
                                                   BytesReference content, boolean lastChunk, int totalTranslogOps) {
                if (throwCorruptedIndexException) {
                    return BaseFuture.failedFuture(new RuntimeException(new CorruptIndexException("foo", "bar")));
                } else {
                    return BaseFuture.failedFuture(new RuntimeException("boom"));
                }
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(null, target, () -> 1, request,
            recoverySettings.getChunkSize().bytesAsInt()) {
            @Override
            protected void failEngine(IOException cause) {
                assertFalse(failedEngine.get());
                failedEngine.set(true);
            }
        };
        try {
            handler.sendFiles(store, metas.toArray(new StoreFileMetaData[0]), () -> 100).get();
            fail("exception index");
        } catch (ExecutionException ex) {
            assertNull(ExceptionsHelper.unwrapCorruption(ex.getCause()));
            if (throwCorruptedIndexException) {
                assertEquals(ex.getCause().getMessage(), "[File corruption occurred on recovery but checksums are ok]");
            } else {
                assertEquals(ex.getCause().getMessage(), "boom");
            }
        }
        assertFalse(failedEngine.get());
        IOUtils.close(store, targetStore);
    }

    public void testThrowExceptionOnPrimaryRelocatedBeforePhase1Started() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean())).thenReturn(mock(SegmentsStats.class));
        when(shard.isRelocatedPrimary()).thenReturn(true);
        when(shard.acquireSafeIndexCommit()).thenReturn(mock(Engine.IndexCommitRef.class));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>)invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), anyObject());
        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new RecoverySourceHandler(
                shard,
                mock(RecoveryTargetHandler.class),
                () -> 1,
                request,
                recoverySettings.getChunkSize().bytesAsInt()) {

            @Override
            public BaseFuture<Phase1Result> phase1(final IndexCommit snapshot, final Supplier<Integer> translogOps) {
                return super.phase1(snapshot, translogOps).whenComplete((res, e) -> phase1Called.set(true));
            }

            @Override
            BaseFuture<TimeValue> prepareTargetForTranslog(final boolean fileBasedRecovery, final int totalTranslogOps) {
                return super.prepareTargetForTranslog(fileBasedRecovery, totalTranslogOps)
                    .whenComplete((res, e) -> prepareTargetForTranslogCalled.set(true));
            }

            @Override
            BaseFuture<SendSnapshotResult> phase2(long startingSeqNo, long requiredSeqNoRangeStart, long endingSeqNo,
                                                  Translog.Snapshot snapshot, long maxSeenAutoIdTimestamp,
                                                  long maxSeqNoOfUpdatesOrDeletes) throws IOException {
                return super.phase2(startingSeqNo, requiredSeqNoRangeStart, endingSeqNo, snapshot,
                    maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes).whenComplete((res, e) -> phase2Called.set(true));
            }

        };
        try {
            handler.recoverToTarget().get();
        } catch (Exception e) {
            assertThat(ExceptionsHelper.unwrap(e, IndexShardRelocatedException.class), notNullValue());
        }
        assertFalse(phase1Called.get());
        assertFalse(prepareTargetForTranslogCalled.get());
        assertFalse(phase2Called.get());
    }

    public void testCancellationsDoesNotLeakPrimaryPermits() throws Exception {
        final CancellableThreads cancellableThreads = new CancellableThreads();
        final IndexShard shard = mock(IndexShard.class);
        final AtomicBoolean freed = new AtomicBoolean(true);
        when(shard.isRelocatedPrimary()).thenReturn(false);
        doAnswer(invocation -> {
            freed.set(false);
            ((ActionListener<Releasable>)invocation.getArguments()[0]).onResponse(() -> freed.set(true));
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), anyObject());

        Thread cancelingThread = new Thread(() -> cancellableThreads.cancel("test"));
        cancelingThread.start();
        try {
            RecoverySourceHandler.runUnderPrimaryPermit(() -> {}, "test", shard, cancellableThreads, logger);
        } catch (CancellableThreads.ExecutionCancelledException e) {
            // expected.
        }
        cancelingThread.join();
        // we have to use assert busy as we may be interrupted while acquiring the permit, if so we want to check
        // that the permit is released.
        assertBusy(() -> assertTrue(freed.get()));
    }

    private Store newStore(Path path) throws IOException {
        return newStore(path, true);
    }

    private Store newStore(Path path, boolean checkIndex) throws IOException {
        BaseDirectoryWrapper baseDirectoryWrapper = RecoverySourceHandlerTests.newFSDirectory(path);
        if (checkIndex == false) {
            baseDirectoryWrapper.setCheckIndexOnClose(false); // don't run checkindex we might corrupt the index in these tests
        }
        return new Store(shardId,  INDEX_SETTINGS, baseDirectoryWrapper, new DummyShardLock(shardId));
    }

    static class TestRecoveryTargetHandler implements RecoveryTargetHandler {
        @Override
        public BaseFuture<Void> prepareForTranslogOperations(boolean fileBasedRecovery, int totalTranslogOps) throws IOException {
            return null;
        }

        @Override
        public BaseFuture<Void> finalizeRecovery(long globalCheckpoint) throws IOException {
            return null;
        }

        @Override
        public void ensureClusterStateVersion(long clusterStateVersion) {

        }

        @Override
        public void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext) {

        }

        @Override
        public BaseFuture<Long> indexTranslogOperations(List<Translog.Operation> operations,
                                                        int totalTranslogOps, long maxSeenAutoIdTimestampOnPrimary,
                                                        long maxSeqNoOfUpdatesOrDeletesOnPrimary) throws IOException {
            return null;
        }

        @Override
        public BaseFuture<Void> receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes,
                                                List<String> phase1ExistingFileNames, List<Long> phase1ExistingFileSizes,
                                                int totalTranslogOps) {
            return null;
        }

        @Override
        public BaseFuture<Void> cleanFiles(int totalTranslogOps, Store.MetadataSnapshot sourceMetaData) throws IOException {
            return null;
        }

        @Override
        public BaseFuture<Void> writeFileChunk(StoreFileMetaData fileMetaData, long position,
                                               BytesReference content, boolean lastChunk, int totalTranslogOps) throws IOException {
            return null;
        }
    }

}
