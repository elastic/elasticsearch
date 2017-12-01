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
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.UidFieldMapper;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
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
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RecoverySourceHandlerTests extends ESTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index", Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

    public void testSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
            put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Store store = newStore(createTempDir());
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request,
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

    public void testSendSnapshotSendsOps() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final int fileChunkSizeInBytes = recoverySettings.getChunkSize().bytesAsInt();
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final RecoveryTargetHandler recoveryTarget = mock(RecoveryTargetHandler.class);
        final RecoverySourceHandler handler =
            new RecoverySourceHandler(shard, recoveryTarget, request, fileChunkSizeInBytes, Settings.EMPTY);
        final List<Translog.Operation> operations = new ArrayList<>();
        final int initialNumberOfDocs = randomIntBetween(16, 64);
        for (int i = 0; i < initialNumberOfDocs; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, SequenceNumbers.UNASSIGNED_SEQ_NO, true)));
        }
        final int numberOfDocsWithValidSequenceNumbers = randomIntBetween(16, 64);
        for (int i = initialNumberOfDocs; i < initialNumberOfDocs + numberOfDocsWithValidSequenceNumbers; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, i - initialNumberOfDocs, true)));
        }
        operations.add(null);
        final long startingSeqNo = randomIntBetween(0, numberOfDocsWithValidSequenceNumbers - 1);
        final long requiredStartingSeqNo = randomIntBetween((int) startingSeqNo, numberOfDocsWithValidSequenceNumbers - 1);
        final long endingSeqNo = randomIntBetween((int) requiredStartingSeqNo - 1, numberOfDocsWithValidSequenceNumbers - 1);
        RecoverySourceHandler.SendSnapshotResult result = handler.sendSnapshot(startingSeqNo, requiredStartingSeqNo,
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
                    return operations.get(counter++);
                }
            });
        final int expectedOps = (int) (endingSeqNo - startingSeqNo + 1);
        assertThat(result.totalOperations, equalTo(expectedOps));
        final ArgumentCaptor<List> shippedOpsCaptor = ArgumentCaptor.forClass(List.class);
        verify(recoveryTarget).indexTranslogOperations(shippedOpsCaptor.capture(), ArgumentCaptor.forClass(Integer.class).capture());
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
            expectThrows(IllegalStateException.class, () ->
                handler.sendSnapshot(startingSeqNo, requiredStartingSeqNo,
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
                                op = operations.get(counter++);
                            } while (op != null && opsToSkip.contains(op));
                            return op;
                        }
                    }));
        }
    }

    private Engine.Index getIndex(final String id) {
        final String type = "test";
        final ParseContext.Document document = new ParseContext.Document();
        document.add(new TextField("test", "test", Field.Store.YES));
        final Field uidField = new Field("_uid", Uid.createUid(type, id), UidFieldMapper.Defaults.FIELD_TYPE);
        final Field versionField = new NumericDocValuesField("_version", Versions.MATCH_ANY);
        final SeqNoFieldMapper.SequenceIDFields seqID = SeqNoFieldMapper.SequenceIDFields.emptySeqID();
        document.add(uidField);
        document.add(versionField);
        document.add(seqID.seqNo);
        document.add(seqID.seqNoDocValue);
        document.add(seqID.primaryTerm);
        final BytesReference source = new BytesArray(new byte[] { 1 });
        final ParsedDocument doc =
            new ParsedDocument(versionField, seqID, id, type, null, Arrays.asList(document), source, XContentType.JSON, null);
        return new Engine.Index(new Term("_uid", Uid.createUidAsBytes(doc.type(), doc.id())), doc);
    }

    public void testHandleCorruptedIndexOnSendSendFiles() throws Throwable {
        Settings settings = Settings.builder().put("indices.recovery.concurrent_streams", 1).
            put("indices.recovery.concurrent_small_file_streams", 1).build();
        final RecoverySettings recoverySettings = new RecoverySettings(settings, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request,
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
        final StartRecoveryRequest request = getStartRecoveryRequest();
        Path tempDir = createTempDir();
        Store store = newStore(tempDir, false);
        AtomicBoolean failedEngine = new AtomicBoolean(false);
        RecoverySourceHandler handler = new RecoverySourceHandler(null, null, request,
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

    public void testThrowExceptionOnPrimaryRelocatedBeforePhase1Started() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean())).thenReturn(mock(SegmentsStats.class));
        when(shard.state()).thenReturn(IndexShardState.RELOCATED);
        when(shard.acquireIndexCommit(anyBoolean())).thenReturn(mock(Engine.IndexCommitRef.class));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>)invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString());
        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new RecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            request,
            recoverySettings.getChunkSize().bytesAsInt(),
            Settings.EMPTY) {


            @Override
            boolean isTranslogReadyForSequenceNumberBasedRecovery() throws IOException {
                return randomBoolean();
            }

            @Override
            public void phase1(final IndexCommit snapshot, final Supplier<Integer> translogOps) {
                phase1Called.set(true);
            }

            @Override
            void prepareTargetForTranslog(final int totalTranslogOps) throws IOException {
                prepareTargetForTranslogCalled.set(true);
            }

            @Override
            long phase2(long startingSeqNo, long requiredSeqNoRangeStart, long endingSeqNo, Translog.Snapshot snapshot) throws IOException {
                phase2Called.set(true);
                return SequenceNumbers.UNASSIGNED_SEQ_NO;
            }

        };
        expectThrows(IndexShardRelocatedException.class, handler::recoverToTarget);
        assertFalse(phase1Called.get());
        assertFalse(prepareTargetForTranslogCalled.get());
        assertFalse(phase2Called.get());
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
