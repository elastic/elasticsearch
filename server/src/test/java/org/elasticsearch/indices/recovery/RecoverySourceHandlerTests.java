/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Numbers;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.StopWatch;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.ReleasableBytesReference;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CancellableThreads;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.RecoveryEngineException;
import org.elasticsearch.index.engine.SegmentsStats;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SeqNoStats;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardRelocatedException;
import org.elasticsearch.index.shard.IndexShardState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.store.StoreFileMetadata;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.plan.RecoveryPlannerService;
import org.elasticsearch.indices.recovery.plan.ShardRecoveryPlan;
import org.elasticsearch.indices.recovery.plan.SourceOnlyRecoveryPlannerService;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.test.CorruptionUtils;
import org.elasticsearch.test.DummyShardLock;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.FixedExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;
import java.util.zip.CRC32;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RecoverySourceHandlerTests extends ESTestCase {
    private static final IndexSettings INDEX_SETTINGS = IndexSettingsModule.newIndexSettings("index",
        Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.Version.CURRENT).build());
    private final ShardId shardId = new ShardId(INDEX_SETTINGS.getIndex(), 1);
    private final ClusterSettings service = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    private final RecoveryPlannerService recoveryPlannerService = SourceOnlyRecoveryPlannerService.INSTANCE;

    private ThreadPool threadPool;
    private Executor recoveryExecutor;

    @Before
    public void setUpThreadPool() {
        if (randomBoolean()) {
            threadPool = new TestThreadPool(getTestName());
            recoveryExecutor = threadPool.generic();
        } else {
            // verify that both sending and receiving files can be completed with a single thread
            threadPool = new TestThreadPool(getTestName(),
                new FixedExecutorBuilder(Settings.EMPTY, "recovery_executor", between(1, 16), between(16, 128), "recovery_executor",
                    false));
            recoveryExecutor = threadPool.executor("recovery_executor");
        }
    }

    @After
    public void tearDownThreadPool() {
        terminate(threadPool);
    }

    public void testSendFiles() throws Throwable {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
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
        List<StoreFileMetadata> metas = new ArrayList<>();
        for (StoreFileMetadata md : metadata) {
            metas.add(md);
        }
        Store targetStore = newStore(createTempDir());
        MultiFileWriter multiFileWriter = new MultiFileWriter(targetStore, mock(RecoveryState.Index.class), "", logger, () -> {});
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void writeFileChunk(StoreFileMetadata md, long position, ReleasableBytesReference content, boolean lastChunk,
                                       int totalTranslogOps, ActionListener<Void> listener) {
                ActionListener.completeWith(listener, () -> {
                    multiFileWriter.writeFileChunk(md, position, content, lastChunk);
                    return null;
                });
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(null,
            new AsyncRecoveryTarget(target, recoveryExecutor),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 5),
            between(1, 5),
            between(1, 5),
            false,
            recoveryPlannerService);
        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
        sendFilesFuture.actionGet();
        Store.MetadataSnapshot targetStoreMetadata = targetStore.getMetadata(null);
        Store.RecoveryDiff recoveryDiff = targetStoreMetadata.recoveryDiff(metadata);
        assertEquals(metas.size(), recoveryDiff.identical.size());
        assertEquals(0, recoveryDiff.different.size());
        assertEquals(0, recoveryDiff.missing.size());
        IndexReader reader = DirectoryReader.open(targetStore.directory());
        assertEquals(numDocs, reader.maxDoc());
        IOUtils.close(reader, store, multiFileWriter, targetStore);
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
        final int fileChunkSizeInBytes = between(1, 4096);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final List<Translog.Operation> operations = new ArrayList<>();
        final int initialNumberOfDocs = randomIntBetween(10, 1000);
        for (int i = 0; i < initialNumberOfDocs; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, 1, SequenceNumbers.UNASSIGNED_SEQ_NO, true)));
        }
        final int numberOfDocsWithValidSequenceNumbers = randomIntBetween(10, 1000);
        for (int i = initialNumberOfDocs; i < initialNumberOfDocs + numberOfDocsWithValidSequenceNumbers; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            operations.add(new Translog.Index(index, new Engine.IndexResult(1, 1, i - initialNumberOfDocs, true)));
        }
        final long startingSeqNo = randomIntBetween(0, numberOfDocsWithValidSequenceNumbers - 1);
        final long endingSeqNo = randomLongBetween(startingSeqNo, numberOfDocsWithValidSequenceNumbers - 1);

        final Queue<Translog.Operation> shippedOps = ConcurrentCollections.newQueue();
        final AtomicLong checkpointOnTarget = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        RecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            @Override
            public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps, long timestamp, long msu,
                                                RetentionLeases retentionLeases, long mappingVersion, ActionListener<Long> listener) {
                shippedOps.addAll(operations);
                if (randomBoolean()) {
                    checkpointOnTarget.addAndGet(between(1, 20));
                }
                listener.onResponse(checkpointOnTarget.get());
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(shard, new AsyncRecoveryTarget(recoveryTarget, threadPool.generic()),
            threadPool, request, fileChunkSizeInBytes, between(1, 10), between(1, 10), between(1, 10), false, recoveryPlannerService);
        PlainActionFuture<RecoverySourceHandler.SendSnapshotResult> future = new PlainActionFuture<>();
        handler.phase2(startingSeqNo, endingSeqNo, newTranslogSnapshot(operations, emptyList()),
            randomNonNegativeLong(), randomNonNegativeLong(), RetentionLeases.EMPTY, randomNonNegativeLong(), future);
        final int expectedOps = (int) (endingSeqNo - startingSeqNo + 1);
        RecoverySourceHandler.SendSnapshotResult result = future.actionGet();
        assertThat(result.sentOperations, equalTo(expectedOps));
        List<Translog.Operation> sortedShippedOps = shippedOps.stream()
            .sorted(Comparator.comparing(Translog.Operation::seqNo))
            .collect(Collectors.toList());
        assertThat(shippedOps.size(), equalTo(expectedOps));
        for (int i = 0; i < shippedOps.size(); i++) {
            assertThat(sortedShippedOps.get(i), equalTo(operations.get(i + (int) startingSeqNo + initialNumberOfDocs)));
        }
        assertThat(result.targetLocalCheckpoint, equalTo(checkpointOnTarget.get()));
    }

    public void testSendSnapshotStopOnError() throws Exception {
        final int fileChunkSizeInBytes = between(1, 10 * 1024);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final List<Translog.Operation> ops = new ArrayList<>();
        for (int numOps = between(1, 256), i = 0; i < numOps; i++) {
            final Engine.Index index = getIndex(Integer.toString(i));
            ops.add(new Translog.Index(index, new Engine.IndexResult(1, 1, i, true)));
        }
        final AtomicBoolean wasFailed = new AtomicBoolean();
        RecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            @Override
            public void indexTranslogOperations(List<Translog.Operation> operations, int totalTranslogOps, long timestamp,
                                                long msu, RetentionLeases retentionLeases, long mappingVersion,
                                                ActionListener<Long> listener) {
                if (randomBoolean()) {
                    listener.onResponse(SequenceNumbers.NO_OPS_PERFORMED);
                } else {
                    listener.onFailure(new RuntimeException("test - failed to index"));
                    wasFailed.set(true);
                }
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(shard, new AsyncRecoveryTarget(recoveryTarget, threadPool.generic()),
            threadPool, request, fileChunkSizeInBytes, between(1, 10), between(1, 10), between(1, 10), false, recoveryPlannerService);
        PlainActionFuture<RecoverySourceHandler.SendSnapshotResult> future = new PlainActionFuture<>();
        final long startingSeqNo = randomLongBetween(0, ops.size() - 1L);
        final long endingSeqNo = randomLongBetween(startingSeqNo, ops.size() - 1L);
        handler.phase2(startingSeqNo, endingSeqNo, newTranslogSnapshot(ops, emptyList()),
            randomNonNegativeLong(), randomNonNegativeLong(), RetentionLeases.EMPTY, randomNonNegativeLong(), future);
        if (wasFailed.get()) {
            final RecoveryEngineException error = expectThrows(RecoveryEngineException.class, future::actionGet);
            assertThat(error.getMessage(), equalTo("Phase[2] failed to send/replay operations"));
            assertThat(error.getCause().getMessage(), equalTo("test - failed to index"));
        }
    }

    public void testSendOperationsConcurrently() throws Throwable {
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        Set<Long> receivedSeqNos = ConcurrentCollections.newConcurrentSet();
        long maxSeenAutoIdTimestamp = randomBoolean() ? -1 : randomNonNegativeLong();
        long maxSeqNoOfUpdatesOrDeletes = randomBoolean() ? -1 : randomNonNegativeLong();
        RetentionLeases retentionLeases = new RetentionLeases(randomNonNegativeLong(), randomNonNegativeLong(), List.of());
        long mappingVersion = randomNonNegativeLong();
        AtomicLong localCheckpoint = new AtomicLong(SequenceNumbers.NO_OPS_PERFORMED);
        int numOps = randomIntBetween(0, 1000);
        AtomicBoolean received = new AtomicBoolean();
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void indexTranslogOperations(List<Translog.Operation> operations, int receivedTotalOps,
                                                long receivedMaxSeenAutoIdTimestamp, long receivedMaxSeqNoOfUpdatesOrDeletes,
                                                RetentionLeases receivedRetentionLease, long receivedMappingVersion,
                                                ActionListener<Long> listener) {
                received.set(true);
                assertThat(receivedMaxSeenAutoIdTimestamp, equalTo(maxSeenAutoIdTimestamp));
                assertThat(receivedMaxSeqNoOfUpdatesOrDeletes, equalTo(maxSeqNoOfUpdatesOrDeletes));
                assertThat(receivedRetentionLease, equalTo(retentionLeases));
                assertThat(receivedMappingVersion, equalTo(mappingVersion));
                assertThat(receivedTotalOps, equalTo(numOps));
                for (Translog.Operation operation : operations) {
                    receivedSeqNos.add(operation.seqNo());
                }
                if (randomBoolean()) {
                    localCheckpoint.addAndGet(randomIntBetween(1, 100));
                }
                listener.onResponse(localCheckpoint.get());
            }
        };

        PlainActionFuture<RecoverySourceHandler.SendSnapshotResult> sendFuture = new PlainActionFuture<>();
        long startingSeqNo = randomIntBetween(0, 1000);
        long endingSeqNo = startingSeqNo + randomIntBetween(0, 10000);
        List<Translog.Operation> operations = generateOperations(numOps);
        Randomness.shuffle(operations);
        List<Translog.Operation> skipOperations = randomSubsetOf(operations);
        Translog.Snapshot snapshot = newTranslogSnapshot(operations, skipOperations);
        RecoverySourceHandler handler = new RecoverySourceHandler(shard, new AsyncRecoveryTarget(target, recoveryExecutor),
            threadPool, getStartRecoveryRequest(), between(1, 10 * 1024), between(1, 5), between(1, 5), between(1, 5), false,
            recoveryPlannerService);
        handler.phase2(startingSeqNo, endingSeqNo, snapshot, maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes, retentionLeases,
            mappingVersion, sendFuture);
        RecoverySourceHandler.SendSnapshotResult sendSnapshotResult = sendFuture.actionGet();
        assertTrue(received.get());
        assertThat(sendSnapshotResult.targetLocalCheckpoint, equalTo(localCheckpoint.get()));
        assertThat(sendSnapshotResult.sentOperations, equalTo(receivedSeqNos.size()));
        Set<Long> sentSeqNos = new HashSet<>();
        for (Translog.Operation op : operations) {
            if (startingSeqNo <= op.seqNo() && op.seqNo() <= endingSeqNo && skipOperations.contains(op) == false) {
                sentSeqNos.add(op.seqNo());
            }
        }
        assertThat(receivedSeqNos, equalTo(sentSeqNos));
    }

    private Engine.Index getIndex(final String id) {
        final LuceneDocument document = new LuceneDocument();
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
            new ParsedDocument(versionField, seqID, id, null, Arrays.asList(document), source, XContentType.JSON, null);
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
        List<StoreFileMetadata> metas = new ArrayList<>();
        for (StoreFileMetadata md : metadata) {
            metas.add(md);
        }

        CorruptionUtils.corruptFile(random(), FileSystemUtils.files(tempDir, (p) ->
            (p.getFileName().toString().equals("write.lock") ||
                p.getFileName().toString().startsWith("extra")) == false));
        Store targetStore = newStore(createTempDir(), false);
        MultiFileWriter multiFileWriter = new MultiFileWriter(targetStore, mock(RecoveryState.Index.class), "", logger, () -> {});
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
             @Override
            public void writeFileChunk(StoreFileMetadata md, long position, ReleasableBytesReference content, boolean lastChunk,
                                       int totalTranslogOps, ActionListener<Void> listener) {
                 ActionListener.completeWith(listener, () -> {
                     multiFileWriter.writeFileChunk(md, position, content, lastChunk);
                     return null;
                 });
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(null, new AsyncRecoveryTarget(target, recoveryExecutor), threadPool,
            request, Math.toIntExact(recoverySettings.getChunkSize().getBytes()), between(1, 8), between(1, 8), between(1, 8), false,
            recoveryPlannerService) {
            @Override
            protected void failEngine(IOException cause) {
                assertFalse(failedEngine.get());
                failedEngine.set(true);
            }
        };
        SetOnce<Exception> sendFilesError = new SetOnce<>();
        CountDownLatch latch = new CountDownLatch(1);
        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0,
            new LatchedActionListener<>(ActionListener.wrap(r -> sendFilesError.set(null), e -> sendFilesError.set(e)), latch));
        latch.await();
        assertThat(sendFilesError.get(), instanceOf(IOException.class));
        assertNotNull(ExceptionsHelper.unwrapCorruption(sendFilesError.get()));
        assertTrue(failedEngine.get());
        // ensure all chunk requests have been completed; otherwise some files on the target are left open.
        IOUtils.close(() -> terminate(threadPool), () -> threadPool = null);
        IOUtils.close(store, multiFileWriter, targetStore);
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
        List<StoreFileMetadata> metas = new ArrayList<>();
        for (StoreFileMetadata md : metadata) {
            metas.add(md);
        }
        final boolean throwCorruptedIndexException = randomBoolean();
        RecoveryTargetHandler target = new TestRecoveryTargetHandler() {
            @Override
            public void writeFileChunk(StoreFileMetadata md, long position, ReleasableBytesReference content, boolean lastChunk,
                                       int totalTranslogOps, ActionListener<Void> listener) {
                if (throwCorruptedIndexException) {
                    listener.onFailure(new RuntimeException(new CorruptIndexException("foo", "bar")));
                } else {
                    listener.onFailure(new RuntimeException("boom"));
                }
            }
        };
        RecoverySourceHandler handler = new RecoverySourceHandler(null, new AsyncRecoveryTarget(target, recoveryExecutor), threadPool,
            request, Math.toIntExact(recoverySettings.getChunkSize().getBytes()), between(1, 10), between(1, 4), between(1, 4), false,
            recoveryPlannerService) {
            @Override
            protected void failEngine(IOException cause) {
                assertFalse(failedEngine.get());
                failedEngine.set(true);
            }
        };
        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        handler.sendFiles(store, metas.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
        Exception ex = expectThrows(Exception.class, sendFilesFuture::actionGet);
        final IOException unwrappedCorruption = ExceptionsHelper.unwrapCorruption(ex);
        if (throwCorruptedIndexException) {
            assertNotNull(unwrappedCorruption);
            assertEquals(ex.getMessage(), "[File corruption occurred on recovery but checksums are ok]");
        } else {
            assertNull(unwrappedCorruption);
            assertEquals(ex.getMessage(), "boom");
        }
        assertFalse(failedEngine.get());
        IOUtils.close(store);
    }

    @SuppressWarnings("unchecked")
    public void testThrowExceptionOnPrimaryRelocatedBeforePhase1Started() throws IOException {
        final RecoverySettings recoverySettings = new RecoverySettings(Settings.EMPTY, service);
        final StartRecoveryRequest request = getStartRecoveryRequest();
        final IndexShard shard = mock(IndexShard.class);
        when(shard.seqNoStats()).thenReturn(mock(SeqNoStats.class));
        when(shard.segmentStats(anyBoolean(), anyBoolean())).thenReturn(mock(SegmentsStats.class));
        when(shard.isRelocatedPrimary()).thenReturn(true);
        when(shard.acquireSafeIndexCommit()).thenReturn(mock(Engine.IndexCommitRef.class));
        doAnswer(invocation -> {
            ((ActionListener<Releasable>)invocation.getArguments()[0]).onResponse(() -> {});
            return null;
        }).when(shard).acquirePrimaryOperationPermit(any(), anyString(), anyObject());

        final IndexMetadata.Builder indexMetadata = IndexMetadata.builder("test").settings(Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0,5))
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(1,5))
            .put(IndexMetadata.SETTING_VERSION_CREATED, VersionUtils.randomVersion(random()))
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID(random())));
        if (randomBoolean()) {
            indexMetadata.state(IndexMetadata.State.CLOSE);
        }
        when(shard.indexSettings()).thenReturn(new IndexSettings(indexMetadata.build(), Settings.EMPTY));

        final AtomicBoolean phase1Called = new AtomicBoolean();
        final AtomicBoolean prepareTargetForTranslogCalled = new AtomicBoolean();
        final AtomicBoolean phase2Called = new AtomicBoolean();
        final RecoverySourceHandler handler = new RecoverySourceHandler(
            shard,
            mock(RecoveryTargetHandler.class),
            threadPool,
            request,
            Math.toIntExact(recoverySettings.getChunkSize().getBytes()),
            between(1, 8),
            between(1, 8),
            between(1, 5),
            false,
            recoveryPlannerService) {

            @Override
            void phase1(IndexCommit snapshot, long startingSeqNo, IntSupplier translogOps, ActionListener<SendFileResult> listener) {
                phase1Called.set(true);
                super.phase1(snapshot, startingSeqNo, translogOps, listener);
            }

            @Override
            void prepareTargetForTranslog(int totalTranslogOps, ActionListener<TimeValue> listener) {
                prepareTargetForTranslogCalled.set(true);
                super.prepareTargetForTranslog(totalTranslogOps, listener);
            }

            @Override
            void phase2(long startingSeqNo, long endingSeqNo, Translog.Snapshot snapshot,
                        long maxSeenAutoIdTimestamp, long maxSeqNoOfUpdatesOrDeletes, RetentionLeases retentionLeases,
                        long mappingVersion, ActionListener<SendSnapshotResult> listener) throws IOException {
                phase2Called.set(true);
                super.phase2(startingSeqNo, endingSeqNo, snapshot,
                    maxSeenAutoIdTimestamp, maxSeqNoOfUpdatesOrDeletes, retentionLeases, mappingVersion, listener);
            }

        };
        PlainActionFuture<RecoveryResponse> future = new PlainActionFuture<>();
        expectThrows(IndexShardRelocatedException.class, () -> {
            handler.recoverToTarget(future);
            future.actionGet();
        });
        assertFalse(phase1Called.get());
        assertFalse(prepareTargetForTranslogCalled.get());
        assertFalse(phase2Called.get());
    }

    @SuppressWarnings("unchecked")
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

    public void testSendFileChunksConcurrently() throws Exception {
        final IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        final List<FileChunkResponse> unrepliedChunks = new CopyOnWriteArrayList<>();
        final AtomicInteger sentChunks = new AtomicInteger();
        final TestRecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            final AtomicLong chunkNumberGenerator = new AtomicLong();
            @Override
            public void writeFileChunk(StoreFileMetadata md, long position, ReleasableBytesReference content, boolean lastChunk,
                                       int totalTranslogOps, ActionListener<Void> listener) {
                final long chunkNumber = chunkNumberGenerator.getAndIncrement();
                logger.info("--> write chunk name={} seq={}, position={}", md.name(), chunkNumber, position);
                unrepliedChunks.add(new FileChunkResponse(chunkNumber, listener));
                sentChunks.incrementAndGet();
            }
        };
        final int maxConcurrentChunks = between(1, 8);
        final int chunkSize = between(1, 32);
        final RecoverySourceHandler handler = new RecoverySourceHandler(shard, recoveryTarget, threadPool, getStartRecoveryRequest(),
            chunkSize, maxConcurrentChunks, between(1, 10), between(1, 5), false, recoveryPlannerService);
        Store store = newStore(createTempDir(), false);
        List<StoreFileMetadata> files = generateFiles(store, between(1, 10), () -> between(1, chunkSize * 20));
        int totalChunks = files.stream().mapToInt(md -> ((int) md.length() + chunkSize - 1) / chunkSize).sum();
        PlainActionFuture<Void> sendFilesFuture = new PlainActionFuture<>();
        handler.sendFiles(store, files.toArray(new StoreFileMetadata[0]), () -> 0, sendFilesFuture);
        assertBusy(() -> {
            assertThat(sentChunks.get(), equalTo(Math.min(totalChunks, maxConcurrentChunks)));
            assertThat(unrepliedChunks, hasSize(sentChunks.get()));
        });

        List<FileChunkResponse> ackedChunks = new ArrayList<>();
        while (sentChunks.get() < totalChunks || unrepliedChunks.isEmpty() == false) {
            List<FileChunkResponse> chunksToAck = randomSubsetOf(between(1, unrepliedChunks.size()), unrepliedChunks);
            unrepliedChunks.removeAll(chunksToAck);
            ackedChunks.addAll(chunksToAck);
            ackedChunks.sort(Comparator.comparing(c -> c.chunkNumber));
            int checkpoint = -1;
            for (int i = 0; i < ackedChunks.size(); i++) {
                if (i != ackedChunks.get(i).chunkNumber) {
                    break;
                } else {
                    checkpoint = i;
                }
            }
            int chunksToSend = Math.min(
                totalChunks - sentChunks.get(),                             // limited by the remaining chunks
                maxConcurrentChunks - (sentChunks.get() - 1 - checkpoint)); // limited by the buffering chunks

            int expectedSentChunks = sentChunks.get() + chunksToSend;
            int expectedUnrepliedChunks = unrepliedChunks.size() + chunksToSend;
            chunksToAck.forEach(c -> c.listener.onResponse(null));
            assertBusy(() -> {
                assertThat(sentChunks.get(), equalTo(expectedSentChunks));
                assertThat(unrepliedChunks, hasSize(expectedUnrepliedChunks));
            });
        }
        sendFilesFuture.actionGet();
        store.close();
    }

    public void testSendFileChunksStopOnError() throws Exception {
        final List<FileChunkResponse> unrepliedChunks = new CopyOnWriteArrayList<>();
        final AtomicInteger sentChunks = new AtomicInteger();
        final TestRecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            final AtomicLong chunkNumberGenerator = new AtomicLong();
            @Override
            public void writeFileChunk(StoreFileMetadata md, long position, ReleasableBytesReference content, boolean lastChunk,
                                       int totalTranslogOps, ActionListener<Void> listener) {
                final long chunkNumber = chunkNumberGenerator.getAndIncrement();
                logger.info("--> write chunk name={} seq={}, position={}", md.name(), chunkNumber, position);
                unrepliedChunks.add(new FileChunkResponse(chunkNumber, listener));
                sentChunks.incrementAndGet();
            }
        };
        final int maxConcurrentChunks = between(1, 4);
        final int chunkSize = between(1, 16);
        final RecoverySourceHandler handler = new RecoverySourceHandler(null, new AsyncRecoveryTarget(recoveryTarget, recoveryExecutor),
            threadPool, getStartRecoveryRequest(), chunkSize, maxConcurrentChunks, between(1, 5), between(1, 5),
            false, recoveryPlannerService);
        Store store = newStore(createTempDir(), false);
        List<StoreFileMetadata> files = generateFiles(store, between(1, 10), () -> between(1, chunkSize * 20));
        int totalChunks = files.stream().mapToInt(md -> ((int) md.length() + chunkSize - 1) / chunkSize).sum();
        SetOnce<Exception> sendFilesError = new SetOnce<>();
        CountDownLatch sendFilesLatch = new CountDownLatch(1);
        handler.sendFiles(store, files.toArray(new StoreFileMetadata[0]), () -> 0,
            new LatchedActionListener<>(ActionListener.wrap(r -> sendFilesError.set(null), e -> sendFilesError.set(e)), sendFilesLatch));
        assertBusy(() -> assertThat(sentChunks.get(), equalTo(Math.min(totalChunks, maxConcurrentChunks))));
        List<FileChunkResponse> failedChunks = randomSubsetOf(between(1, unrepliedChunks.size()), unrepliedChunks);
        CountDownLatch replyLatch = new CountDownLatch(failedChunks.size());
        failedChunks.forEach(c -> {
            c.listener.onFailure(new IllegalStateException("test chunk exception"));
            replyLatch.countDown();
        });
        replyLatch.await();
        unrepliedChunks.removeAll(failedChunks);
        unrepliedChunks.forEach(c -> {
            if (randomBoolean()) {
                c.listener.onFailure(new RuntimeException("test"));
            } else {
                c.listener.onResponse(null);
            }
        });
        sendFilesLatch.await();
        assertThat(sendFilesError.get(), instanceOf(IllegalStateException.class));
        assertThat(sendFilesError.get().getMessage(), containsString("test chunk exception"));
        assertThat("no more chunks should be sent", sentChunks.get(), equalTo(Math.min(totalChunks, maxConcurrentChunks)));
        store.close();
    }

    public void testCancelRecoveryDuringPhase1() throws Exception {
        Store store = newStore(createTempDir("source"), false);
        IndexShard shard = mock(IndexShard.class);
        when(shard.store()).thenReturn(store);
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
        AtomicBoolean wasCancelled = new AtomicBoolean();
        SetOnce<Runnable> cancelRecovery = new SetOnce<>();
        final TestRecoveryTargetHandler recoveryTarget = new TestRecoveryTargetHandler() {
            @Override
            public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                        List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {
                recoveryExecutor.execute(() -> listener.onResponse(null));
                if (randomBoolean()) {
                    wasCancelled.set(true);
                    cancelRecovery.get().run();
                }
            }

            @Override
            public void writeFileChunk(StoreFileMetadata md, long position, ReleasableBytesReference content,
                                       boolean lastChunk, int totalTranslogOps, ActionListener<Void> listener) {
                recoveryExecutor.execute(() -> listener.onResponse(null));
                if (rarely()) {
                    wasCancelled.set(true);
                    cancelRecovery.get().run();
                }
            }

            @Override
            public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                                   ActionListener<Void> listener) {
                recoveryExecutor.execute(() -> listener.onResponse(null));
                if (randomBoolean()) {
                    wasCancelled.set(true);
                    cancelRecovery.get().run();
                }
            }
        };
        final StartRecoveryRequest startRecoveryRequest = getStartRecoveryRequest();
        final RecoverySourceHandler handler = new RecoverySourceHandler(shard,
            recoveryTarget,
            threadPool,
            startRecoveryRequest,
            between(1, 16),
            between(1, 4),
            between(1, 4),
            between(1, 4),
            false,
            recoveryPlannerService) {
            @Override
            void createRetentionLease(long startingSeqNo, ActionListener<RetentionLease> listener) {
                final String leaseId = ReplicationTracker.getPeerRecoveryRetentionLeaseId(startRecoveryRequest.targetNode().getId());
                listener.onResponse(new RetentionLease(leaseId, startingSeqNo, threadPool.absoluteTimeInMillis(),
                    ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE));
            }
        };
        cancelRecovery.set(() -> handler.cancel("test"));
        final StepListener<RecoverySourceHandler.SendFileResult> phase1Listener = new StepListener<>();
        try {
            final CountDownLatch latch = new CountDownLatch(1);
            handler.phase1(DirectoryReader.listCommits(dir).get(0),
                0,
                () -> 0,
                new LatchedActionListener<>(phase1Listener, latch));
            latch.await();
            phase1Listener.result();
        } catch (Exception e) {
            assertTrue(wasCancelled.get());
            assertNotNull(ExceptionsHelper.unwrap(e, CancellableThreads.ExecutionCancelledException.class));
        }
        store.close();
    }

    public void testVerifySeqNoStatsWhenRecoverWithSyncId() throws Exception {
        IndexShard shard = mock(IndexShard.class);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        RecoverySourceHandler handler = new RecoverySourceHandler(
            shard,
            new TestRecoveryTargetHandler(),
            threadPool,
            getStartRecoveryRequest(),
            between(1, 16),
            between(1, 4),
            between(1, 4),
            between(1, 4),
            false,
            recoveryPlannerService);

        String syncId = UUIDs.randomBase64UUID();
        int numDocs = between(0, 1000);
        long localCheckpoint = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        long maxSeqNo = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
        assertTrue(handler.canSkipPhase1(
            newMetadataSnapshot(syncId, Long.toString(localCheckpoint), Long.toString(maxSeqNo), numDocs),
            newMetadataSnapshot(syncId, Long.toString(localCheckpoint), Long.toString(maxSeqNo), numDocs)));

        AssertionError error = expectThrows(AssertionError.class, () -> {
            long localCheckpointOnTarget = randomValueOtherThan(localCheckpoint,
                () -> randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE));
            long maxSeqNoOnTarget = randomValueOtherThan(maxSeqNo,
                () -> randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE));
            handler.canSkipPhase1(newMetadataSnapshot(syncId, Long.toString(localCheckpoint), Long.toString(maxSeqNo), numDocs),
                newMetadataSnapshot(syncId, Long.toString(localCheckpointOnTarget), Long.toString(maxSeqNoOnTarget), numDocs));
        });
        assertThat(error.getMessage(), containsString("try to recover [index][1] with sync id but seq_no stats are mismatched:"));
    }

    public void testRecoveryPlannerServiceIsUsed() throws Exception {
        try (Store store = newStore(createTempDir("source"), false)) {
            IndexShard shard = mock(IndexShard.class);
            when(shard.store()).thenReturn(store);
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
            when(shard.state()).thenReturn(IndexShardState.STARTED);

            TestRecoveryTargetHandler recoveryTarget = new Phase1RecoveryTargetHandler();
            AtomicReference<ShardRecoveryPlan> computedRecoveryPlanRef = new AtomicReference<>();
            RecoverySourceHandler handler = new RecoverySourceHandler(
                shard,
                recoveryTarget,
                threadPool,
                getStartRecoveryRequest(),
                between(1, 16),
                between(1, 4),
                between(1, 4),
                between(1, 4),
                true,
                recoveryPlannerService
            ) {
                @Override
                void createRetentionLease(long startingSeqNo, ActionListener<RetentionLease> listener) {
                    listener.onResponse(new RetentionLease("id", startingSeqNo, 0, "test"));
                }

                @Override
                void recoverFilesFromSourceAndSnapshot(ShardRecoveryPlan shardRecoveryPlan,
                                                       Store store,
                                                       StopWatch stopWatch,
                                                       ActionListener<SendFileResult> listener) {
                    assertThat(computedRecoveryPlanRef.compareAndSet(null, shardRecoveryPlan), equalTo(true));
                    super.recoverFilesFromSourceAndSnapshot(shardRecoveryPlan, store, stopWatch, listener);
                }
            };
            PlainActionFuture<RecoverySourceHandler.SendFileResult> phase1Listener = PlainActionFuture.newFuture();
            IndexCommit indexCommit = DirectoryReader.listCommits(dir).get(0);
            handler.phase1(indexCommit, 0, () -> 0, phase1Listener);
            phase1Listener.get();

            ShardRecoveryPlan computedRecoveryPlan = computedRecoveryPlanRef.get();
            assertThat(computedRecoveryPlan, is(notNullValue()));

            Set<String> sourceFilesToRecover = computedRecoveryPlan.getSourceFilesToRecover()
                .stream()
                .map(StoreFileMetadata::name)
                .collect(Collectors.toSet());
            assertThat(sourceFilesToRecover, equalTo(new HashSet<>(indexCommit.getFileNames())));
        }
    }

    public void testSnapshotFilesThatFailToDownloadAreSentFromSource() throws Exception {
        try (Store store = newStore(createTempDir("source"), false)) {
            IndexShard shard = mock(IndexShard.class);
            when(shard.store()).thenReturn(store);
            when(shard.state()).thenReturn(IndexShardState.STARTED);

            final ShardRecoveryPlan shardRecoveryPlan = createShardRecoveryPlan(store, randomIntBetween(10, 20), randomIntBetween(10, 20));

            final ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover = shardRecoveryPlan.getSnapshotFilesToRecover();
            final List<String> fileNamesToBeRecoveredFromSnapshot = snapshotFilesToRecover.getSnapshotFiles()
                .stream()
                .map(fileInfo -> fileInfo.metadata().name())
                .collect(Collectors.toList());

            final List<String> sourceFilesToRecover =
                shardRecoveryPlan.getSourceFilesToRecover().stream().map(StoreFileMetadata::name).collect(Collectors.toList());

            Set<String> filesFailedToDownload = Collections.synchronizedSet(new HashSet<>());
            Set<String> filesRecoveredFromSource = Collections.synchronizedSet(new HashSet<>());
            Set<String> filesRecoveredFromSnapshot = Collections.synchronizedSet(new HashSet<>());
            TestRecoveryTargetHandler recoveryTarget = new Phase1RecoveryTargetHandler() {
                @Override
                public void restoreFileFromSnapshot(String repository,
                                                    IndexId indexId,
                                                    BlobStoreIndexShardSnapshot.FileInfo snapshotFile,
                                                    ActionListener<Void> listener) {
                    assertThat(repository, is(equalTo(snapshotFilesToRecover.getRepository())));
                    assertThat(indexId, is(equalTo(snapshotFilesToRecover.getIndexId())));
                    assertThat(containsSnapshotFile(snapshotFilesToRecover, snapshotFile), is(equalTo(true)));
                    String fileName = snapshotFile.metadata().name();

                    if (randomBoolean()) {
                        filesFailedToDownload.add(fileName);
                        if (randomBoolean()) {
                            listener.onFailure(randomFrom(new IOException("Failure"), new CorruptIndexException("idx", "")));
                        } else {
                            throw new RuntimeException("Unexpected exception");
                        }
                    } else {
                        filesRecoveredFromSnapshot.add(fileName);
                        listener.onResponse(null);
                    }
                }

                @Override
                public void writeFileChunk(StoreFileMetadata fileMetadata,
                                           long position,
                                           ReleasableBytesReference content,
                                           boolean lastChunk,
                                           int totalTranslogOps,
                                           ActionListener<Void> listener) {
                    filesRecoveredFromSource.add(fileMetadata.name());
                    listener.onResponse(null);
                }
            };

            RecoverySourceHandler handler = new RecoverySourceHandler(
                shard,
                recoveryTarget,
                threadPool,
                getStartRecoveryRequest(),
                between(1, 16),
                between(1, 4),
                between(1, 4),
                between(1, 4),
                true,
                recoveryPlannerService) {
                @Override
                void createRetentionLease(long startingSeqNo, ActionListener<RetentionLease> listener) {
                    listener.onResponse(new RetentionLease("id", startingSeqNo, 0, "test"));
                }
            };

            PlainActionFuture<RecoverySourceHandler.SendFileResult> future = PlainActionFuture.newFuture();
            handler.recoverFilesFromSourceAndSnapshot(shardRecoveryPlan,
                store,
                mock(StopWatch.class),
                future
            );
            future.actionGet();

            filesRecoveredFromSource.removeAll(sourceFilesToRecover);
            filesRecoveredFromSource.removeAll(filesFailedToDownload);
            assertThat(filesRecoveredFromSource, is(empty()));

            assertThat(fileNamesToBeRecoveredFromSnapshot.containsAll(filesRecoveredFromSnapshot), is(equalTo(true)));
        }
    }

    public void testSnapshotFilesRequestAreSentConcurrently() throws Exception {
        try (Store store = newStore(createTempDir("source"), false)) {
            IndexShard shard = mock(IndexShard.class);
            when(shard.store()).thenReturn(store);
            when(shard.state()).thenReturn(IndexShardState.STARTED);

            ShardRecoveryPlan shardRecoveryPlan = createShardRecoveryPlan(store, 0, randomIntBetween(10, 20));
            final int snapshotFileToRecoverCount = shardRecoveryPlan.getSnapshotFilesToRecover().size();

            AtomicInteger recoverSnapshotFileRequests = new AtomicInteger();
            List<RecoverSnapshotFileResponse> unrespondedRecoverSnapshotFiles = new CopyOnWriteArrayList<>();
            TestRecoveryTargetHandler recoveryTarget = new Phase1RecoveryTargetHandler() {
                @Override
                public void restoreFileFromSnapshot(String repository,
                                                    IndexId indexId,
                                                    BlobStoreIndexShardSnapshot.FileInfo snapshotFile,
                                                    ActionListener<Void> listener) {
                    unrespondedRecoverSnapshotFiles.add(new RecoverSnapshotFileResponse(snapshotFile, listener));
                    recoverSnapshotFileRequests.incrementAndGet();
                }

                @Override
                public void writeFileChunk(StoreFileMetadata fileMetadata,
                                           long position,
                                           ReleasableBytesReference content,
                                           boolean lastChunk,
                                           int totalTranslogOps,
                                           ActionListener<Void> listener) {
                    assert false : "Unexpected call";
                }
            };

            int maxConcurrentSnapshotFileDownloads = between(1, 4);
            RecoverySourceHandler handler = new RecoverySourceHandler(
                shard,
                recoveryTarget,
                threadPool,
                getStartRecoveryRequest(),
                between(1, 16),
                between(1, 4),
                between(1, 4),
                maxConcurrentSnapshotFileDownloads,
                true,
                null) {
                @Override
                void createRetentionLease(long startingSeqNo, ActionListener<RetentionLease> listener) {
                    listener.onResponse(new RetentionLease("id", startingSeqNo, 0, "test"));
                }
            };

            PlainActionFuture<RecoverySourceHandler.SendFileResult> future = PlainActionFuture.newFuture();
            handler.recoverFilesFromSourceAndSnapshot(shardRecoveryPlan, store, mock(StopWatch.class), future);

            assertBusy(() -> {
                assertThat(recoverSnapshotFileRequests.get(),
                    equalTo(Math.min(snapshotFileToRecoverCount, maxConcurrentSnapshotFileDownloads)));
                assertThat(unrespondedRecoverSnapshotFiles, hasSize(recoverSnapshotFileRequests.get()));
            });

            while (recoverSnapshotFileRequests.get() < snapshotFileToRecoverCount || unrespondedRecoverSnapshotFiles.isEmpty() == false) {
                List<RecoverSnapshotFileResponse> recoverSnapshotFilesToRespond =
                    randomSubsetOf(between(1, unrespondedRecoverSnapshotFiles.size()), unrespondedRecoverSnapshotFiles);
                unrespondedRecoverSnapshotFiles.removeAll(recoverSnapshotFilesToRespond);

                int newRecoverSnapshotFileRequestCount = Math.min(
                    Math.min(recoverSnapshotFilesToRespond.size(), maxConcurrentSnapshotFileDownloads),
                    snapshotFileToRecoverCount - recoverSnapshotFileRequests.get()
                );

                int expectedSentRecoverSnapshotFiles = recoverSnapshotFileRequests.get() + newRecoverSnapshotFileRequestCount;
                int expectedUnAckedRecoverSnapshotFiles = unrespondedRecoverSnapshotFiles.size() + newRecoverSnapshotFileRequestCount;
                recoverSnapshotFilesToRespond.forEach(c -> c.listener.onResponse(null));
                assertBusy(() -> {
                    assertThat(recoverSnapshotFileRequests.get(), equalTo(expectedSentRecoverSnapshotFiles));
                    assertThat(unrespondedRecoverSnapshotFiles, hasSize(expectedUnAckedRecoverSnapshotFiles));
                });
            }

            future.actionGet();
        }
    }

    public void testDownloadSnapshotFilesRequestStopAfterCancelling() throws Exception {
        try (Store store = newStore(createTempDir("source"), false)) {
            IndexShard shard = mock(IndexShard.class);
            when(shard.store()).thenReturn(store);
            when(shard.state()).thenReturn(IndexShardState.STARTED);

            ShardRecoveryPlan shardRecoveryPlan = createShardRecoveryPlan(store, 0, randomIntBetween(10, 20));

            CountDownLatch downloadSnapshotFileReceived = new CountDownLatch(1);
            List<RecoverSnapshotFileResponse> unrespondedRecoverSnapshotFiles = new CopyOnWriteArrayList<>();
            TestRecoveryTargetHandler recoveryTarget = new Phase1RecoveryTargetHandler() {
                @Override
                public void restoreFileFromSnapshot(String repository,
                                                    IndexId indexId,
                                                    BlobStoreIndexShardSnapshot.FileInfo snapshotFile,
                                                    ActionListener<Void> listener) {
                    assert unrespondedRecoverSnapshotFiles.isEmpty(): "Unexpected call";

                    unrespondedRecoverSnapshotFiles.add(new RecoverSnapshotFileResponse(snapshotFile, listener));
                    downloadSnapshotFileReceived.countDown();
                }

                @Override
                public void writeFileChunk(StoreFileMetadata fileMetadata,
                                           long position,
                                           ReleasableBytesReference content,
                                           boolean lastChunk,
                                           int totalTranslogOps,
                                           ActionListener<Void> listener) {
                    assert false : "Unexpected call";
                }
            };

            int maxConcurrentSnapshotFileDownloads = 1;
            RecoverySourceHandler handler = new RecoverySourceHandler(
                shard,
                recoveryTarget,
                threadPool,
                getStartRecoveryRequest(),
                between(1, 16),
                between(1, 4),
                between(1, 4),
                maxConcurrentSnapshotFileDownloads,
                true,
                null) {
                @Override
                void createRetentionLease(long startingSeqNo, ActionListener<RetentionLease> listener) {
                    listener.onResponse(new RetentionLease("id", startingSeqNo, 0, "test"));
                }
            };

            PlainActionFuture<RecoverySourceHandler.SendFileResult> future = PlainActionFuture.newFuture();
            handler.recoverFilesFromSourceAndSnapshot(shardRecoveryPlan, store, mock(StopWatch.class), future);

            downloadSnapshotFileReceived.await();
            assertThat(unrespondedRecoverSnapshotFiles.size(), is(equalTo(1)));

            handler.cancel("test");

            RecoverSnapshotFileResponse recoverSnapshotFileResponse = unrespondedRecoverSnapshotFiles.remove(0);
            recoverSnapshotFileResponse.listener.onResponse(null);

            expectThrows(Exception.class, future::get);

            assertThat(unrespondedRecoverSnapshotFiles.isEmpty(), is(equalTo(true)));
        }
    }

    public void testWaitsForOutstandingRestoreFileFromSnapshotRequestsToNotifyCancellation() throws Exception {
        try (Store store = newStore(createTempDir("source"), false)) {
            IndexShard shard = mock(IndexShard.class);
            when(shard.store()).thenReturn(store);
            when(shard.state()).thenReturn(IndexShardState.STARTED);

            ShardRecoveryPlan shardRecoveryPlan = createShardRecoveryPlan(store, 0, randomIntBetween(10, 20));

            int maxConcurrentSnapshotFileDownloads = randomIntBetween(2, 4);
            CountDownLatch downloadSnapshotFileReceived = new CountDownLatch(maxConcurrentSnapshotFileDownloads);
            List<RecoverSnapshotFileResponse> unrespondedRecoverSnapshotFiles = new CopyOnWriteArrayList<>();
            TestRecoveryTargetHandler recoveryTarget = new Phase1RecoveryTargetHandler() {
                @Override
                public void restoreFileFromSnapshot(String repository,
                                                    IndexId indexId,
                                                    BlobStoreIndexShardSnapshot.FileInfo snapshotFile,
                                                    ActionListener<Void> listener) {
                    unrespondedRecoverSnapshotFiles.add(new RecoverSnapshotFileResponse(snapshotFile, listener));
                    downloadSnapshotFileReceived.countDown();
                }

                @Override
                public void writeFileChunk(StoreFileMetadata fileMetadata,
                                           long position,
                                           ReleasableBytesReference content,
                                           boolean lastChunk,
                                           int totalTranslogOps,
                                           ActionListener<Void> listener) {
                    assert false : "Unexpected call";
                }
            };

            RecoverySourceHandler handler = new RecoverySourceHandler(
                shard,
                recoveryTarget,
                threadPool,
                getStartRecoveryRequest(),
                between(1, 16),
                between(1, 4),
                between(1, 4),
                maxConcurrentSnapshotFileDownloads,
                true,
                null) {
                @Override
                void createRetentionLease(long startingSeqNo, ActionListener<RetentionLease> listener) {
                    listener.onResponse(new RetentionLease("id", startingSeqNo, 0, "test"));
                }
            };

            PlainActionFuture<RecoverySourceHandler.SendFileResult> future = PlainActionFuture.newFuture();
            handler.recoverFilesFromSourceAndSnapshot(shardRecoveryPlan, store, mock(StopWatch.class), future);

            downloadSnapshotFileReceived.await();
            assertThat(unrespondedRecoverSnapshotFiles.size(), is(equalTo(maxConcurrentSnapshotFileDownloads)));

            handler.cancel("test");

            assertThat(future.isDone(), is(equalTo(false)));
            for (int i = 0; i < unrespondedRecoverSnapshotFiles.size(); i++) {
                RecoverSnapshotFileResponse snapshotFileResponse = unrespondedRecoverSnapshotFiles.get(i);
                if (randomBoolean()) {
                    snapshotFileResponse.listener.onResponse(null);
                } else {
                    snapshotFileResponse.listener.onFailure(new RuntimeException("boom"));
                }

                if (i < unrespondedRecoverSnapshotFiles.size() - 1) {
                    assertThat(future.isDone(), is(equalTo(false)));
                }
            }

            expectThrows(Exception.class, future::get);
        }
    }

    private boolean containsSnapshotFile(ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover,
                                         BlobStoreIndexShardSnapshot.FileInfo snapshotFile) {
        return snapshotFilesToRecover.getSnapshotFiles().stream().anyMatch(f -> f.metadata().isSame(snapshotFile.metadata()));
    }

    private ShardRecoveryPlan createShardRecoveryPlan(Store store, int sourceFileCount, int snapshotFileCount) throws Exception {
        List<StoreFileMetadata> sourceFiles = generateFiles(store, snapshotFileCount + sourceFileCount, () -> randomIntBetween(1, 100));
        Store.MetadataSnapshot metadata = new Store.MetadataSnapshot(
            sourceFiles.stream().collect(Collectors.toMap(StoreFileMetadata::name, Function.identity())),
            emptyMap(),
            0
        );

        ByteSizeValue partSize = new ByteSizeValue(Long.MAX_VALUE, ByteSizeUnit.BYTES);

        List<StoreFileMetadata> filesToRecoverFromSource = sourceFiles.subList(0, sourceFileCount);
        List<StoreFileMetadata> filesToRecoverFromSnapshot = sourceFiles.subList(sourceFileCount, sourceFiles.size());

        List<BlobStoreIndexShardSnapshot.FileInfo> snapshotFiles = new ArrayList<>(snapshotFileCount);
        for (StoreFileMetadata storeFileMetadata : filesToRecoverFromSnapshot) {
            snapshotFiles.add(new BlobStoreIndexShardSnapshot.FileInfo(storeFileMetadata.name(), storeFileMetadata, partSize));
        }

        IndexId indexId = new IndexId("index", "id");
        String repository = "repo";
        ShardRecoveryPlan.SnapshotFilesToRecover snapshotFilesToRecover = new ShardRecoveryPlan.SnapshotFilesToRecover(indexId,
            repository,
            snapshotFiles
        );

        return new ShardRecoveryPlan(snapshotFilesToRecover,
            filesToRecoverFromSource,
            emptyList(),
            0,
            0,
            metadata
        );
    }

    private Store.MetadataSnapshot newMetadataSnapshot(String syncId, String localCheckpoint, String maxSeqNo, int numDocs) {
        Map<String, String> userData = new HashMap<>();
        userData.put(Engine.SYNC_COMMIT_ID, syncId);
        if (localCheckpoint != null) {
            userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, localCheckpoint);
        }
        if (maxSeqNo != null) {
            userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, maxSeqNo);
        }
        return new Store.MetadataSnapshot(Collections.emptyMap(), userData, numDocs);
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

    static final class FileChunkResponse {
        final long chunkNumber;
        final ActionListener<Void> listener;

        FileChunkResponse(long chunkNumber, ActionListener<Void> listener) {
            this.chunkNumber = chunkNumber;
            this.listener = listener;
        }
    }

    private List<StoreFileMetadata> generateFiles(Store store, int numFiles, IntSupplier fileSizeSupplier) throws IOException {
        List<StoreFileMetadata> files = new ArrayList<>();
        for (int i = 0; i < numFiles; i++) {
            byte[] buffer = randomByteArrayOfLength(fileSizeSupplier.getAsInt());
            CRC32 digest = new CRC32();
            digest.update(buffer, 0, buffer.length);
            StoreFileMetadata md = new StoreFileMetadata("test-" + i, buffer.length + 8,
                Store.digestToString(digest.getValue()), org.apache.lucene.util.Version.LATEST.toString());
            try (OutputStream out = new IndexOutputOutputStream(store.createVerifyingOutput(md.name(), md, IOContext.DEFAULT))) {
                out.write(buffer);
                out.write(Numbers.longToBytes(digest.getValue()));
            }
            store.directory().sync(Collections.singleton(md.name()));
            files.add(md);
        }
        return files;
    }

    class TestRecoveryTargetHandler implements RecoveryTargetHandler {
        @Override
        public void prepareForTranslogOperations(int totalTranslogOps, ActionListener<Void> listener) {
        }

        @Override
        public void finalizeRecovery(long globalCheckpoint, long trimAboveSeqNo, ActionListener<Void> listener) {
        }

        @Override
        public void handoffPrimaryContext(ReplicationTracker.PrimaryContext primaryContext, ActionListener<Void> listener) {
        }

        @Override
        public void indexTranslogOperations(
                final List<Translog.Operation> operations,
                final int totalTranslogOps,
                final long timestamp,
                final long msu,
                final RetentionLeases retentionLeases,
                final long mappingVersion,
                final ActionListener<Long> listener) {
        }

        @Override
        public void receiveFileInfo(List<String> phase1FileNames, List<Long> phase1FileSizes, List<String> phase1ExistingFileNames,
                                    List<Long> phase1ExistingFileSizes, int totalTranslogOps, ActionListener<Void> listener) {

        }

        @Override
        public void cleanFiles(int totalTranslogOps, long globalCheckpoint, Store.MetadataSnapshot sourceMetadata,
                               ActionListener<Void> listener) {
        }

        @Override
        public void restoreFileFromSnapshot(String repository, IndexId indexId, BlobStoreIndexShardSnapshot.FileInfo snapshotFile,
                                            ActionListener<Void> listener) {
        }

        @Override
        public void writeFileChunk(StoreFileMetadata fileMetadata, long position, ReleasableBytesReference content, boolean lastChunk,
                                   int totalTranslogOps, ActionListener<Void> listener) {
        }
    }

    class Phase1RecoveryTargetHandler extends TestRecoveryTargetHandler {
        @Override
        public void receiveFileInfo(List<String> phase1FileNames,
                                    List<Long> phase1FileSizes,
                                    List<String> phase1ExistingFileNames,
                                    List<Long> phase1ExistingFileSizes,
                                    int totalTranslogOps,
                                    ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public void writeFileChunk(StoreFileMetadata fileMetadata,
                                   long position,
                                   ReleasableBytesReference content,
                                   boolean lastChunk,
                                   int totalTranslogOps,
                                   ActionListener<Void> listener) {
            listener.onResponse(null);
        }

        @Override
        public void cleanFiles(int totalTranslogOps,
                               long globalCheckpoint,
                               Store.MetadataSnapshot sourceMetadata,
                               ActionListener<Void> listener) {
            listener.onResponse(null);
        }
    }

    private Translog.Snapshot newTranslogSnapshot(List<Translog.Operation> operations, List<Translog.Operation> operationsToSkip) {
        Iterator<Translog.Operation> iterator = operations.iterator();
        return new Translog.Snapshot() {
            int skippedCount = 0;

            @Override
            public int totalOperations() {
                return operations.size();
            }

            @Override
            public int skippedOperations() {
                return skippedCount;
            }

            @Override
            public Translog.Operation next() {
                while (iterator.hasNext()) {
                    Translog.Operation op = iterator.next();
                    if (operationsToSkip.contains(op)) {
                        skippedCount++;
                    } else {
                        return op;
                    }
                }
                return null;
            }

            @Override
            public void close() {

            }
        };
    }

    private static List<Translog.Operation> generateOperations(int numOps) {
        final List<Translog.Operation> operations = new ArrayList<>(numOps);
        final byte[] source = "{}".getBytes(StandardCharsets.UTF_8);
        final Set<Long> seqNos = new HashSet<>();
        for (int i = 0; i < numOps; i++) {
            final long seqNo = randomValueOtherThanMany(n -> seqNos.add(n) == false, ESTestCase::randomNonNegativeLong);
            final Translog.Operation op;
            if (randomBoolean()) {
                op = new Translog.Index("id", seqNo, randomNonNegativeLong(), randomNonNegativeLong(), source, null, -1);
            } else if (randomBoolean()) {
                op = new Translog.Delete("id", seqNo, randomNonNegativeLong(), randomNonNegativeLong());
            } else {
                op = new Translog.NoOp(seqNo, randomNonNegativeLong(), "test");
            }
            operations.add(op);
        }
        return operations;
    }

    static class RecoverSnapshotFileResponse {
        final BlobStoreIndexShardSnapshot.FileInfo fileInfo;
        final ActionListener<Void> listener;

        RecoverSnapshotFileResponse(BlobStoreIndexShardSnapshot.FileInfo fileInfo, ActionListener<Void> listener) {
            this.fileInfo = fileInfo;
            this.listener = listener;
        }
    }
}
