/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.DiskIoBufferPool;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.RecyclerBytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.escf.EscfBatch;
import org.elasticsearch.escf.EscfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.IndexOperationBatch;
import org.elasticsearch.index.engine.TranslogOperationAsserter;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.transport.BytesRefRecycler;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongConsumer;

import static org.elasticsearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;
import static org.elasticsearch.index.engine.IndexOperationBatch.TranslogRecord.ROW_INDEXED;
import static org.elasticsearch.index.engine.IndexOperationBatch.TranslogRecord.ROW_NO_OP;
import static org.elasticsearch.index.engine.IndexOperationBatch.TranslogRecord.ROW_PREFLIGHT_ERROR;

public class TranslogIndexBatchTests extends ESTestCase {

    private final ShardId shardId = new ShardId("index", "_na_", 1);
    private final AtomicLong primaryTerm = new AtomicLong();
    private Path translogDir;
    private Translog translog;

    @Before
    public void createTranslog() throws Exception {
        assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
        primaryTerm.set(randomLongBetween(1, Integer.MAX_VALUE));
        translogDir = createTempDir();
        translog = create(translogDir);
    }

    @After
    public void closeTranslog() throws Exception {
        try {
            if (translog != null) {
                translog.close();
            }
        } finally {
            IOUtils.rm(translogDir);
        }
    }

    private Translog create(Path path) throws IOException {
        return create(path, longsRef -> {}, (d, s, l) -> {});
    }

    private Translog create(Path path, Consumer<LongsRef> persistedSeqNoConsumer) throws IOException {
        return create(path, persistedSeqNoConsumer, (d, s, l) -> {});
    }

    private Translog create(Path path, OperationListener operationListener) throws IOException {
        return create(path, longsRef -> {}, operationListener);
    }

    private Translog create(Path path, Consumer<LongsRef> persistedSeqNoConsumer, OperationListener operationListener) throws IOException {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.index.IndexVersion.current())
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings);
        final TranslogConfig translogConfig = new TranslogConfig(
            shardId,
            path,
            indexSettings,
            NON_RECYCLING_INSTANCE,
            ByteSizeValue.ofBytes(8 * 1024),
            DiskIoBufferPool.INSTANCE,
            operationListener,
            true
        );
        final String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        return new Translog(
            translogConfig,
            translogUUID,
            new TranslogDeletionPolicy(),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTerm::get,
            persistedSeqNoConsumer,
            TranslogOperationAsserter.DEFAULT
        );
    }

    private static Consumer<LongsRef> longsRefConsumer(LongConsumer consumer) {
        return longsRef -> {
            for (int i = longsRef.offset; i < longsRef.offset + longsRef.length; i++) {
                consumer.accept(longsRef.longs[i]);
            }
        };
    }

    /** Encodes {@code sources} as an ESCF batch and returns a standalone copy of the batch bytes. */
    private static BytesReference encodeBatchData(List<BytesReference> sources) throws IOException {
        try (EscfBatch escfBatch = EscfEncoder.encode(sources, XContentType.JSON)) {
            return new BytesArray(escfBatch.data().toBytesRef(), true);
        }
    }

    /**
     * Mutable builder for {@link IndexOperationBatch.TranslogRecord} instances. Rows default to
     * skipped; {@link #indexed} and {@link #noOp} mark rows replayable and fill the canonical
     * metadata for the status (non-indexed rows carry zeroed/null values, as the production factory
     * {@code IndexOperationBatch#toTranslogRecord} does).
     */
    private static final class RecordBuilder {
        private final byte[] statuses;
        private final long[] seqNos;
        private final long[] versions;
        private final long[] timestamps;
        private final XContentType[] types;
        private final BytesRef[] uids;
        private final String[] routings;
        private final String[] reasons;

        RecordBuilder(int docCount) {
            statuses = new byte[docCount];
            seqNos = new long[docCount];
            versions = new long[docCount];
            timestamps = new long[docCount];
            types = new XContentType[docCount];
            uids = new BytesRef[docCount];
            routings = new String[docCount];
            reasons = new String[docCount];
            for (int i = 0; i < docCount; i++) {
                skipped(i);
            }
        }

        RecordBuilder indexed(int i, long seqNo, long version, long timestamp, XContentType type, String id, String routing) {
            statuses[i] = ROW_INDEXED;
            seqNos[i] = seqNo;
            versions[i] = version;
            timestamps[i] = timestamp;
            types[i] = type;
            uids[i] = Uid.encodeId(id);
            routings[i] = routing;
            reasons[i] = null;
            return this;
        }

        RecordBuilder noOp(int i, long seqNo, String reason) {
            statuses[i] = ROW_NO_OP;
            seqNos[i] = seqNo;
            versions[i] = 0;
            timestamps[i] = 0;
            types[i] = null;
            uids[i] = null;
            routings[i] = null;
            reasons[i] = reason;
            return this;
        }

        RecordBuilder skipped(int i) {
            statuses[i] = ROW_PREFLIGHT_ERROR;
            seqNos[i] = SequenceNumbers.UNASSIGNED_SEQ_NO;
            versions[i] = 0;
            timestamps[i] = 0;
            types[i] = null;
            uids[i] = null;
            routings[i] = null;
            reasons[i] = null;
            return this;
        }

        IndexOperationBatch.TranslogRecord build(long term, BytesReference batchData) {
            return new IndexOperationBatch.TranslogRecord(
                term,
                statuses,
                seqNos,
                versions,
                timestamps,
                types,
                uids,
                anyNonNull(routings) ? routings : null,
                anyNonNull(reasons) ? reasons : null,
                batchData
            );
        }

        /** The record stores routings/noOpReasons as null when no row has a value, as the production factories do. */
        private static boolean anyNonNull(String[] values) {
            for (String value : values) {
                if (value != null) {
                    return true;
                }
            }
            return false;
        }
    }

    /**
     * Build an all-indexed batch record with the given documents (one map per doc), assigning
     * sequence numbers starting at {@code firstSeqNo}, version 1, autoGeneratedIdTimestamps
     * {@code 100 + i}, uids {@code doc-i}, and routing on odd rows.
     */
    private static IndexOperationBatch.TranslogRecord buildBatch(
        List<Map<String, Object>> docs,
        XContentType xContentType,
        long firstSeqNo,
        long term
    ) throws IOException {
        final List<BytesReference> sources = new ArrayList<>(docs.size());
        for (Map<String, Object> doc : docs) {
            try (XContentBuilder b = XContentBuilder.builder(xContentType.xContent())) {
                b.map(doc);
                sources.add(BytesReference.bytes(b));
            }
        }
        final RecordBuilder builder = new RecordBuilder(docs.size());
        for (int i = 0; i < docs.size(); i++) {
            builder.indexed(i, firstSeqNo + i, 1L, 100L + i, xContentType, "doc-" + i, i % 2 == 0 ? null : "route-" + i);
        }
        return builder.build(term, encodeBatchData(sources));
    }

    public void testWireFormatRoundTrip() throws IOException {
        // ESCF only round-trips JSON sources (its parser flips allowDuplicateKeys, which SMILE/CBOR
        // reject). The xContentType byte on the wire is independent of how the ESCF bytes were
        // produced, so we verify the envelope round-trips for several types while encoding via JSON.
        final XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE, XContentType.CBOR, XContentType.YAML);
        final BytesReference batchData = encodeBatchData(
            List.of(
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("a", 1, "b", "hello"))),
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("a", 2, "b", "world")))
            )
        );
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(2).indexed(0, 5L, 1L, 100L, xContentType, "doc-0", null)
            .indexed(1, 6L, 1L, 101L, xContentType, "doc-1", "route")
            .build(primaryTerm.get(), batchData);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            batch.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final byte typeByte = in.readByte();
                assertEquals(Translog.Record.Type.BATCH.id(), typeByte);
                final IndexOperationBatch.TranslogRecord read = IndexOperationBatch.TranslogRecord.readFrom(in);
                assertEquals(batch, read);
                assertEquals(xContentType, read.contentTypes()[0]);
                assertEquals(xContentType, read.contentTypes()[1]);
            }
        }
    }

    public void testWireFormatRoundTripWithNoOpAndSkippedRows() throws IOException {
        // Every row of the source batch keeps its slot in the metadata arrays regardless of
        // outcome, so a record mixing all three row states must round-trip losslessly.
        final BytesReference batchData = encodeBatchData(
            List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}"), new BytesArray("{\"k\":\"row-2\"}"))
        );
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(0, 0L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .noOp(1, 1L, "post-lucene failure")
            .skipped(2)
            .build(primaryTerm.get(), batchData);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            batch.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(Translog.Record.Type.BATCH.id(), in.readByte());
                final IndexOperationBatch.TranslogRecord read = IndexOperationBatch.TranslogRecord.readFrom(in);
                assertEquals(batch, read);
                assertEquals(3, read.docCount());
                assertEquals(2, read.operationCount());
            }
        }
    }

    public void testSnapshotExplodesBatchIntoIndexOps() throws IOException {
        // ESCF's parseToScratch flips allowDuplicateKeys on the source parser, which only JSON
        // supports today, so the ESCF-encoded sources here are JSON.
        final XContentType xContentType = XContentType.JSON;
        final List<Map<String, Object>> docs = List.of(
            Map.of("field", "alpha", "n", 1),
            Map.of("field", "beta", "n", 2),
            Map.of("field", "gamma", "n", 3)
        );
        final IndexOperationBatch.TranslogRecord batch = buildBatch(docs, xContentType, 0L, primaryTerm.get());

        translog.add(batch);

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertEquals(docs.size(), snapshot.totalOperations());
            for (int i = 0; i < docs.size(); i++) {
                final Translog.Operation op = snapshot.next();
                assertNotNull("expected op at index " + i, op);
                assertTrue("expected Index op, got " + op.getClass(), op instanceof Translog.Index);
                final Translog.Index idx = (Translog.Index) op;
                assertEquals(i, idx.seqNo());
                assertEquals(primaryTerm.get(), idx.primaryTerm());
                assertEquals(1L, idx.version());
                assertEquals(100L + i, idx.getAutoGeneratedIdTimestamp());
                assertEquals(Uid.encodeId("doc-" + i), idx.uid());
                if (i % 2 == 0) {
                    assertNull(idx.routing());
                } else {
                    assertEquals("route-" + i, idx.routing());
                }
                // Source round-trips to the same map content, in the original xContentType.
                final Map<String, Object> roundTripped = XContentHelper.convertToMap(idx.source(), false, xContentType).v2();
                final Map<String, Object> expected = docs.get(i);
                assertEquals(expected.keySet(), roundTripped.keySet());
                for (Map.Entry<String, Object> e : expected.entrySet()) {
                    // numeric types may widen (int -> long) through batch encoding; compare via Number.longValue or string equality
                    final Object actual = roundTripped.get(e.getKey());
                    if (e.getValue() instanceof Number expectedN && actual instanceof Number actualN) {
                        assertEquals(expectedN.longValue(), actualN.longValue());
                    } else {
                        assertEquals(e.getValue(), actual);
                    }
                }
                assertEquals(xContentType, XContentHelper.xContentType(idx.source()));
            }
            assertNull(snapshot.next());
        }
    }

    public void testAddBatchNotifiesOperationListener() throws IOException {
        final List<long[]> recordSeqNos = new ArrayList<>();
        final List<Translog.Location> recordLocations = new ArrayList<>();
        final List<BytesReference> records = new ArrayList<>();
        final OperationListener listener = (operation, seqNos, location) -> {
            recordSeqNos.add(seqNos);
            recordLocations.add(location);
            try (RecyclerBytesStreamOutput output = new RecyclerBytesStreamOutput(BytesRefRecycler.NON_RECYCLING_INSTANCE)) {
                operation.writeToTranslogBuffer(output);
                records.add(output.bytes());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };

        final Path dir = createTempDir();
        final long term = primaryTerm.get();
        try (Translog listeningTranslog = create(dir, listener)) {
            final Translog.Index solo = new Translog.Index(Uid.encodeId("solo"), 0, term, 1L, new BytesArray("{\"k\":\"v\"}"), null, -1L);
            listeningTranslog.add(solo);

            final List<BytesReference> sources = List.of(
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("k", "v1"))),
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("k", "v2"))),
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("k", "v3"))),
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("k", "v4")))
            );
            final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(4).indexed(0, 1L, 100L, 0L, XContentType.JSON, "doc-0", null)
                .indexed(1, 2L, 101L, 1L, XContentType.JSON, "doc-1", null)
                .noOp(2, 3L, "test failure")
                .skipped(3)
                .build(term, encodeBatchData(sources));
            final Translog.Location location = listeningTranslog.add(batch);

            // Two records: the solo op (one seqNo) and the batch (one seqNo per replayable row;
            // the preflight-error row never consumed a seqNo and is not reported).
            assertEquals(2, recordSeqNos.size());
            assertArrayEquals(new long[] { 0L }, recordSeqNos.get(0));
            assertArrayEquals(new long[] { 1L, 2L, 3L }, recordSeqNos.get(1));
            assertEquals(location, recordLocations.get(1));

            // The listener received the full framed records: they must round-trip through readRecord to equal records.
            try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(records.get(0).streamInput(), "test")) {
                final Translog.Record record = Translog.readRecord(in);
                assertEquals(solo, record);
            }
            try (BufferedChecksumStreamInput in = new BufferedChecksumStreamInput(records.get(1).streamInput(), "test")) {
                final Translog.Record record = Translog.readRecord(in);
                assertEquals(batch, record);
            }
        }
    }

    public void testInterleavedBatchesAndRegularOps() throws IOException {
        final long term = primaryTerm.get();
        final Translog.Index op0 = new Translog.Index(Uid.encodeId("solo-0"), 0, term, 1L, new BytesArray("{\"k\":\"v0\"}"), null, -1L);
        translog.add(op0);

        final IndexOperationBatch.TranslogRecord batchA = buildBatch(
            List.of(Map.of("k", "v1"), Map.of("k", "v2")),
            XContentType.JSON,
            1L,
            term
        );
        translog.add(batchA);

        translog.add(new Translog.Delete("solo-3", 3, term));

        final IndexOperationBatch.TranslogRecord batchB = buildBatch(
            List.of(Map.of("k", "v4"), Map.of("k", "v5")),
            XContentType.JSON,
            4L,
            term
        );
        translog.add(batchB);

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertEquals(1 + 2 + 1 + 2, snapshot.totalOperations());

            final Translog.Operation r0 = snapshot.next();
            assertTrue(r0 instanceof Translog.Index);
            assertEquals(0L, r0.seqNo());

            final Translog.Operation r1 = snapshot.next();
            assertTrue(r1 instanceof Translog.Index);
            assertEquals(1L, r1.seqNo());

            final Translog.Operation r2 = snapshot.next();
            assertTrue(r2 instanceof Translog.Index);
            assertEquals(2L, r2.seqNo());

            final Translog.Operation r3 = snapshot.next();
            assertTrue(r3 instanceof Translog.Delete);
            assertEquals(3L, r3.seqNo());

            final Translog.Operation r4 = snapshot.next();
            assertTrue(r4 instanceof Translog.Index);
            assertEquals(4L, r4.seqNo());

            final Translog.Operation r5 = snapshot.next();
            assertTrue(r5 instanceof Translog.Index);
            assertEquals(5L, r5.seqNo());

            assertNull(snapshot.next());
        }
    }

    public void testCheckpointAccounting() throws IOException {
        final long term = primaryTerm.get();
        final int docCount = 5;
        final List<Map<String, Object>> docs = new ArrayList<>(docCount);
        for (int i = 0; i < docCount; i++) {
            docs.add(Map.of("idx", i));
        }
        final IndexOperationBatch.TranslogRecord batch = buildBatch(docs, XContentType.JSON, 10L, term);
        translog.add(batch);

        assertEquals(docCount, translog.totalOperations());

        translog.sync();
        final Checkpoint cp = translog.getLastSyncedCheckpoint();
        assertEquals(10L, cp.minSeqNo);
        assertEquals(10L + docCount - 1, cp.maxSeqNo);
        assertEquals(docCount, cp.numOps);
    }

    public void testCheckpointAccountingIgnoresSkippedRows() throws IOException {
        // Skipped rows never consumed a seqNo, so they must not contribute to numOps or to the
        // min/max seqNo bounds of the generation.
        final long term = primaryTerm.get();
        final BytesReference batchData = encodeBatchData(
            List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}"), new BytesArray("{\"k\":\"row-2\"}"))
        );
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(0, 10L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .skipped(1)
            .indexed(2, 11L, 1L, 102L, XContentType.JSON, "doc-2", null)
            .build(term, batchData);
        translog.add(batch);

        assertEquals(2, translog.totalOperations());

        translog.sync();
        final Checkpoint cp = translog.getLastSyncedCheckpoint();
        assertEquals(10L, cp.minSeqNo);
        assertEquals(11L, cp.maxSeqNo);
        assertEquals(2, cp.numOps);
    }

    public void testPersistedSeqNoConsumerExcludesPreflightErrorRows() throws IOException {
        // The persisted-seqNo callback drives markSeqNoAsPersisted; a preflight-error row never
        // consumed a seqNo, so it must not be reported as persisted (not even as UNASSIGNED_SEQ_NO).
        final Set<Long> persistedSeqNos = new HashSet<>();
        final Path dir = createTempDir();
        try (Translog collectingTranslog = create(dir, longsRefConsumer(persistedSeqNos::add))) {
            final BytesReference batchData = encodeBatchData(
                List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}"), new BytesArray("{\"k\":\"row-2\"}"))
            );
            final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(
                0,
                10L,
                1L,
                100L,
                XContentType.JSON,
                "doc-0",
                null
            ).skipped(1).noOp(2, 11L, "post-lucene failure").build(primaryTerm.get(), batchData);
            collectingTranslog.add(batch);

            assertTrue("no seqNo may be reported persisted before a sync", persistedSeqNos.isEmpty());
            collectingTranslog.sync();
            assertEquals(Set.of(10L, 11L), persistedSeqNos);
        }
    }

    public void testSnapshotExplodesMixedIndexAndNoOpEntries() throws IOException {
        // Simulates a primary sub-batch where the middle op succeeded preflight but failed
        // post-Lucene with an assigned seqNo, so the engine marked its row ROW_NO_OP while the
        // surrounding rows stayed ROW_INDEXED. The failed row keeps its slot in the source batch;
        // replay must emit Index, NoOp, Index in that order with correctly aligned sources.
        final XContentType xContentType = XContentType.JSON;
        final BytesReference batchData = encodeBatchData(
            List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}"), new BytesArray("{\"k\":\"row-2\"}"))
        );

        final long term = primaryTerm.get();
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(0, 0L, 1L, 100L, xContentType, "doc-0", null)
            .noOp(1, 1L, "post-lucene failure")
            .indexed(2, 2L, 1L, 102L, xContentType, "doc-2", null)
            .build(term, batchData);
        translog.add(batch);

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertEquals(3, snapshot.totalOperations());

            final Translog.Operation op0 = snapshot.next();
            assertTrue("expected Index op, got " + op0, op0 instanceof Translog.Index);
            final Translog.Index idx0 = (Translog.Index) op0;
            assertEquals(0L, idx0.seqNo());
            assertEquals(Uid.encodeId("doc-0"), idx0.uid());
            assertEquals("row-0", XContentHelper.convertToMap(idx0.source(), false, xContentType).v2().get("k"));

            final Translog.Operation op1 = snapshot.next();
            assertTrue("expected NoOp, got " + op1, op1 instanceof Translog.NoOp);
            final Translog.NoOp noOp = (Translog.NoOp) op1;
            assertEquals(1L, noOp.seqNo());
            assertEquals(term, noOp.primaryTerm());
            assertEquals("post-lucene failure", noOp.reason());

            final Translog.Operation op2 = snapshot.next();
            assertTrue("expected Index op, got " + op2, op2 instanceof Translog.Index);
            final Translog.Index idx2 = (Translog.Index) op2;
            assertEquals(2L, idx2.seqNo());
            assertEquals(Uid.encodeId("doc-2"), idx2.uid());
            // Crucially: metadata row 2 maps to source row 2 by position — the NoOp row in between
            // kept its slot in the source batch, so alignment cannot drift.
            assertEquals("row-2", XContentHelper.convertToMap(idx2.source(), false, xContentType).v2().get("k"));

            assertNull(snapshot.next());
        }
    }

    public void testExplodeSkipsSkippedRows() throws IOException {
        // Simulates the primary path where the middle op of a 3-row sub-batch hit a preflight
        // failure (UNASSIGNED_SEQ_NO). Its row is marked ROW_PREFLIGHT_ERROR but still occupies its slot
        // in both the metadata arrays and the source batch, so the surviving rows replay their
        // original sources without any explicit row-index bookkeeping.
        final XContentType xContentType = XContentType.JSON;
        final BytesReference batchData = encodeBatchData(
            List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}"), new BytesArray("{\"k\":\"row-2\"}"))
        );

        final long term = primaryTerm.get();
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(0, 0L, 1L, 100L, xContentType, "doc-0", null)
            .skipped(1)
            .indexed(2, 2L, 1L, 102L, xContentType, "doc-2", null)
            .build(term, batchData);
        translog.add(batch);

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            assertEquals(2, snapshot.totalOperations());

            final Translog.Index op0 = (Translog.Index) snapshot.next();
            assertNotNull(op0);
            assertEquals(0L, op0.seqNo());
            assertEquals(Uid.encodeId("doc-0"), op0.uid());
            assertEquals("row-0", XContentHelper.convertToMap(op0.source(), false, xContentType).v2().get("k"));

            final Translog.Index op2 = (Translog.Index) snapshot.next();
            assertNotNull(op2);
            assertEquals(2L, op2.seqNo());
            assertEquals(Uid.encodeId("doc-2"), op2.uid());
            // Crucially: row-2, not row-1 — the skipped row still occupies source row 1.
            assertEquals("row-2", XContentHelper.convertToMap(op2.source(), false, xContentType).v2().get("k"));

            assertNull(snapshot.next());
        }
    }

    public void testExplodeRowCountMismatchThrows() throws IOException {
        // The record claims 3 rows but the batch data only carries 2. Replay must fail loudly
        // instead of mis-aligning metadata with sources.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}")));
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(0, 0L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .indexed(1, 1L, 1L, 101L, XContentType.JSON, "doc-1", null)
            .indexed(2, 2L, 1L, 102L, XContentType.JSON, "doc-2", null)
            .build(primaryTerm.get(), batchData);

        final IOException ex = expectThrows(IOException.class, batch::explode);
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("rows"));
    }

    public void testGetIndexOp() throws IOException {
        final XContentType xContentType = XContentType.JSON;
        final BytesReference batchData = encodeBatchData(
            List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}"), new BytesArray("{\"k\":\"row-2\"}"))
        );
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(0, 0L, 1L, 100L, xContentType, "doc-0", null)
            .noOp(1, 1L, "post-lucene failure")
            .indexed(2, 2L, 1L, 102L, xContentType, "doc-2", "route-2")
            .build(primaryTerm.get(), batchData);

        final Translog.Index idx = batch.getIndexOp(2);
        assertEquals(2L, idx.seqNo());
        assertEquals(Uid.encodeId("doc-2"), idx.uid());
        assertEquals("route-2", idx.routing());
        assertEquals("row-2", XContentHelper.convertToMap(idx.source(), false, xContentType).v2().get("k"));

        final IOException outOfRange = expectThrows(IOException.class, () -> batch.getIndexOp(5));
        assertTrue("unexpected exception message: " + outOfRange.getMessage(), outOfRange.getMessage().contains("out of range"));

        final IOException notIndexed = expectThrows(IOException.class, () -> batch.getIndexOp(1));
        assertTrue("unexpected exception message: " + notIndexed.getMessage(), notIndexed.getMessage().contains("not an indexed row"));
    }

    public void testReadOperationByLocationAndRowIndex() throws IOException {
        final long term = primaryTerm.get();
        final IndexOperationBatch.TranslogRecord batch = buildBatch(
            List.of(Map.of("k", "v0"), Map.of("k", "v1")),
            XContentType.JSON,
            0L,
            term
        );
        final Translog.Location location = translog.add(batch);

        final Translog.Operation op = translog.readOperation(location, 1);
        assertTrue("expected Index op, got " + op, op instanceof Translog.Index);
        final Translog.Index idx = (Translog.Index) op;
        assertEquals(1L, idx.seqNo());
        assertEquals(Uid.encodeId("doc-1"), idx.uid());
        assertEquals("v1", XContentHelper.convertToMap(idx.source(), false, XContentType.JSON).v2().get("k"));
    }

    public void testReadByLocationThrowsForBatch() throws IOException {
        final long term = primaryTerm.get();
        final IndexOperationBatch.TranslogRecord batch = buildBatch(
            List.of(Map.of("k", "v0"), Map.of("k", "v1")),
            XContentType.JSON,
            0L,
            term
        );
        final Translog.Location location = translog.add(batch);

        final IOException ex = expectThrows(IOException.class, () -> translog.readOperation(location));
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage() != null && ex.getMessage().contains("batch"));
    }

    public void testConstructorRejectsUnknownStatus() throws IOException {
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v\"}")));
        final RecordBuilder builder = new RecordBuilder(1).indexed(0, 0L, 1L, 100L, XContentType.JSON, "doc-0", null);
        builder.statuses[0] = 7;
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> builder.build(primaryTerm.get(), batchData));
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("unknown row status"));
    }

    public void testConstructorRejectsAllSkippedRows() throws IOException {
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v0\"}"), new BytesArray("{\"k\":\"v1\"}")));
        final RecordBuilder builder = new RecordBuilder(2);
        final IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> builder.build(primaryTerm.get(), batchData));
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("at least one replayable row"));
    }

    public void testWireFormatRoundTripWithNullRoutingsAndReasons() throws IOException {
        // A batch with no routing values and no no-op rows stores null routings/noOpReasons
        // arrays; the wire format must reconstruct that canonical form so round-trips stay equal
        // and replay produces index operations without routing.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}")));
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(2).indexed(0, 0L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .indexed(1, 1L, 1L, 101L, XContentType.JSON, "doc-1", null)
            .build(primaryTerm.get(), batchData);
        assertNull(batch.routings());
        assertNull(batch.noOpReasons());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            batch.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(Translog.Record.Type.BATCH.id(), in.readByte());
                final IndexOperationBatch.TranslogRecord read = IndexOperationBatch.TranslogRecord.readFrom(in);
                assertEquals(batch, read);
                assertNull(read.routings());
                assertNull(read.noOpReasons());

                final List<Translog.Operation> replayed = read.explode();
                assertEquals(2, replayed.size());
                for (Translog.Operation op : replayed) {
                    assertNull(((Translog.Index) op).routing());
                }
            }
        }
    }

    public void testConstructorRejectsAllNullArrays() throws IOException {
        // A value-free routings/noOpReasons array carries no information and must be passed as
        // null; an all-null array trips the constructor assertion.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v\"}")));
        final long term = primaryTerm.get();
        expectThrows(AssertionError.class, () -> allIndexedRecord(term, new String[1], null, batchData));
        expectThrows(AssertionError.class, () -> allIndexedRecord(term, null, new String[1], batchData));
    }

    private static IndexOperationBatch.TranslogRecord allIndexedRecord(
        long term,
        String[] routings,
        String[] noOpReasons,
        BytesReference batchData
    ) {
        return new IndexOperationBatch.TranslogRecord(
            term,
            new byte[] { ROW_INDEXED },
            new long[] { 0L },
            new long[] { 1L },
            new long[] { 100L },
            new XContentType[] { XContentType.JSON },
            new BytesRef[] { Uid.encodeId("doc-0") },
            routings,
            noOpReasons,
            batchData
        );
    }

    public void testConstructorAssertsNoOpReasonPresent() throws IOException {
        // Every ROW_NO_OP row must carry a reason; a record with a no-op row but no reasons array
        // (or a null reason in its slot) trips the constructor assertion.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v\"}")));
        final long term = primaryTerm.get();
        expectThrows(
            AssertionError.class,
            () -> new IndexOperationBatch.TranslogRecord(
                term,
                new byte[] { ROW_NO_OP },
                new long[] { 0L },
                new long[] { 0L },
                new long[] { 0L },
                new XContentType[1],
                new BytesRef[1],
                null,
                randomBoolean() ? null : new String[1],
                batchData
            )
        );
    }

    public void testSeqNumberConflictAssertsDifferentOps() throws IOException {
        final long term = primaryTerm.get();
        final Translog.Index op0 = new Translog.Index(Uid.encodeId("solo-0"), 0, term, 1L, new BytesArray("{\"k\":\"v0\"}"), null, -1L);
        translog.add(op0);

        translog.add(new Translog.Delete("solo-3", 2, term));

        final IndexOperationBatch.TranslogRecord batchA = buildBatch(
            List.of(Map.of("k", "v1"), Map.of("k", "v2")),
            XContentType.JSON,
            1L,
            term
        );

        // Assertion should fail since batchA will contain seqNo 2 which was added as a Delete Op.
        expectThrows(AssertionError.class, () -> translog.add(batchA));
    }

    public void testSeqNumberConflictAssertsSemanticEquality() throws IOException {
        final long term = primaryTerm.get();
        final Translog.Index op0 = new Translog.Index(Uid.encodeId("doc-0"), 0, term, 1L, new BytesArray("{\"k\":\"v1\"}"), null, -1L);
        translog.add(op0);

        final IndexOperationBatch.TranslogRecord batchA = buildBatch(
            List.of(Map.of("k", "v1"), Map.of("k", "v2")),
            XContentType.JSON,
            0L,
            term
        );

        // Assertion should succeed since batchA will contain seqNo 0 which was same as the individual op that was added.
        translog.add(batchA);
    }

    public void testSeqNumberConflictAssertsSemanticInequality() throws IOException {
        final long term = primaryTerm.get();
        final Translog.Index op0 = new Translog.Index(Uid.encodeId("solo-0"), 0, term, 1L, new BytesArray("{\"k\":\"v1\"}"), null, -1L);
        translog.add(op0);

        final IndexOperationBatch.TranslogRecord batchA = buildBatch(
            List.of(Map.of("k", "v1"), Map.of("k", "v2")),
            XContentType.JSON,
            0L,
            term
        );

        // Assertion should fail since batchA will contain seqNo 0 with a different uid.
        expectThrows(AssertionError.class, () -> translog.add(batchA));
    }

}
