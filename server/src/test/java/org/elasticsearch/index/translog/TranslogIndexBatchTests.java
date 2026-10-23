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
import org.elasticsearch.index.engine.IndexOperationBatch.TranslogRecord.NoOpEntries;
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
import java.util.HashMap;
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
        final String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        return new Translog(
            translogConfig(path, operationListener),
            translogUUID,
            new TranslogDeletionPolicy(),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTerm::get,
            persistedSeqNoConsumer,
            TranslogOperationAsserter.DEFAULT
        );
    }

    private TranslogConfig translogConfig(Path path, OperationListener operationListener) {
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, org.elasticsearch.index.IndexVersion.current())
            .build();
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings(shardId.getIndex(), settings);
        return new TranslogConfig(
            shardId,
            path,
            indexSettings,
            NON_RECYCLING_INSTANCE,
            ByteSizeValue.ofBytes(8 * 1024),
            DiskIoBufferPool.INSTANCE,
            operationListener,
            true
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
            // every row carries its own seqNo; skipped rows hold UNASSIGNED_SEQ_NO, as the production factory guarantees
            int noOpCount = 0;
            for (int i = 0; i < statuses.length; i++) {
                switch (statuses[i]) {
                    case ROW_INDEXED, ROW_PREFLIGHT_ERROR -> {
                    }
                    case ROW_NO_OP -> noOpCount++;
                    default -> throw new IllegalArgumentException("unknown row status [" + statuses[i] + "] for row [" + i + "]");
                }
            }
            final int[] noOpRows = noOpCount == 0 ? null : new int[noOpCount];
            final String[] noOpReasons = noOpCount == 0 ? null : new String[noOpCount];
            int n = 0;
            for (int i = 0; i < statuses.length; i++) {
                if (statuses[i] == ROW_NO_OP) {
                    noOpReasons[n] = reasons[i];
                    noOpRows[n++] = i;
                }
            }
            return new IndexOperationBatch.TranslogRecord(
                term,
                statuses.length,
                seqNos,
                versions,
                timestamps,
                types,
                uids,
                anyNonNull(routings) ? routings : null,
                noOpCount == 0 ? null : new NoOpEntries(noOpRows, noOpReasons),
                batchData
            );
        }

        /** The record stores routings/no-op entries as null when no row has a value, as the production factories do. */
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

            // Two records: the solo op and the batch (one seqNo per replayable row; the preflight-error
            // row never consumed a seqNo and is not listed).
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

    public void testReopenedTranslogReplaysBatches() throws IOException {
        // Batch records must survive a close/reopen cycle: on reopen the previous generations are
        // recovered from disk and read through TranslogReader (the recovery path), not the writer
        // that produced them. The generation roll additionally puts the mixed-status batch behind
        // a closed reader before the reopen, so recovery must replay indexed rows as Index ops and
        // no-op rows as NoOps, and must skip preflight-error rows entirely.
        final long term = primaryTerm.get();

        translog.add(new Translog.Index(Uid.encodeId("solo-0"), 0, term, 1L, new BytesArray("{\"k\":\"v0\"}"), null, -1L));
        // batch A carries every row status: two indexed rows, a post-Lucene failure that replays
        // as a NoOp, and a preflight failure whose source stays in the batch data but never
        // consumed a seqNo and must not replay at all
        final List<BytesReference> batchASources = List.of(
            new BytesArray("{\"k\":\"v1\"}"),
            new BytesArray("{\"k\":\"v2\"}"),
            new BytesArray("{\"k\":\"v3\"}"),
            new BytesArray("{\"k\":\"v4\"}")
        );
        final IndexOperationBatch.TranslogRecord batchA = new RecordBuilder(4).indexed(0, 1L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .indexed(1, 2L, 1L, 101L, XContentType.JSON, "doc-1", "route-1")
            .noOp(2, 3L, "post-lucene failure")
            .skipped(3)
            .build(term, encodeBatchData(batchASources));
        translog.add(batchA);
        translog.rollGeneration();
        translog.add(buildBatch(List.of(Map.of("k", "v5"), Map.of("k", "v6"), Map.of("k", "v7")), XContentType.JSON, 4L, term));
        translog.add(new Translog.Delete("solo-7", 7, term));
        translog.sync();

        final String translogUUID = translog.getTranslogUUID();
        translog.close();
        translog = new Translog(
            translogConfig(translogDir, (d, s, l) -> {}),
            translogUUID,
            new TranslogDeletionPolicy(),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTerm::get,
            longsRef -> {},
            TranslogOperationAsserter.DEFAULT
        );

        try (Translog.Snapshot snapshot = translog.newSnapshot()) {
            // the preflight-error row is not an operation: batch A contributes 3, not 4
            assertEquals(1 + 3 + 3 + 1, snapshot.totalOperations());
            // snapshots iterate generations newest-first, so collect and assert by seqNo
            final Map<Long, Translog.Operation> bySeqNo = new HashMap<>();
            Translog.Operation op;
            while ((op = snapshot.next()) != null) {
                assertNull("duplicate seqNo " + op.seqNo(), bySeqNo.put(op.seqNo(), op));
            }
            // 8 operations at seqNos 0..7; the skipped row surfaced nowhere
            assertEquals(8, bySeqNo.size());
            for (long seqNo = 0; seqNo <= 7; seqNo++) {
                final Translog.Operation replayed = bySeqNo.get(seqNo);
                assertNotNull("expected op at seqNo " + seqNo, replayed);
                assertEquals(term, replayed.primaryTerm());
                if (seqNo == 3) {
                    assertTrue(replayed instanceof Translog.NoOp);
                } else if (seqNo == 7) {
                    assertTrue(replayed instanceof Translog.Delete);
                } else {
                    assertTrue(replayed instanceof Translog.Index);
                }
            }
            // second row of the first (rolled) batch: full metadata and source survive the reopen
            final Translog.Index idx = (Translog.Index) bySeqNo.get(2L);
            assertEquals(Uid.encodeId("doc-1"), idx.uid());
            assertEquals("route-1", idx.routing());
            assertEquals(Map.of("k", "v2"), XContentHelper.convertToMap(idx.source(), false, XContentType.JSON).v2());
            // the no-op row keeps its reason across the reopen
            assertEquals("post-lucene failure", ((Translog.NoOp) bySeqNo.get(3L)).reason());
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

    public void testBatchWithSeqNoGapIsRecordedRowByRow() throws IOException {
        // A replica batch carries the seqNos the primary assigned, which may have a gap when the primary
        // split the rows into several sub-batches with other operations reserving seqNos in between.
        // Every row must be persisted, counted and replayed under its own seqNo; nothing in the gap may
        // be claimed.
        final Set<Long> persistedSeqNos = new HashSet<>();
        final List<long[]> recordSeqNos = new ArrayList<>();
        final Path dir = createTempDir();
        try (
            Translog gapTranslog = create(
                dir,
                longsRefConsumer(persistedSeqNos::add),
                (operation, seqNos, location) -> recordSeqNos.add(seqNos)
            )
        ) {
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
            )
                .indexed(1, 11L, 1L, 101L, XContentType.JSON, "doc-1", null)
                .indexed(2, 15L, 1L, 102L, XContentType.JSON, "doc-2", null)
                .build(primaryTerm.get(), batchData);
            gapTranslog.add(batch);
            assertEquals(3, gapTranslog.totalOperations());
            // the listener (stateless replication) is told exactly which seqNos the record carries
            assertEquals(1, recordSeqNos.size());
            assertArrayEquals(new long[] { 10L, 11L, 15L }, recordSeqNos.get(0));

            gapTranslog.sync();
            assertEquals(Set.of(10L, 11L, 15L), persistedSeqNos);
            final Checkpoint cp = gapTranslog.getLastSyncedCheckpoint();
            assertEquals(10L, cp.minSeqNo);
            assertEquals(15L, cp.maxSeqNo);
            assertEquals(3, cp.numOps);

            final List<Long> replayedSeqNos = new ArrayList<>();
            try (Translog.Snapshot snapshot = gapTranslog.newSnapshot()) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    replayedSeqNos.add(op.seqNo());
                }
            }
            assertEquals(List.of(10L, 11L, 15L), replayedSeqNos);
        }
    }

    public void testWireFormatRoundTripWithSeqNoGap() throws IOException {
        // seqNos are written as deltas from the previous row that has one, so a gap and a preflight row
        // in the middle must both survive the round trip unchanged.
        final BytesReference batchData = encodeBatchData(
            List.of(
                new BytesArray("{\"k\":\"row-0\"}"),
                new BytesArray("{\"k\":\"row-1\"}"),
                new BytesArray("{\"k\":\"row-2\"}"),
                new BytesArray("{\"k\":\"row-3\"}")
            )
        );
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(4).indexed(0, 100L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .indexed(1, 101L, 1L, 101L, XContentType.JSON, "doc-1", null)
            .skipped(2)
            .noOp(3, 205L, "post-lucene failure")
            .build(primaryTerm.get(), batchData);
        assertEquals(100L, batch.seqNo(0));
        assertEquals(101L, batch.seqNo(1));
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, batch.seqNo(2));
        assertEquals(205L, batch.seqNo(3));
        assertEquals(3, batch.operationCount());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            batch.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(Translog.Record.Type.BATCH.id(), in.readByte());
                final IndexOperationBatch.TranslogRecord read = IndexOperationBatch.TranslogRecord.readFrom(in);
                assertEquals(batch, read);
                assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, read.seqNo(2));
                assertEquals(205L, read.seqNo(3));
            }
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
        // failure. Its row is marked ROW_PREFLIGHT_ERROR but still occupies its slot in both the
        // metadata arrays and the source batch; it never consumed a seqNo, so the surviving rows
        // carry the contiguous seqNos 0 and 1 while replaying their original sources.
        final XContentType xContentType = XContentType.JSON;
        final BytesReference batchData = encodeBatchData(
            List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}"), new BytesArray("{\"k\":\"row-2\"}"))
        );

        final long term = primaryTerm.get();
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(3).indexed(0, 0L, 1L, 100L, xContentType, "doc-0", null)
            .skipped(1)
            .indexed(2, 1L, 1L, 102L, xContentType, "doc-2", null)
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
            assertEquals(1L, op2.seqNo());
            assertEquals(Uid.encodeId("doc-2"), op2.uid());
            // Crucially: row-2, not row-1 — the skipped row still occupies source row 1.
            assertEquals("row-2", XContentHelper.convertToMap(op2.source(), false, xContentType).v2().get("k"));

            assertNull(snapshot.next());
        }
    }

    public void testExplodeDerivesRowStateForRandomStatuses() throws IOException {
        // explode() walks the sparse no-op rows in step with the rows instead of looking each row up, so
        // for any mix of row statuses it must emit one op per replayable row in row order, keep indexed
        // rows on their source slot and no-op rows on their reason, and give every op the row's own seqNo,
        // gaps included, as the per-row accessors report.
        final int docCount = randomIntBetween(1, 64);
        final List<BytesReference> sources = new ArrayList<>(docCount);
        final RecordBuilder builder = new RecordBuilder(docCount);
        final List<Long> expectedSeqNos = new ArrayList<>(docCount);
        long seqNo = randomLongBetween(0, 1_000_000L);
        boolean replayable = false;
        for (int i = 0; i < docCount; i++) {
            sources.add(new BytesArray("{\"k\":\"row-" + i + "\"}"));
            final byte status;
            if (i == docCount - 1 && replayable == false) {
                status = ROW_INDEXED; // a record must hold at least one replayable row
            } else {
                status = randomFrom(ROW_INDEXED, ROW_NO_OP, ROW_PREFLIGHT_ERROR);
            }
            switch (status) {
                case ROW_INDEXED -> builder.indexed(
                    i,
                    seqNo,
                    1L,
                    100L + i,
                    XContentType.JSON,
                    "doc-" + i,
                    i % 3 == 0 ? "route-" + i : null
                );
                case ROW_NO_OP -> builder.noOp(i, seqNo, "failure-" + i);
                case ROW_PREFLIGHT_ERROR -> builder.skipped(i);
                default -> throw new AssertionError("unknown row status [" + status + "]");
            }
            if (status != ROW_PREFLIGHT_ERROR) {
                replayable = true;
                expectedSeqNos.add(seqNo);
                // a replica batch may carry a gap where the primary interleaved other operations
                seqNo += rarely() ? randomIntBetween(2, 10) : 1;
            }
        }
        final IndexOperationBatch.TranslogRecord record = builder.build(primaryTerm.get(), encodeBatchData(sources));

        final List<Translog.Operation> ops = record.explode();
        assertEquals(record.operationCount(), ops.size());
        int next = 0;
        for (int i = 0; i < docCount; i++) {
            switch (record.rowStatus(i)) {
                case ROW_INDEXED -> {
                    final Translog.Index idx = (Translog.Index) ops.get(next++);
                    assertEquals(record.seqNo(i), idx.seqNo());
                    assertEquals(Uid.encodeId("doc-" + i), idx.uid());
                    assertEquals(i % 3 == 0 ? "route-" + i : null, idx.routing());
                    assertEquals("row-" + i, XContentHelper.convertToMap(idx.source(), false, XContentType.JSON).v2().get("k"));
                }
                case ROW_NO_OP -> {
                    final Translog.NoOp noOp = (Translog.NoOp) ops.get(next++);
                    assertEquals(record.seqNo(i), noOp.seqNo());
                    assertEquals("failure-" + i, noOp.reason());
                }
                case ROW_PREFLIGHT_ERROR -> assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, record.seqNo(i));
                default -> throw new AssertionError("unknown row status [" + record.rowStatus(i) + "]");
            }
        }
        assertEquals(ops.size(), next);
        for (int k = 0; k < ops.size(); k++) {
            assertEquals("seqNo of op " + k, expectedSeqNos.get(k).longValue(), ops.get(k).seqNo());
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

    public void testReadFromRejectsRecordWithoutReplayableRows() throws IOException {
        // The constructor refuses an all-preflight record, so the only way one reaches readFrom is
        // corrupt or crafted bytes. The reader must reject it rather than hand back a record that
        // replays nothing.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v0\"}"), new BytesArray("{\"k\":\"v1\"}")));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(IndexOperationBatch.TranslogRecord.EXPERIMENTAL_PRE_RELEASE);
            out.writeLong(primaryTerm.get());
            out.writeVInt(2); // docCount
            // seqNos are deltas from the previous assigned seqNo; a preflight row writes UNASSIGNED_SEQ_NO as its marker
            out.writeZLong(SequenceNumbers.UNASSIGNED_SEQ_NO); // row 0: preflight error
            out.writeZLong(SequenceNumbers.UNASSIGNED_SEQ_NO); // row 1: preflight error
            out.writeBoolean(false); // no no-op rows
            // no per-row metadata follows: no row is indexed
            out.writeBoolean(false); // no routings
            out.writeVInt(batchData.length());
            batchData.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final IOException ex = expectThrows(IOException.class, () -> IndexOperationBatch.TranslogRecord.readFrom(in));
                assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("at least one replayable row"));
            }
        }
    }

    public void testReadFromRejectsNegativeSeqNo() throws IOException {
        // A delta is either the UNASSIGNED_SEQ_NO marker or positive; any other value would put a seqNo below
        // the base and is corrupt. It must be rejected by the reader before it reaches the writer's bookkeeping.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v0\"}")));
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(IndexOperationBatch.TranslogRecord.EXPERIMENTAL_PRE_RELEASE);
            out.writeLong(primaryTerm.get());
            out.writeVInt(1); // docCount
            out.writeZLong(-3L); // row 0: delta -3 from the base of -1, i.e. seqNo -4
            out.writeBoolean(false); // no no-op rows
            out.writeBoolean(false); // no routings
            out.writeVInt(batchData.length());
            batchData.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final IOException ex = expectThrows(IOException.class, () -> IndexOperationBatch.TranslogRecord.readFrom(in));
                assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("invalid seqNo [-4]"));
            }
        }
    }

    public void testReadFromRejectsNonIncreasingSeqNos() throws IOException {
        // Rows are processed in order, so a row's seqNo is always above the previous row's. A record that
        // goes backwards, or repeats a seqNo, is corrupt and must be rejected before the per-row metadata
        // that follows the seqNos is interpreted.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v0\"}"), new BytesArray("{\"k\":\"v1\"}")));
        final long second = randomFrom(5L, 4L); // equal to or below the first row's seqNo
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeVInt(IndexOperationBatch.TranslogRecord.EXPERIMENTAL_PRE_RELEASE);
            out.writeLong(primaryTerm.get());
            out.writeVInt(2); // docCount
            out.writeZLong(5L - SequenceNumbers.NO_OPS_PERFORMED); // row 0: seqNo 5
            out.writeZLong(second - 5L); // row 1: not above 5
            // the reader stops at the seqNos, so nothing after them needs to be well-formed
            try (StreamInput in = out.bytes().streamInput()) {
                final IOException ex = expectThrows(IOException.class, () -> IndexOperationBatch.TranslogRecord.readFrom(in));
                assertTrue(
                    "unexpected exception message: " + ex.getMessage(),
                    ex.getMessage().contains("invalid seqNo [" + second + "]") && ex.getMessage().contains("previous one is [5]")
                );
            }
        }
    }

    public void testWriteRejectsNonIncreasingSeqNos() throws IOException {
        // The constructor validates the seqNos, but the array is shared with the caller, so the writer
        // re-checks the rule the reader enforces rather than put a record on disk that cannot be read back.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v0\"}"), new BytesArray("{\"k\":\"v1\"}")));
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(2).indexed(0, 5L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .indexed(1, 6L, 1L, 101L, XContentType.JSON, "doc-1", null)
            .build(primaryTerm.get(), batchData);
        batch.seqNos()[1] = randomFrom(5L, 4L); // equal to or below the first row's seqNo
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            final IllegalStateException ex = expectThrows(IllegalStateException.class, () -> batch.writeTo(out));
            assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("previous one is [5]"));
        }
    }

    public void testWireFormatRoundTripWithNullRoutingsAndNoOps() throws IOException {
        // A batch with no routing values and no no-op rows stores null routings and no-op entries;
        // the wire format must reconstruct that canonical form so round-trips stay equal and
        // replay produces index operations without routing.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-1\"}")));
        final IndexOperationBatch.TranslogRecord batch = new RecordBuilder(2).indexed(0, 0L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .indexed(1, 1L, 1L, 101L, XContentType.JSON, "doc-1", null)
            .build(primaryTerm.get(), batchData);
        assertNull(batch.routings());
        assertNull(batch.noOps());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            batch.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                assertEquals(Translog.Record.Type.BATCH.id(), in.readByte());
                final IndexOperationBatch.TranslogRecord read = IndexOperationBatch.TranslogRecord.readFrom(in);
                assertEquals(batch, read);
                assertNull(read.routings());
                assertNull(read.noOps());

                final List<Translog.Operation> replayed = read.explode();
                assertEquals(2, replayed.size());
                for (Translog.Operation op : replayed) {
                    assertNull(((Translog.Index) op).routing());
                }
            }
        }
    }

    public void testConstructorRejectsAllNullArrays() throws IOException {
        // A value-free routings array carries no information and must be passed as null; an
        // all-null array trips the constructor assertion.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v\"}")));
        final long term = primaryTerm.get();
        final AssertionError ex = expectThrows(AssertionError.class, () -> allIndexedRecord(term, 0L, new String[1], batchData));
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("at least one non-null value"));
    }

    public void testConstructorAssertsNoOpRowsHaveSeqNos() throws IOException {
        // A no-op row failed after consuming a seqNo, so a NO_OP row without one is contradictory: replay
        // would skip it as a preflight error while its reason is still listed.
        final BytesReference batchData = encodeBatchData(List.of(new BytesArray("{\"k\":\"v0\"}"), new BytesArray("{\"k\":\"v1\"}")));
        final AssertionError ex = expectThrows(
            AssertionError.class,
            () -> new IndexOperationBatch.TranslogRecord(
                primaryTerm.get(),
                2,
                new long[] { 0L, SequenceNumbers.UNASSIGNED_SEQ_NO },
                new long[2],
                new long[2],
                new XContentType[2],
                new BytesRef[2],
                null,
                new NoOpEntries(new int[] { 1 }, new String[] { "post-lucene failure" }),
                batchData
            )
        );
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("marked NO_OP but has no seqNo"));
    }

    public void testConstructorAssertsRowIndexesAreAscendingInRange() throws IOException {
        // The row scans stop at the first entry past the row they are looking for, so out-of-order
        // indexes would silently misread the record. A row index at or past docCount names no row of
        // the batch, so it is rejected as well.
        final BytesReference batchData = encodeBatchData(
            List.of(new BytesArray("{\"k\":\"v0\"}"), new BytesArray("{\"k\":\"v1\"}"), new BytesArray("{\"k\":\"v2\"}"))
        );
        final long term = primaryTerm.get();
        final AssertionError unsorted = expectThrows(AssertionError.class, () -> noOpRecord(term, new int[] { 1, 0 }, batchData));
        assertTrue("unexpected exception message: " + unsorted.getMessage(), unsorted.getMessage().contains("strictly increasing"));
        final AssertionError outOfRange = expectThrows(AssertionError.class, () -> noOpRecord(term, new int[] { 3 }, batchData));
        assertTrue("unexpected exception message: " + outOfRange.getMessage(), outOfRange.getMessage().contains("within [0, 3)"));
    }

    private static IndexOperationBatch.TranslogRecord noOpRecord(long term, int[] noOpRows, BytesReference batchData) {
        final String[] reasons = new String[noOpRows.length];
        for (int k = 0; k < reasons.length; k++) {
            reasons[k] = "post-lucene failure";
        }
        return new IndexOperationBatch.TranslogRecord(
            term,
            3,
            new long[] { 0L, 1L, 2L },
            new long[3],
            new long[3],
            new XContentType[3],
            new BytesRef[3],
            null,
            new NoOpEntries(noOpRows, reasons),
            batchData
        );
    }

    private static IndexOperationBatch.TranslogRecord allIndexedRecord(long term, long seqNo, String[] routings, BytesReference batchData) {
        return new IndexOperationBatch.TranslogRecord(
            term,
            1,
            new long[] { seqNo },
            new long[] { 1L },
            new long[] { 100L },
            new XContentType[] { XContentType.JSON },
            new BytesRef[] { Uid.encodeId("doc-0") },
            routings,
            null,
            batchData
        );
    }

    public void testNoOpEntriesAssertReasonsPairWithRows() {
        // Every no-op row replays with its reason, so the rows and reasons must pair up one to one
        // with no null reason (a null reason cannot be written or replayed); a record without no-op
        // rows carries null instead of an empty instance.
        expectThrows(AssertionError.class, () -> new NoOpEntries(new int[] { 0 }, null));
        expectThrows(AssertionError.class, () -> new NoOpEntries(new int[0], new String[0]));
        final AssertionError mismatch = expectThrows(AssertionError.class, () -> new NoOpEntries(new int[] { 0 }, new String[2]));
        assertTrue("unexpected exception message: " + mismatch.getMessage(), mismatch.getMessage().contains("same size"));
        final AssertionError nullReason = expectThrows(AssertionError.class, () -> new NoOpEntries(new int[] { 0 }, new String[1]));
        assertTrue("unexpected exception message: " + nullReason.getMessage(), nullReason.getMessage().contains("must have a reason"));
    }

    public void testConstructorRejectsInvalidSeqNos() throws IOException {
        // Every replayable row carries its own seqNo, which must be a real one: UNASSIGNED_SEQ_NO marks a
        // preflight row and any other negative value is corrupt. Long.MAX_VALUE is legal, nothing is
        // derived from it, and preflight rows consume no seqNo.
        final BytesReference oneRow = encodeBatchData(List.of(new BytesArray("{\"k\":\"v\"}")));
        final BytesReference twoRows = encodeBatchData(List.of(new BytesArray("{\"k\":\"v0\"}"), new BytesArray("{\"k\":\"v1\"}")));
        final long term = primaryTerm.get();
        final IllegalArgumentException negative = expectThrows(
            IllegalArgumentException.class,
            () -> allIndexedRecord(term, -1L, null, oneRow)
        );
        assertTrue("unexpected exception message: " + negative.getMessage(), negative.getMessage().contains("invalid seqNo [-1]"));

        assertEquals(Long.MAX_VALUE, allIndexedRecord(term, Long.MAX_VALUE, null, oneRow).seqNo(0));
        final IndexOperationBatch.TranslogRecord preflightThenMax = new RecordBuilder(2).skipped(0)
            .indexed(1, Long.MAX_VALUE, 1L, 101L, XContentType.JSON, "doc-1", null)
            .build(term, twoRows);
        assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, preflightThenMax.seqNo(0));
        assertEquals(Long.MAX_VALUE, preflightThenMax.seqNo(1));
        assertEquals(1, preflightThenMax.operationCount());

        final RecordBuilder corruptNoOp = new RecordBuilder(2).indexed(0, 0L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .noOp(1, Long.MIN_VALUE, "corrupt seqNo");
        final IllegalArgumentException corrupt = expectThrows(IllegalArgumentException.class, () -> corruptNoOp.build(term, twoRows));
        assertTrue("unexpected exception message: " + corrupt.getMessage(), corrupt.getMessage().contains("invalid seqNo"));

        // rows are processed in order, so a seqNo that is not above the previous row's is just as invalid
        final RecordBuilder notIncreasing = new RecordBuilder(2).indexed(0, 7L, 1L, 100L, XContentType.JSON, "doc-0", null)
            .indexed(1, randomFrom(7L, 6L), 1L, 101L, XContentType.JSON, "doc-1", null);
        final IllegalArgumentException backwards = expectThrows(IllegalArgumentException.class, () -> notIncreasing.build(term, twoRows));
        assertTrue("unexpected exception message: " + backwards.getMessage(), backwards.getMessage().contains("previous one is [7]"));
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
