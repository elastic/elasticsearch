/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.translog;

import org.elasticsearch.Build;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.DiskIoBufferPool;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.eirf.EirfBatch;
import org.elasticsearch.eirf.EirfEncoder;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.TranslogOperationAsserter;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.util.BigArrays.NON_RECYCLING_INSTANCE;

public class TranslogIndexBatchTests extends ESTestCase {

    private final ShardId shardId = new ShardId("index", "_na_", 1);
    private final AtomicLong primaryTerm = new AtomicLong();
    private Path translogDir;
    private Translog translog;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        assumeTrue("batch indexing requires snapshot builds", Build.current().isSnapshot());
        primaryTerm.set(randomLongBetween(1, Integer.MAX_VALUE));
        translogDir = createTempDir();
        translog = create(translogDir);
    }

    @Override
    @After
    public void tearDown() throws Exception {
        try {
            if (translog != null) {
                translog.close();
            }
        } finally {
            super.tearDown();
        }
        IOUtils.rm(translogDir);
    }

    private Translog create(Path path) throws IOException {
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
            (d, s, l) -> {},
            true
        );
        final String translogUUID = Translog.createEmptyTranslog(path, SequenceNumbers.NO_OPS_PERFORMED, shardId, primaryTerm.get());
        return new Translog(
            translogConfig,
            translogUUID,
            new TranslogDeletionPolicy(),
            () -> SequenceNumbers.NO_OPS_PERFORMED,
            primaryTerm::get,
            seqNo -> {},
            TranslogOperationAsserter.DEFAULT
        );
    }

    /**
     * Build a batch with the given documents (one map per doc) and assign sequence numbers
     * starting at {@code firstSeqNo}. Returns the batch and the original source bytes for each doc
     * so the test can assert round-trip equality.
     */
    private static Translog.IndexBatch buildBatch(List<Map<String, Object>> docs, XContentType xContentType, long firstSeqNo, long term)
        throws IOException {
        final List<BytesReference> sources = new ArrayList<>(docs.size());
        for (Map<String, Object> doc : docs) {
            try (XContentBuilder b = XContentBuilder.builder(xContentType.xContent())) {
                b.map(doc);
                sources.add(BytesReference.bytes(b));
            }
        }
        final EirfBatch eirf = EirfEncoder.encode(sources, xContentType);
        final BytesReference batchData;
        try (eirf) {
            batchData = new BytesArray(eirf.data().toBytesRef(), true);
        }
        final List<Translog.IndexBatch.Op> metas = new ArrayList<>(docs.size());
        for (int i = 0; i < docs.size(); i++) {
            metas.add(
                new Translog.IndexBatch.IndexOp(
                    1L,
                    firstSeqNo + i,
                    100L + i,
                    i,
                    xContentType,
                    Uid.encodeId("doc-" + i),
                    i % 2 == 0 ? null : "route-" + i
                )
            );
        }
        return new Translog.IndexBatch(batchData, term, metas);
    }

    public void testWireFormatRoundTrip() throws IOException {
        // EIRF only round-trips JSON sources (its parser flips allowDuplicateKeys, which SMILE/CBOR
        // reject). The xContentType byte on the wire is independent of how the EIRF bytes were
        // produced, so we verify the envelope round-trips for several types while encoding via JSON.
        final XContentType xContentType = randomFrom(XContentType.JSON, XContentType.SMILE, XContentType.CBOR, XContentType.YAML);
        final EirfBatch eirf = EirfEncoder.encode(
            List.of(
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("a", 1, "b", "hello"))),
                BytesReference.bytes(XContentBuilder.builder(XContentType.JSON.xContent()).map(Map.of("a", 2, "b", "world")))
            ),
            XContentType.JSON
        );
        final BytesReference batchData;
        try (eirf) {
            batchData = new BytesArray(eirf.data().toBytesRef(), true);
        }
        final List<Translog.IndexBatch.Op> metas = List.of(
            new Translog.IndexBatch.IndexOp(1L, 5L, 100L, 0, xContentType, Uid.encodeId("doc-0"), null),
            new Translog.IndexBatch.IndexOp(1L, 6L, 101L, 1, xContentType, Uid.encodeId("doc-1"), "route")
        );
        final Translog.IndexBatch batch = new Translog.IndexBatch(batchData, primaryTerm.get(), metas);

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            batch.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                final byte typeByte = in.readByte();
                assertEquals(Translog.Record.Type.BATCH.id(), typeByte);
                final Translog.IndexBatch read = Translog.IndexBatch.readFrom(in);
                assertEquals(batch, read);
                final Translog.IndexBatch.IndexOp op0 = (Translog.IndexBatch.IndexOp) read.ops().get(0);
                final Translog.IndexBatch.IndexOp op1 = (Translog.IndexBatch.IndexOp) read.ops().get(1);
                assertEquals(xContentType, op0.xContentType());
                assertEquals(xContentType, op1.xContentType());
            }
        }
    }

    public void testSnapshotExplodesBatchIntoIndexOps() throws IOException {
        // EIRF's parseToScratch flips allowDuplicateKeys on the source parser, which only JSON
        // supports today, so the EIRF-encoded sources here are JSON.
        final XContentType xContentType = XContentType.JSON;
        final List<Map<String, Object>> docs = List.of(
            Map.of("field", "alpha", "n", 1),
            Map.of("field", "beta", "n", 2),
            Map.of("field", "gamma", "n", 3)
        );
        final Translog.IndexBatch batch = buildBatch(docs, xContentType, 0L, primaryTerm.get());

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
                    // numeric types may widen (int -> long) through EIRF; compare via Number.longValue or string equality
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

    public void testInterleavedBatchesAndRegularOps() throws IOException {
        final long term = primaryTerm.get();
        final Translog.Index op0 = new Translog.Index(Uid.encodeId("solo-0"), 0, term, 1L, new BytesArray("{\"k\":\"v0\"}"), null, -1L);
        translog.add(op0);

        final Translog.IndexBatch batchA = buildBatch(List.of(Map.of("k", "v1"), Map.of("k", "v2")), XContentType.JSON, 1L, term);
        translog.add(batchA);

        translog.add(new Translog.Delete("solo-3", 3, term));

        final Translog.IndexBatch batchB = buildBatch(List.of(Map.of("k", "v4"), Map.of("k", "v5")), XContentType.JSON, 4L, term);
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
        final Translog.IndexBatch batch = buildBatch(docs, XContentType.JSON, 10L, term);
        translog.add(batch);

        assertEquals(docCount, translog.totalOperations());

        translog.sync();
        final Checkpoint cp = translog.getLastSyncedCheckpoint();
        assertEquals(10L, cp.minSeqNo);
        assertEquals(10L + docCount - 1, cp.maxSeqNo);
        assertEquals(docCount, cp.numOps);
    }

    public void testSnapshotExplodesMixedIndexAndNoOpEntries() throws IOException {
        // Simulates a primary sub-batch where the middle op succeeded preflight but failed
        // post-Lucene with an assigned seqNo, so the engine converted it into a NoOpOp while the
        // surrounding ops remained IndexOps. Replay must emit Index, NoOp, Index in that order.
        final XContentType xContentType = XContentType.JSON;
        final List<BytesReference> sources = List.of(new BytesArray("{\"k\":\"row-0\"}"), new BytesArray("{\"k\":\"row-2\"}"));
        final EirfBatch eirf = EirfEncoder.encode(sources, xContentType);
        final BytesReference batchData;
        try (eirf) {
            batchData = new BytesArray(eirf.data().toBytesRef(), true);
        }

        final long term = primaryTerm.get();
        final List<Translog.IndexBatch.Op> metas = List.of(
            new Translog.IndexBatch.IndexOp(1L, 0L, 100L, 0, xContentType, Uid.encodeId("doc-0"), null),
            new Translog.IndexBatch.NoOpOp(1L, "post-lucene failure"),
            new Translog.IndexBatch.IndexOp(1L, 2L, 102L, 1, xContentType, Uid.encodeId("doc-2"), null)
        );
        final Translog.IndexBatch batch = new Translog.IndexBatch(batchData, term, metas);
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
            // Crucially: the NoOp between the two IndexOps did not consume a row from the EIRF
            // batch, and idx2's explicit rowIndex (1) correctly maps to the second row.
            assertEquals("row-2", XContentHelper.convertToMap(idx2.source(), false, xContentType).v2().get("k"));

            assertNull(snapshot.next());
        }
    }

    public void testExplodeHonoursSparseRowIndex() throws IOException {
        // Simulates the primary path where the middle op of a 3-row sub-batch hit a preflight
        // failure (UNASSIGNED_SEQ_NO) and was dropped from the ops list. The EIRF batch still
        // carries all three rows; surviving IndexOps must point at their original row indices
        // so the replayed source matches the surviving op's uid/seqNo.
        final XContentType xContentType = XContentType.JSON;
        final List<BytesReference> sources = List.of(
            new BytesArray("{\"k\":\"row-0\"}"),
            new BytesArray("{\"k\":\"row-1\"}"),
            new BytesArray("{\"k\":\"row-2\"}")
        );
        final EirfBatch eirf = EirfEncoder.encode(sources, xContentType);
        final BytesReference batchData;
        try (eirf) {
            batchData = new BytesArray(eirf.data().toBytesRef(), true);
        }

        final long term = primaryTerm.get();
        // Two surviving IndexOps point at rows 0 and 2; row 1's op was dropped (preflight skip).
        final List<Translog.IndexBatch.Op> metas = List.of(
            new Translog.IndexBatch.IndexOp(1L, 0L, 100L, 0, xContentType, Uid.encodeId("doc-0"), null),
            new Translog.IndexBatch.IndexOp(1L, 2L, 102L, 2, xContentType, Uid.encodeId("doc-2"), null)
        );
        final Translog.IndexBatch batch = new Translog.IndexBatch(batchData, term, metas);
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
            // Crucially: row-2, not row-1 (which list-position-based explode would have returned).
            assertEquals("row-2", XContentHelper.convertToMap(op2.source(), false, xContentType).v2().get("k"));

            assertNull(snapshot.next());
        }
    }

    public void testRowIndexOutOfRangeThrows() throws IOException {
        final XContentType xContentType = XContentType.JSON;
        final EirfBatch eirf = EirfEncoder.encode(List.of(new BytesArray("{\"k\":\"v\"}")), xContentType);
        final BytesReference batchData;
        try (eirf) {
            batchData = new BytesArray(eirf.data().toBytesRef(), true);
        }
        // rowIndex 5 is out of range for a 1-row batch.
        final Translog.IndexBatch batch = new Translog.IndexBatch(
            batchData,
            primaryTerm.get(),
            List.of(new Translog.IndexBatch.IndexOp(1L, 0L, 0L, 5, xContentType, Uid.encodeId("doc-0"), null))
        );
        final IOException ex = expectThrows(IOException.class, batch::explode);
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage().contains("rowIndex"));
    }

    public void testNegativeRowIndexRejectedAtConstruction() {
        expectThrows(
            IllegalArgumentException.class,
            () -> new Translog.IndexBatch.IndexOp(1L, 0L, 0L, -1, XContentType.JSON, Uid.encodeId("doc-0"), null)
        );
    }

    public void testReadByLocationThrowsForBatch() throws IOException {
        final long term = primaryTerm.get();
        final Translog.IndexBatch batch = buildBatch(List.of(Map.of("k", "v0"), Map.of("k", "v1")), XContentType.JSON, 0L, term);
        final Translog.Location location = translog.add(batch);

        final IOException ex = expectThrows(IOException.class, () -> translog.readOperation(location));
        assertTrue("unexpected exception message: " + ex.getMessage(), ex.getMessage() != null && ex.getMessage().contains("batch"));
    }

}
