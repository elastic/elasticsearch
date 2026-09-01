/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.NoMergePolicy;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.engine.Engine.Operation.Origin;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;
import java.util.ArrayList;

public class LuceneChangesSnapshotTests extends SearchBasedChangesSnapshotTests {
    @Override
    protected Translog.Snapshot newRandomSnapshot(
        MapperService mapperService,
        Engine.Searcher engineSearcher,
        int searchBatchSize,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean singleConsumer,
        boolean accessStats,
        IndexVersion indexVersionCreated
    ) throws IOException {
        return new LuceneChangesSnapshot(
            mapperService,
            engineSearcher,
            searchBatchSize,
            fromSeqNo,
            toSeqNo,
            requiredFullRange,
            singleConsumer,
            accessStats
        );
    }

    /**
     * Regression test for a stale-slot bug: when columnar _id is enabled, a NO_OP tombstone that
     * appears in a later batch at the same parallel-array slot as an INDEX op from the previous batch
     * would inherit the INDEX op's id and be misread as a DELETE.
     */
    public void testNoOpAfterIndexInColumnarIdMode() throws Exception {
        try (Store store = createStore(); Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            // seqNo 0: an INDEX op with columnar _id — sets columnarIds[0] to a non-null BytesRef in
            // batch 1. A columnarId=true doc stores _id as binary doc values, which causes
            // determineIdIsColumnar to return true and allocate the columnarIds array.
            engine.index(replicaIndexForDoc(createParsedDoc("0", null, false, false, true), 1, 0, true));
            // seqNo 1: a NO_OP — advanceExact returns false (NO_OP tombstones have no _id doc value),
            // so without the fix columnarIds[0] from the previous batch survives and the NO_OP is
            // misread as a DELETE.
            engine.noOp(new Engine.NoOp(1, primaryTerm.get(), Origin.REPLICA, System.nanoTime(), "test"));
            engine.refresh("test");

            final var ops = new ArrayList<Translog.Operation>();
            // Use batch size 1 so the INDEX and NO_OP land in separate fillParallelArray calls,
            // each reusing slot 0 — this is the exact scenario that triggers the stale-slot bug.
            // The snapshot takes ownership of the searcher and closes it, so don't close it separately.
            Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try (
                LuceneChangesSnapshot snapshot = new LuceneChangesSnapshot(
                    engine.engineConfig.getMapperService(),
                    searcher,
                    1,
                    0L,
                    Long.MAX_VALUE,
                    false,
                    true,
                    false
                )
            ) {
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    ops.add(op);
                }
            }
            assertThat(ops.size(), org.hamcrest.Matchers.equalTo(2));
            assertThat(ops.get(0).opType(), org.hamcrest.Matchers.equalTo(Translog.Operation.Type.INDEX));
            assertThat(ops.get(1).opType(), org.hamcrest.Matchers.equalTo(Translog.Operation.Type.NO_OP));
        }
    }

    public void testAccessStoredFieldsSequentially() throws Exception {
        try (Store store = createStore(); Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            int smallBatch = between(5, 9);
            long seqNo = 0;
            for (int i = 0; i < smallBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null, false, false, columnarId), 1, seqNo, true));
                seqNo++;
            }
            engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(1000), null, false, false, columnarId), 1, 1000, true));
            seqNo = 11;
            int largeBatch = between(15, 100);
            for (int i = 0; i < largeBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null, false, false, columnarId), 1, seqNo, true));
                seqNo++;
            }
            // disable optimization for a small batch
            Translog.Operation op;
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    0L,
                    between(1, smallBatch),
                    false,
                    randomBoolean(),
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
            // disable optimization for non-sequential accesses
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    between(1, 3),
                    between(20, 100),
                    false,
                    randomBoolean(),
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
            // enable optimization for sequential access of 10+ docs
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    11,
                    between(21, 100),
                    false,
                    true,
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertTrue(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertTrue(snapshot.useSequentialStoredFieldsReader());
            }
            // disable optimization if snapshot is accessed by multiple consumers
            try (
                LuceneChangesSnapshot snapshot = (LuceneChangesSnapshot) engine.newChangesSnapshot(
                    "test",
                    11,
                    between(21, 100),
                    false,
                    false,
                    randomBoolean(),
                    randomLongBetween(1, ByteSizeValue.ofMb(32).getBytes())
                )
            ) {
                while ((op = snapshot.next()) != null) {
                    assertFalse(op.toString(), snapshot.useSequentialStoredFieldsReader());
                }
                assertFalse(snapshot.useSequentialStoredFieldsReader());
            }
        }
    }
}
