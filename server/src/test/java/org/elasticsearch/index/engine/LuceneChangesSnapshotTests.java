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
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;

import java.io.IOException;

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
            accessStats,
            indexVersionCreated
        );
    }

    public void testAccessStoredFieldsSequentially() throws Exception {
        try (Store store = createStore(); Engine engine = createEngine(defaultSettings, store, createTempDir(), NoMergePolicy.INSTANCE)) {
            int smallBatch = between(5, 9);
            long seqNo = 0;
            for (int i = 0; i < smallBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null), 1, seqNo, true));
                seqNo++;
            }
            engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(1000), null), 1, 1000, true));
            seqNo = 11;
            int largeBatch = between(15, 100);
            for (int i = 0; i < largeBatch; i++) {
                engine.index(replicaIndexForDoc(createParsedDoc(Long.toString(seqNo), null), 1, seqNo, true));
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
