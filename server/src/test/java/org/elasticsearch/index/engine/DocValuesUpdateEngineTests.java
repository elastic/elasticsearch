/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.util.Bits;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.List;

import static org.elasticsearch.index.engine.Engine.Operation.Origin.PRIMARY;
import static org.elasticsearch.index.engine.Engine.Operation.Origin.REPLICA;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Engine-level tests for in-place doc-values updates ({@link Engine#docValuesUpdate}): the primary apply/reconstruct path, translog
 * recovery replay, version conflicts, and the out-of-order/idempotent/base-not-present replica paths on a seq-no-aware columnar engine.
 */
public class DocValuesUpdateEngineTests extends EngineTestCase {

    public void testDocValuesUpdateAppliesInPlaceAndReconstructsFromLucene() throws Exception {
        // Index a document with a doc-values-only numeric field, then update that field in place.
        LuceneDocument document = testDocument();
        document.add(new SortedNumericDocValuesField("counter", 1));
        ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
        Engine.IndexResult indexResult = engine.index(indexForDoc(doc));
        assertThat(indexResult.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
        engine.refresh("test");
        assertThat(readCounterOfLiveDoc(engine), equalTo(1L));

        Engine.DocValuesUpdate op = new Engine.DocValuesUpdate(
            "1",
            Uid.encodeId("1"),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            VersionType.INTERNAL,
            PRIMARY,
            System.nanoTime(),
            List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("counter", 42))
        );
        Engine.DocValuesUpdateResult result = engine.docValuesUpdate(op);
        assertThat(result.getResultType(), equalTo(Engine.Result.Type.SUCCESS));
        // the update got its own sequence number, after the index (seqNo 0)
        assertThat(result.getSeqNo(), equalTo(1L));
        assertThat(result.getTranslogLocation(), notNullValue());
        engine.refresh("test");

        // The live document still exists (the history document is soft-deleted and excluded) and carries the new value.
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            assertThat(searcher.getIndexReader().numDocs(), equalTo(1));
        }
        assertThat(readCounterOfLiveDoc(engine), equalTo(42L));

        // The operation is reconstructable from Lucene, so peer recovery and CCR can replay it.
        List<Translog.Operation> ops = readAllOperationsInLucene(engine);
        Translog.DocValuesUpdate reconstructed = ops.stream()
            .filter(o -> o instanceof Translog.DocValuesUpdate)
            .map(o -> (Translog.DocValuesUpdate) o)
            .reduce((a, b) -> {
                throw new AssertionError("expected a single doc values update, got more");
            })
            .orElseThrow(() -> new AssertionError("no doc values update reconstructed from " + ops));
        assertThat(reconstructed.seqNo(), equalTo(1L));
        assertThat(Uid.decodeId(reconstructed.uid()), equalTo("1"));
        assertThat(reconstructed.updates(), equalTo(List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("counter", 42))));
    }

    public void testDocValuesUpdateReplaysDuringTranslogRecovery() throws Exception {
        Engine initialEngine = engine;
        try {
            LuceneDocument document = testDocument();
            document.add(new SortedNumericDocValuesField("counter", 1));
            ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
            initialEngine.index(indexForDoc(doc));
            initialEngine.docValuesUpdate(
                new Engine.DocValuesUpdate(
                    "1",
                    Uid.encodeId("1"),
                    UNASSIGNED_SEQ_NO,
                    primaryTerm.get(),
                    1L,
                    VersionType.INTERNAL,
                    PRIMARY,
                    System.nanoTime(),
                    List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("counter", 42))
                )
            );
        } finally {
            IOUtils.close(initialEngine);
        }

        // Recover a fresh engine from the translog: the doc-values update is replayed and reproduces the updated value.
        try (Engine recoveringEngine = new InternalEngine(initialEngine.config())) {
            recoverFromTranslog(recoveringEngine, translogHandler, Long.MAX_VALUE);
            recoveringEngine.refresh("test");
            assertThat(readCounterOfLiveDoc(recoveringEngine), equalTo(42L));
        }
    }

    public void testDocValuesUpdateConflictsWhenBaseSeqNoChanged() throws Exception {
        // A concurrent change between the pre-update get and the apply must make the in-place update conflict, not silently patch a
        // document it was not computed against.
        runOnColumnarEngine(engine -> {
            LuceneDocument document = testDocument();
            document.add(new SortedNumericDocValuesField("counter", 1));
            ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
            Engine.IndexResult indexResult = engine.index(indexForDoc(doc));
            long base = indexResult.getSeqNo();

            // First update against the correct base applies and advances the document's effective identity.
            Engine.DocValuesUpdateResult applied = engine.docValuesUpdate(primaryDocValuesUpdate("1", 42, base, indexResult.getTerm()));
            assertThat(applied.getResultType(), equalTo(Engine.Result.Type.SUCCESS));

            // A second update still carrying the now-stale base must conflict and must not apply.
            Engine.DocValuesUpdateResult stale = engine.docValuesUpdate(primaryDocValuesUpdate("1", 99, base, indexResult.getTerm()));
            assertThat(stale.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            assertThat(stale.getFailure(), instanceOf(VersionConflictEngineException.class));
            engine.refresh("test");
            assertThat(readCounterOfLiveDoc(engine), equalTo(42L));
        });
    }

    public void testDocValuesUpdateConflictsWhenDocumentDeleted() throws Exception {
        runOnColumnarEngine(engine -> {
            LuceneDocument document = testDocument();
            document.add(new SortedNumericDocValuesField("counter", 1));
            ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
            Engine.IndexResult indexResult = engine.index(indexForDoc(doc));
            engine.delete(new Engine.Delete("1", newUid(doc), primaryTerm.get()));

            // The document the update was prepared against is gone, so the in-place update conflicts instead of resurrecting it.
            Engine.DocValuesUpdateResult result = engine.docValuesUpdate(
                primaryDocValuesUpdate("1", 42, indexResult.getSeqNo(), indexResult.getTerm())
            );
            assertThat(result.getResultType(), equalTo(Engine.Result.Type.FAILURE));
            assertThat(result.getFailure(), instanceOf(VersionConflictEngineException.class));
        });
    }

    public void testDocValuesUpdateStaleReplicaOpIsNotAppliedInPlace() throws Exception {
        // A replica can receive two in-place updates of the same document out of order. The newer one must win regardless of arrival
        // order, and the stale one must not overwrite the live document's columns.
        runOnColumnarEngine(engine -> {
            LuceneDocument document = testDocument();
            document.add(new SortedNumericDocValuesField("counter", 1));
            ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
            engine.advanceMaxSeqNoOfUpdatesOrDeletes(2L);
            engine.index(replicaIndexForDoc(doc, 1L, 0L, false));

            // Two concurrent updates computed against the same base (the indexed document at seqNo 0). The newer operation (seqNo 2)
            // arrives first and is applied in place because the live document is still exactly its base.
            assertThat(
                engine.docValuesUpdate(replicaDocValuesUpdate("1", 2L, 1L, 99, 0L, primaryTerm.get())).getResultType(),
                equalTo(Engine.Result.Type.SUCCESS)
            );
            engine.refresh("test"); // evict the id from the version map so the stale op resolves against the effective seq_no in Lucene
            assertThat(readCounterOfLiveDoc(engine), equalTo(99L));

            // The stale operation (seqNo 1) arrives second: its base (seqNo 0) was applied but the live document has moved past it, so it
            // is obsolete — recorded as history but not applied to the live document.
            assertThat(
                engine.docValuesUpdate(replicaDocValuesUpdate("1", 1L, 1L, 42, 0L, primaryTerm.get())).getResultType(),
                equalTo(Engine.Result.Type.SUCCESS)
            );
            engine.refresh("test");
            assertThat(readCounterOfLiveDoc(engine), equalTo(99L));

            try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
                assertThat(searcher.getIndexReader().numDocs(), equalTo(1));
            }
            // Both operations left a history document so peer recovery and CCR can replay the full history, even the stale one.
            assertThat(countHistoryDocsInLucene(engine), equalTo(2));
        });
    }

    public void testDocValuesUpdateInOrderReplicaOpsApplyInPlace() throws Exception {
        runOnColumnarEngine(engine -> {
            LuceneDocument document = testDocument();
            document.add(new SortedNumericDocValuesField("counter", 1));
            ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
            engine.advanceMaxSeqNoOfUpdatesOrDeletes(2L);
            engine.index(replicaIndexForDoc(doc, 1L, 0L, false));

            // First update is computed against the indexed document (base seqNo 0) and applies.
            engine.docValuesUpdate(replicaDocValuesUpdate("1", 1L, 1L, 42, 0L, primaryTerm.get()));
            engine.refresh("test");
            assertThat(readCounterOfLiveDoc(engine), equalTo(42L));

            // Second update is computed against the result of the first (base seqNo 1) and applies on top of it.
            engine.docValuesUpdate(replicaDocValuesUpdate("1", 2L, 1L, 99, 1L, primaryTerm.get()));
            engine.refresh("test");
            assertThat(readCounterOfLiveDoc(engine), equalTo(99L));
            assertThat(countHistoryDocsInLucene(engine), equalTo(2));
        });
    }

    public void testDocValuesUpdateReplicaOpIsIdempotent() throws Exception {
        // Recovery can replay an operation that is already in the Lucene commit; applying it twice must be a no-op in Lucene.
        runOnColumnarEngine(engine -> {
            LuceneDocument document = testDocument();
            document.add(new SortedNumericDocValuesField("counter", 1));
            ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
            engine.advanceMaxSeqNoOfUpdatesOrDeletes(1L);
            engine.index(replicaIndexForDoc(doc, 1L, 0L, false));

            engine.docValuesUpdate(replicaDocValuesUpdate("1", 1L, 1L, 42, 0L, primaryTerm.get()));
            engine.refresh("test");
            assertThat(readCounterOfLiveDoc(engine), equalTo(42L));
            assertThat(countHistoryDocsInLucene(engine), equalTo(1));

            // Replaying the same operation (same seq_no) is skipped: the value is unchanged and no second history document is written.
            assertThat(
                engine.docValuesUpdate(replicaDocValuesUpdate("1", 1L, 1L, 42, 0L, primaryTerm.get())).getResultType(),
                equalTo(Engine.Result.Type.SUCCESS)
            );
            engine.refresh("test");
            assertThat(readCounterOfLiveDoc(engine), equalTo(42L));
            assertThat(countHistoryDocsInLucene(engine), equalTo(1));
        });
    }

    public void testDocValuesUpdateReplicaFailsWhenBaseNotPresent() throws Exception {
        // A delta can race ahead of its base version on a copy that is still catching up (peer recovery / CCR deliver operations
        // concurrently). Applying it against an older document would corrupt it, so the operation is failed and retried once the base,
        // delivered in sequence-number order, is present. The failure is retryable, not a client error.
        runOnColumnarEngine(engine -> {
            LuceneDocument document = testDocument();
            document.add(new SortedNumericDocValuesField("counter", 1));
            ParsedDocument doc = testParsedDocument("1", null, document, new BytesArray("{}"));
            engine.advanceMaxSeqNoOfUpdatesOrDeletes(2L);
            engine.index(replicaIndexForDoc(doc, 1L, 0L, false));

            // The delta's base is seqNo 1, which has not been applied on this copy (only the index at seqNo 0 has): it cannot be applied.
            expectThrows(
                DocValuesUpdateBaseNotPresentException.class,
                () -> engine.docValuesUpdate(replicaDocValuesUpdate("1", 2L, 1L, 99, 1L, primaryTerm.get()))
            );
            engine.refresh("test");
            // The live document is untouched and no history document was written for the failed operation.
            assertThat(readCounterOfLiveDoc(engine), equalTo(1L));
            assertThat(countHistoryDocsInLucene(engine), equalTo(0));
        });
    }

    /**
     * Runs {@code body} against a fresh strict-columnar engine with sequence numbers enabled, which is the only configuration that
     * supports in-place doc-values updates (see {@link IndexSettings#seqNoAwareDocValuesUpdates()}). The companion
     * {@code _dv_update_seq_no} column it writes is what lets a replica resolve out-of-order updates. Columnar uses doc-values-only
     * {@code _seq_no}, so the shared {@link #seqNoIndexOptions} must match for the duration or the base and history documents disagree
     * on the {@code _seq_no} field structure.
     */
    private void runOnColumnarEngine(CheckedConsumer<InternalEngine, Exception> body) throws Exception {
        final SeqNoFieldMapper.SeqNoIndexOptions previous = seqNoIndexOptions;
        seqNoIndexOptions = SeqNoFieldMapper.SeqNoIndexOptions.DOC_VALUES_ONLY;
        try {
            Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_GC_DELETES_SETTING.getKey(), "1h")
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                .put(IndexSettings.SEQ_NO_INDEX_OPTIONS_SETTING.getKey(), seqNoIndexOptions)
                .put("index.disable_sequence_numbers", false)
                .build();
            IndexSettings columnarSettings = IndexSettingsModule.newIndexSettings("index", settings);
            assertTrue("in-place doc-values updates require a seq-no-aware columnar index", columnarSettings.seqNoAwareDocValuesUpdates());
            try (
                Store store = createStore();
                // No merges so the born-soft-deleted history documents are not reclaimed and can be counted.
                InternalEngine columnarEngine = createEngine(columnarSettings, store, createTempDir(), NoMergePolicy.INSTANCE)
            ) {
                body.accept(columnarEngine);
            }
        } finally {
            seqNoIndexOptions = previous;
        }
    }

    private Engine.DocValuesUpdate replicaDocValuesUpdate(
        String id,
        long seqNo,
        long version,
        long counter,
        long baseSeqNo,
        long baseTerm
    ) {
        return new Engine.DocValuesUpdate(
            id,
            Uid.encodeId(id),
            seqNo,
            primaryTerm.get(),
            version,
            null,
            REPLICA,
            System.nanoTime(),
            List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("counter", counter)),
            baseSeqNo,
            baseTerm
        );
    }

    private Engine.DocValuesUpdate primaryDocValuesUpdate(String id, long counter, long ifSeqNo, long ifPrimaryTerm) {
        return new Engine.DocValuesUpdate(
            id,
            Uid.encodeId(id),
            UNASSIGNED_SEQ_NO,
            primaryTerm.get(),
            1L,
            VersionType.INTERNAL,
            PRIMARY,
            System.nanoTime(),
            List.of(new Translog.DocValuesUpdate.NumericFieldUpdate("counter", counter)),
            ifSeqNo,
            ifPrimaryTerm
        );
    }

    /**
     * The number of born-soft-deleted history documents in Lucene. Each applied or stale in-place update writes exactly one, so this is
     * the count of operations recorded for history reconstruction (peer recovery and CCR). The live document is patched in place and is
     * never soft-deleted, so it is not counted.
     */
    private static int countHistoryDocsInLucene(Engine engine) throws IOException {
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            return searcher.getIndexReader().maxDoc() - searcher.getIndexReader().numDocs();
        }
    }

    private static long readCounterOfLiveDoc(Engine engine) throws IOException {
        try (Engine.Searcher searcher = engine.acquireSearcher("test")) {
            for (LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                SortedNumericDocValues counter = leaf.reader().getSortedNumericDocValues("counter");
                if (counter == null) {
                    continue;
                }
                Bits liveDocs = leaf.reader().getLiveDocs();
                for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
                    if (liveDocs != null && liveDocs.get(docId) == false) {
                        continue;
                    }
                    if (counter.advanceExact(docId)) {
                        return counter.nextValue();
                    }
                }
            }
        }
        throw new AssertionError("no live document with a [counter] doc value");
    }
}
