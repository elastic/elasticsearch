/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.lucene.index.ElasticsearchDirectoryReader;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.flattened.ColumnarKeyedBinaryDocValues;
import org.elasticsearch.index.codec.flattened.FlattenedDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;

public class KeyedFlattenedDocValuesBlockLoaderTests extends ESTestCase {

    private static final String KEYED_FIELD = "field._keyed";
    private static final String KEY = "host.name";

    /**
     * Regression test for the sorted-set keyed reader: after {@code read()} has been called,
     * {@code canReuse(int)} must answer without throwing so the {@code ValuesSourceReaderOperator}
     * can reuse the reader for the next page on the same segment. The key-filtered view is an
     * {@code AbstractSortedSetDocValues} so it cannot answer {@code docID()} on the underlying
     * iterator surface — the reader has to track the last requested doc itself. This is the
     * unit-level counterpart to the {@code csv-spec:field_extract.inlineStatsOverFlattenedSubfield}
     * suite-level coverage, which only catches the bug when the random {@code smallChunks}
     * setting forces multiple page loads per segment.
     */
    public void testCanReuseAfterReadOnSortedSetKeyedReader() throws IOException {
        assertCanReuseTracksLastReadDoc(false);
    }

    /**
     * Mirror of {@link #testCanReuseAfterReadOnSortedSetKeyedReader} for the binary-doc-values
     * path, kept symmetric so a future change to the sorted-set reader's docId tracking does
     * not silently diverge from the binary reader's behavior.
     */
    public void testCanReuseAfterReadOnBinaryKeyedReader() throws IOException {
        assertCanReuseTracksLastReadDoc(true);
    }

    private void assertCanReuseTracksLastReadDoc(boolean binary) throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, binary)) {
            addDoc(writer, binary, KEY + "\0server-a");
            addDoc(writer, binary, KEY + "\0server-b");
            addDoc(writer, binary, KEY + "\0server-c");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = new KeyedFlattenedDocValuesBlockLoader(KEYED_FIELD, KEY, binary).reader(
                    new NoopCircuitBreaker("test"),
                    leaf
                );

                // Read docs [0, 1] in a single page. This positions the reader at doc 1.
                columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1), 0, false).close();

                // Going forward (or staying put) must be reusable; the previous implementation
                // threw UnsupportedOperationException here on the sorted-set path because it
                // delegated docId() to AbstractSortedSetDocValues.docID(), which is the
                // unsupported DocIdSetIterator surface for that wrapper.
                assertTrue("reader should be reusable for doc == last read", columnReader.canReuse(1));
                assertTrue("reader should be reusable for doc > last read", columnReader.canReuse(2));

                // Going backwards must not be reusable. This pins down that docId() reflects
                // the last requested doc, not a fixed value like -1 that would also pass the
                // forward checks above.
                assertFalse("reader must not be reused going backwards", columnReader.canReuse(0));

                // A second read advances the tracked docId so canReuse keeps moving forward,
                // matching what ValuesSourceReaderOperator expects when a new page lands on
                // the same segment after a previous page positioned the reader.
                columnReader.read(TestBlock.factory(), TestBlock.docs(2), 0, false).close();
                assertFalse("reader must not be reused for a doc strictly before last read", columnReader.canReuse(1));
                assertTrue("reader should still be reusable for doc == last read after another read", columnReader.canReuse(2));

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that when the {@code ._keyed} field is stored via {@link FlattenedDocValuesFormat},
     * {@link KeyedFlattenedDocValuesBlockLoader} picks the {@link ColumnarKeyedBinaryDocValues} fast path
     * (i.e. {@link org.elasticsearch.index.fielddata.KeyLookupArrayOrderBinaryDocValues}) and that
     * {@code canReuse} still correctly tracks the last-read document ID.
     */
    public void testCanReuseAfterReadOnColumnarKeyedReader() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            addColumnarDoc(writer, KEY + "\0server-a");
            addColumnarDoc(writer, KEY + "\0server-b");
            addColumnarDoc(writer, KEY + "\0server-c");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);

                // Verify the fast path: the underlying BDV must be a ColumnarKeyedBinaryDocValues.
                assertTrue(
                    "expected ColumnarKeyedBinaryDocValues from FlattenedDocValuesFormat",
                    leaf.reader().getBinaryDocValues(KEYED_FIELD) instanceof ColumnarKeyedBinaryDocValues
                );

                BlockLoader.ColumnAtATimeReader columnReader = new KeyedFlattenedDocValuesBlockLoader(
                    KEYED_FIELD,
                    KEY,
                    true,  // usesBinaryDocValues
                    true   // usesArrayOrderBinaryDocValues → uses KeyLookupArrayOrderBinaryDocValues for ColumnarKeyedBinaryDocValues
                ).reader(new NoopCircuitBreaker("test"), leaf);

                columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1), 0, false).close();

                assertTrue("reader should be reusable for doc == last read", columnReader.canReuse(1));
                assertTrue("reader should be reusable for doc > last read", columnReader.canReuse(2));
                assertFalse("reader must not be reused going backwards", columnReader.canReuse(0));

                columnReader.read(TestBlock.factory(), TestBlock.docs(2), 0, false).close();
                assertFalse("reader must not be reused for a doc strictly before last read", columnReader.canReuse(1));
                assertTrue("reader should still be reusable for doc == last read after another read", columnReader.canReuse(2));

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that the columnar batch path ({@link org.elasticsearch.index.codec.flattened.KeyColumnBatchReader})
     * produces the same values as the per-doc path for the common cases:
     * single value, multi-value (sorted + deduplicated), all-null slots, and absent key.
     */
    public void testColumnarBatchReaderValues_singleValue() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            addColumnarDoc(writer, KEY + "\0alpha");
            addColumnarDoc(writer, KEY + "\0beta");
            addColumnarDoc(writer, KEY + "\0gamma");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);
                assertEquals(3, block.size());
                assertEquals(new BytesRef("alpha"), block.get(0));
                assertEquals(new BytesRef("beta"), block.get(1));
                assertEquals(new BytesRef("gamma"), block.get(2));
                block.close();

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that multi-valued slots are emitted sorted and deduplicated, matching the semantics
     * of the per-doc {@code KeyLookupArrayOrderBinaryDocValues} path.
     */
    public void testColumnarBatchReaderValues_multiValueSortAndDedup() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            // Doc 0: two values in reverse order — batch path must sort them.
            addColumnarDoc(writer, KEY + "\0zebra", KEY + "\0apple");
            // Doc 1: two identical values — batch path must deduplicate them.
            addColumnarDoc(writer, KEY + "\0same", KEY + "\0same");
            // Doc 2: three values with one duplicate.
            addColumnarDoc(writer, KEY + "\0c", KEY + "\0a", KEY + "\0b", KEY + "\0a");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);
                assertEquals(3, block.size());

                // Doc 0: sorted → [apple, zebra].
                assertEquals(List.of(new BytesRef("apple"), new BytesRef("zebra")), block.get(0));

                // Doc 1: two identical values → single value after dedup (no position entry).
                assertEquals(new BytesRef("same"), block.get(1));

                // Doc 2: [a, b, c] after sort+dedup of [a, b, c, a].
                assertEquals(List.of(new BytesRef("a"), new BytesRef("b"), new BytesRef("c")), block.get(2));

                block.close();
                columnReader.close();
            }
        }
    }

    /**
     * Verifies that a document that does not contain the requested key emits a null entry,
     * and that a segment where the requested key does not appear at all also produces all-null output.
     */
    public void testColumnarBatchReaderValues_missingKey() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            // Doc 0 has KEY. Doc 1 has a different key. Doc 2 has KEY again.
            addColumnarDoc(writer, KEY + "\0present");
            addColumnarDoc(writer, "other.key\0value");
            addColumnarDoc(writer, KEY + "\0also-present");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);
                assertEquals(3, block.size());
                assertEquals(new BytesRef("present"), block.get(0));
                assertNull("doc missing key must produce null", block.get(1));
                assertEquals(new BytesRef("also-present"), block.get(2));
                block.close();

                // Now request a key that doesn't appear anywhere in this segment.
                BlockLoader.ColumnAtATimeReader absentReader = new KeyedFlattenedDocValuesBlockLoader(KEYED_FIELD, "absent.key", true, true)
                    .reader(new NoopCircuitBreaker("test"), leaf);
                TestBlock absentBlock = (TestBlock) absentReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);
                assertEquals(3, absentBlock.size());
                for (int i = 0; i < 3; i++) {
                    assertNull("absent key must produce null for every doc", absentBlock.get(i));
                }
                absentBlock.close();
                absentReader.close();

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that {@code offset > 0} causes the first {@code offset} docs to be skipped,
     * i.e. the returned block has {@code docs.count() - offset} positions.
     */
    public void testColumnarBatchReaderValues_offset() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            addColumnarDoc(writer, KEY + "\0skip-me");
            addColumnarDoc(writer, KEY + "\0load-me");
            addColumnarDoc(writer, KEY + "\0also-load");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                // offset=1 skips doc 0; only docs 1 and 2 are loaded.
                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 1, false);
                assertEquals(2, block.size());
                assertEquals(new BytesRef("load-me"), block.get(0));
                assertEquals(new BytesRef("also-load"), block.get(1));
                block.close();

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that duplicate doc ids in {@code Docs} are handled correctly: the cursor
     * is idempotent when asked to advance to a doc it has already landed on.
     */
    public void testColumnarBatchReaderValues_duplicateDocs() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            addColumnarDoc(writer, KEY + "\0alpha");
            addColumnarDoc(writer, KEY + "\0beta");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                // Duplicate doc 0 and doc 1 — TestBlock.docs sets mayContainDuplicates=true.
                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 0, 1, 1), 0, false);
                assertEquals(4, block.size());
                assertEquals(new BytesRef("alpha"), block.get(0));
                assertEquals(new BytesRef("alpha"), block.get(1));
                assertEquals(new BytesRef("beta"), block.get(2));
                assertEquals(new BytesRef("beta"), block.get(3));
                block.close();

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that the batch reader correctly crosses block boundaries. A tiny block size
     * (4 docs per block) forces the three indexed docs into separate blocks when there are enough
     * other columns, so {@link org.elasticsearch.index.codec.flattened.SequentialColumnReader#advance}
     * must skip entire blocks to reach the target documents.
     */
    public void testColumnarBatchReaderValues_multiBlock() throws IOException {
        // Use a very small maxDocsPerBlock to force multiple blocks per column.
        FlattenedDocValuesFormat tinyBlocks = new FlattenedDocValuesFormat(
            FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT,
            2, // 2 docs per block at most
            FlattenedDocValuesFormat.MIN_COMPRESS_BYTES_DEFAULT,
            FlattenedDocValuesFormat.MAX_BUFFERED_BYTES_DEFAULT
        );
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig().setCodec(new Elasticsearch93Lucene104Codec() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return KEYED_FIELD.equals(field) ? tinyBlocks : super.getDocValuesFormatForField(field);
                }
            });
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc)) {
                // Four docs so the column spans two blocks of 2 each.
                addColumnarDoc(writer, KEY + "\0block0-doc0");
                addColumnarDoc(writer, KEY + "\0block0-doc1");
                addColumnarDoc(writer, KEY + "\0block1-doc0");
                addColumnarDoc(writer, KEY + "\0block1-doc1");

                try (IndexReader reader = openReader(writer)) {
                    LeafReaderContext leaf = reader.leaves().get(0);
                    BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                    // Load all four docs to exercise cross-block forward seek.
                    TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2, 3), 0, false);
                    assertEquals(4, block.size());
                    assertEquals(new BytesRef("block0-doc0"), block.get(0));
                    assertEquals(new BytesRef("block0-doc1"), block.get(1));
                    assertEquals(new BytesRef("block1-doc0"), block.get(2));
                    assertEquals(new BytesRef("block1-doc1"), block.get(3));
                    block.close();

                    // Second page: skip block 0 entirely (advance must not decompresses it).
                    columnReader = newColumnarBatchLoader(leaf);
                    TestBlock sparseBlock = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(2, 3), 0, false);
                    assertEquals(2, sparseBlock.size());
                    assertEquals(new BytesRef("block1-doc0"), sparseBlock.get(0));
                    assertEquals(new BytesRef("block1-doc1"), sparseBlock.get(1));
                    sparseBlock.close();

                    columnReader.close();
                }
            }
        }
    }

    /**
     * Verifies that the fast path correctly handles a page with gaps inside a single block.
     * When docs [0, 2] are requested from a 4-doc all-single-slot no-null block, the run
     * coalescer emits two separate runs (slot 0 alone, then slot 2 alone) because the slot
     * indices are not consecutive; both values must still be correct.
     */
    public void testRunCoalescing_sparsePageWithinBlock() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            addColumnarDoc(writer, KEY + "\0first");
            addColumnarDoc(writer, KEY + "\0second");
            addColumnarDoc(writer, KEY + "\0third");
            addColumnarDoc(writer, KEY + "\0fourth");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                // Docs [0, 2] — gaps at docs 1 and 3. Both are present so the fast path is taken,
                // but the non-consecutive slot indices (0, then 2) force two arraycopy runs.
                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 2), 0, false);
                assertEquals(2, block.size());
                assertEquals(new BytesRef("first"), block.get(0));
                assertEquals(new BytesRef("third"), block.get(1));
                block.close();

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that a block containing a null slot falls through to the {@code emitDoc} per-doc
     * path. The fast path bails when {@code blockHasNulls()} is true; {@code emitDoc} then reads
     * slot metadata from the pre-built {@code slotLens} and {@code valueOffsets} tables. The null
     * doc must produce {@code null} in the block while surrounding non-null docs remain correct.
     */
    public void testRunCoalescing_nullSlotFallsBackToEmitDoc() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newColumnarIndexWriter(dir)) {
            addColumnarDoc(writer, KEY + "\0first");
            // Doc 1: KEY is present but the value is null.
            LuceneDocument nullDoc = new LuceneDocument();
            KeyedArrayOrderInlineNull.recordNull(nullDoc, KEYED_FIELD, new BytesRef(KEY + "\0"));
            writer.addDocument(nullDoc);
            addColumnarDoc(writer, KEY + "\0third");

            try (IndexReader reader = openReader(writer)) {
                LeafReaderContext leaf = reader.leaves().get(0);
                BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                // blockHasNulls()=true → fast path bails at the first doc; emitDoc handles all three.
                TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(0, 1, 2), 0, false);
                assertEquals(3, block.size());
                assertEquals(new BytesRef("first"), block.get(0));
                assertNull("null-value slot must produce null", block.get(1));
                assertEquals(new BytesRef("third"), block.get(2));
                block.close();

                columnReader.close();
            }
        }
    }

    /**
     * Verifies that run coalescing handles a page whose docs span two blocks. With a tiny block
     * size (2 docs per block), docs [1, 2] land in different blocks: doc 1 is the last doc of
     * block 0, doc 2 is the first of block 1. The run coalescer must end the first run at block 0's
     * boundary and start a new run in block 1, each copied via a separate {@code arraycopy}.
     */
    public void testRunCoalescing_crossBlockBoundary() throws IOException {
        FlattenedDocValuesFormat tinyBlocks = new FlattenedDocValuesFormat(
            FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT,
            2, // 2 docs per block at most
            FlattenedDocValuesFormat.MIN_COMPRESS_BYTES_DEFAULT,
            FlattenedDocValuesFormat.MAX_BUFFERED_BYTES_DEFAULT
        );
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig().setCodec(new Elasticsearch93Lucene104Codec() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return KEYED_FIELD.equals(field) ? tinyBlocks : super.getDocValuesFormatForField(field);
                }
            });
            try (RandomIndexWriter writer = new RandomIndexWriter(random(), dir, iwc)) {
                addColumnarDoc(writer, KEY + "\0block0-doc0");
                addColumnarDoc(writer, KEY + "\0block0-doc1");
                addColumnarDoc(writer, KEY + "\0block1-doc0");
                addColumnarDoc(writer, KEY + "\0block1-doc1");

                try (IndexReader reader = openReader(writer)) {
                    LeafReaderContext leaf = reader.leaves().get(0);
                    BlockLoader.ColumnAtATimeReader columnReader = newColumnarBatchLoader(leaf);

                    // Docs [1, 2]: last doc of block 0 and first doc of block 1.
                    // Run of length 1 in block 0, then a fresh run of length 1 in block 1.
                    TestBlock block = (TestBlock) columnReader.read(TestBlock.factory(), TestBlock.docs(1, 2), 0, false);
                    assertEquals(2, block.size());
                    assertEquals(new BytesRef("block0-doc1"), block.get(0));
                    assertEquals(new BytesRef("block1-doc0"), block.get(1));
                    block.close();

                    columnReader.close();
                }
            }
        }
    }

    /** Creates a batch-capable columnar block loader reader for {@link #KEY} on the given leaf. */
    private static BlockLoader.ColumnAtATimeReader newColumnarBatchLoader(LeafReaderContext leaf) throws IOException {
        return new KeyedFlattenedDocValuesBlockLoader(
            KEYED_FIELD,
            KEY,
            true,  // usesBinaryDocValues
            true   // usesArrayOrderBinaryDocValues → columnar path with batch reader
        ).reader(new NoopCircuitBreaker("test"), leaf);
    }

    private static RandomIndexWriter newRandomIndexWriter(Directory dir, boolean binary) throws IOException {
        if (binary) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(TestUtil.alwaysDocValuesFormat(new ES819TSDBDocValuesFormat()));
            return new RandomIndexWriter(random(), dir, iwc);
        }
        return new RandomIndexWriter(random(), dir);
    }

    /**
     * Creates an index writer that routes {@code KEYED_FIELD} through {@link FlattenedDocValuesFormat}
     * while leaving all other fields (including the {@code .counts} companion numeric DV) on the default codec.
     * This mirrors the production dispatch in
     * {@link org.elasticsearch.index.codec.PerFieldFormatSupplier#getDocValuesFormatForField}.
     */
    private static RandomIndexWriter newColumnarIndexWriter(Directory dir) throws IOException {
        FlattenedDocValuesFormat flattenedFmt = new FlattenedDocValuesFormat();
        IndexWriterConfig iwc = newIndexWriterConfig().setCodec(new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return KEYED_FIELD.equals(field) ? flattenedFmt : super.getDocValuesFormatForField(field);
            }
        });
        return new RandomIndexWriter(random(), dir, iwc);
    }

    private static void addDoc(RandomIndexWriter writer, boolean binary, String... keyedValues) throws IOException {
        Document doc = new Document();
        if (binary) {
            var field = new MultiValuedBinaryDocValuesField.SeparateCount(
                KEYED_FIELD,
                MultiValuedBinaryDocValuesField.ValueOrdering.SORTED_UNIQUE
            );
            for (String kv : keyedValues) {
                field.add(new BytesRef(kv));
            }
            doc.add(field);
            doc.add(NumericDocValuesField.indexedField(KEYED_FIELD + ".counts", field.count()));
        } else {
            for (String kv : keyedValues) {
                doc.add(new SortedSetDocValuesField(KEYED_FIELD, new BytesRef(kv)));
            }
        }
        writer.addDocument(doc);
    }

    /**
     * Writes a document with {@link KeyedArrayOrderInlineNull}-encoded key-value pairs in the columnar
     * format. The {@code .counts} companion numeric DV field is also written so the document is complete,
     * but the columnar block loader never reads it (it uses the {@link ColumnarKeyedBinaryDocValues} path
     * which does not require a separate count field).
     */
    private static void addColumnarDoc(RandomIndexWriter writer, String... keyedValues) throws IOException {
        LuceneDocument doc = new LuceneDocument();
        for (String kv : keyedValues) {
            KeyedArrayOrderInlineNull.recordValue(doc, KEYED_FIELD, new BytesRef(kv));
        }
        writer.addDocument(doc);
    }

    private static IndexReader openReader(RandomIndexWriter writer) throws IOException {
        // forceMerge(1) keeps the docs in a single segment so canReuse is exercised across
        // pages of the same segment, which is the operator-level path the bug regresses.
        writer.forceMerge(1);
        return ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer.w), new ShardId("test", "_na_", 0));
    }
}
