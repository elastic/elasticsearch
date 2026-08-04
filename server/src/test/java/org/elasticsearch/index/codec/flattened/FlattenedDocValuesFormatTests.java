/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.flattened;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.fielddata.KeyLookupArrayOrderBinaryDocValues;
import org.elasticsearch.index.mapper.LuceneDocument;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
import org.elasticsearch.index.mapper.flattened.FlattenedFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Format-level tests for {@link FlattenedDocValuesFormat}: merge correctness,
 * empty segments, and iterator API invariants ({@code advance}, {@code advanceExact}).
 *
 * <p>The Lucene {@code BaseDocValuesFormatTestCase} cannot be used directly because
 * our format only handles binary DV (throwing {@link UnsupportedOperationException}
 * for numeric, sorted, and sorted-set DV types). These focused tests validate the
 * properties that matter for the {@code ._keyed} field use case.
 *
 * <p>Round-trip correctness is comprehensively covered by {@link FlattenedColumnarBinaryDuelTests}.
 */
public class FlattenedDocValuesFormatTests extends ESTestCase {

    private static final String KEYED_FIELD = "labels" + FlattenedFieldMapper.KEYED_FIELD_SUFFIX;

    // ---------------------------------------------------------------------------------
    // Merge tests
    // ---------------------------------------------------------------------------------

    /**
     * Two separate columnar segments merged into one: every binaryValue() after merge
     * must match the original blob.
     */
    public void testMergeTwoSegments() throws IOException {
        final int docsPerSegment = 30;
        final int numKeys = 5;
        final List<byte[]> blobs = new ArrayList<>();

        for (int d = 0; d < docsPerSegment * 2; d++) {
            blobs.add(generateBlob(numKeys, d));
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                // Write first segment.
                for (int d = 0; d < docsPerSegment; d++) {
                    writer.addDocument(docWithBlob(blobs.get(d)));
                }
                writer.commit();
                // Write second segment.
                for (int d = docsPerSegment; d < docsPerSegment * 2; d++) {
                    writer.addDocument(docWithBlob(blobs.get(d)));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("expected one merged segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues(KEYED_FIELD);
                assertNotNull(dv);

                // Merge order is not guaranteed to match insertion order, so compare as sorted sets.
                final List<BytesRef> expected = blobs.stream().map(BytesRef::new).sorted().collect(Collectors.toList());
                final List<BytesRef> actual = new ArrayList<>();
                for (int d = 0; d < docsPerSegment * 2; d++) {
                    assertTrue("doc " + d + " must have DV", dv.advanceExact(d));
                    actual.add(BytesRef.deepCopyOf(dv.binaryValue()));
                }
                actual.sort(BytesRef::compareTo);
                assertEquals("all blobs must survive the merge", expected, actual);
            }
        }
    }

    /**
     * Mixed-source merge: one segment written with the default Lucene binary DV format
     * (row layout) and one with our columnar format. After force-merge (which uses our
     * format for the merged segment), every binaryValue() must match the original.
     */
    public void testMixedSourceMerge() throws IOException {
        final int numDocs = 40;
        final int numKeys = 4;
        final List<byte[]> blobs = new ArrayList<>();
        for (int d = 0; d < numDocs; d++) {
            blobs.add(generateBlob(numKeys, d));
        }

        try (Directory dir = newDirectory()) {
            // Segment 1: default row format (Lucene binary DV)
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
                for (int d = 0; d < numDocs / 2; d++) {
                    writer.addDocument(docWithBlob(blobs.get(d)));
                }
            }

            // Segment 2: columnar format
            IndexWriterConfig colConfig = new IndexWriterConfig();
            colConfig.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, colConfig)) {
                for (int d = numDocs / 2; d < numDocs; d++) {
                    writer.addDocument(docWithBlob(blobs.get(d)));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                int docIdx = 0;
                for (final LeafReaderContext ctx : reader.leaves()) {
                    final BinaryDocValues dv = ctx.reader().getBinaryDocValues(KEYED_FIELD);
                    if (dv == null) continue;
                    for (int d = 0; d < ctx.reader().maxDoc(); d++) {
                        if (dv.advanceExact(d)) {
                            assertEquals("binaryValue mismatch at doc " + docIdx, new BytesRef(blobs.get(docIdx)), dv.binaryValue());
                            docIdx++;
                        }
                    }
                }
                assertEquals("all docs must be read back", numDocs, docIdx);
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // Empty / no-field segment
    // ---------------------------------------------------------------------------------

    /** A segment where NO document has the keyed field: DISI should be empty. */
    public void testEmptyField() throws IOException {
        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int d = 0; d < 10; d++) {
                    writer.addDocument(new Document()); // no DV field
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    assertNull("field absent → null BinaryDocValues", ctx.reader().getBinaryDocValues(KEYED_FIELD));
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // Iterator API invariants
    // ---------------------------------------------------------------------------------

    /**
     * After {@code advanceExact(target)}, {@code docID()} must equal {@code target}
     * regardless of whether the target document has a value.
     */
    public void testAdvanceExactSetsDocId() throws IOException {
        final int numDocs = 50;
        final float absentFraction = 0.4f;
        final List<byte[]> blobs = new ArrayList<>();
        final List<Integer> presentDocs = new ArrayList<>();

        for (int d = 0; d < numDocs; d++) {
            if (random().nextFloat() >= absentFraction) {
                blobs.add(generateBlob(3, d));
                presentDocs.add(d);
            } else {
                blobs.add(null);
            }
        }

        if (presentDocs.isEmpty()) {
            // All absent — nothing to test; regenerate one doc.
            blobs.set(0, generateBlob(3, 0));
            presentDocs.add(0);
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (byte[] blob : blobs) {
                    writer.addDocument(blob == null ? new Document() : docWithBlob(blob));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    final BinaryDocValues dv = ctx.reader().getBinaryDocValues(KEYED_FIELD);
                    if (dv == null) continue;

                    // For every doc, advanceExact must set docID() == target.
                    for (int d = 0; d < ctx.reader().maxDoc(); d++) {
                        dv.advanceExact(d);
                        assertEquals("docID() must equal target after advanceExact(" + d + ")", d, dv.docID());
                    }
                }
            }
        }
    }

    /**
     * {@code advance(target)} must return the first doc >= target that has a value,
     * and leave {@code docID()} at that doc.
     */
    public void testAdvanceSkipsAbsentDocs() throws IOException {
        // Create sparse field: docs 0,10,20,30,40 have the field; rest are absent.
        final int step = 10;
        final int maxDoc = 50;

        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (int d = 0; d < maxDoc; d++) {
                    if (d % step == 0) {
                        writer.addDocument(docWithBlob(generateBlob(2, d)));
                    } else {
                        writer.addDocument(new Document());
                    }
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    final BinaryDocValues dv = ctx.reader().getBinaryDocValues(KEYED_FIELD);
                    if (dv == null) continue;

                    // advance(1) should land at doc 10 (next present doc after 0).
                    // advance() requires target > docID(); initial docID() is -1, so 1 is valid.
                    int landed = dv.advance(1);
                    assertEquals("advance(1) should reach doc 10", 10, landed);
                    assertEquals("docID() after advance", 10, dv.docID());

                    // advance(11) from position 10 should reach 20.
                    landed = dv.advance(11);
                    assertEquals("advance(11) should reach doc 20", 20, landed);

                    // advance(25) should reach 30.
                    landed = dv.advance(25);
                    assertEquals("advance(25) should reach 30", 30, landed);
                }
            }
        }
    }

    /**
     * Dense field (all docs have a value): iteration via nextDoc() covers all docs.
     */
    public void testDenseNextDoc() throws IOException {
        final int numDocs = 60;
        final List<byte[]> blobs = new ArrayList<>();
        for (int d = 0; d < numDocs; d++) {
            blobs.add(generateBlob(4, d));
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (byte[] blob : blobs) {
                    writer.addDocument(docWithBlob(blob));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    final BinaryDocValues dv = ctx.reader().getBinaryDocValues(KEYED_FIELD);
                    assertNotNull(dv);
                    int count = 0;
                    for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                        assertEquals("sequential doc at index " + count, count, d);
                        assertEquals("binaryValue mismatch", new BytesRef(blobs.get(d)), dv.binaryValue());
                        count++;
                    }
                    assertEquals("all docs visited", numDocs, count);
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // KeyLookupArrayOrderBinaryDocValues
    // ---------------------------------------------------------------------------------

    /**
     * Verifies {@link KeyLookupArrayOrderBinaryDocValues}: absent key returns -1 ordinal and
     * every {@code advanceExact} returns false; present key returns sorted, deduped values
     * matching the full-blob scan.
     */
    public void testKeyLookupFastPath() throws IOException {
        final int numDocs = 40;
        final int numKeys = 6;
        // key0000..key0005; blobs have shuffled key order, duplicates, and null slots.
        final List<byte[]> blobs = new ArrayList<>();
        for (int d = 0; d < numDocs; d++) {
            final LuceneDocument doc = new LuceneDocument();
            for (int ki = 0; ki < numKeys; ki++) {
                final String kv = String.format(java.util.Locale.ROOT, "key%04d", ki) + "\0val-" + d + "-" + ki;
                // Add two slots for key0 to exercise sort+dedup.
                MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull.recordValue(doc, KEYED_FIELD, new BytesRef(kv));
                if (ki == 0) {
                    // Duplicate value: same key, same value → should be deduped to 1.
                    MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull.recordValue(doc, KEYED_FIELD, new BytesRef(kv));
                }
            }
            blobs.add(extractBlob(doc));
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig config = new IndexWriterConfig();
            config.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                for (byte[] blob : blobs) {
                    writer.addDocument(docWithBlob(blob));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext ctx : reader.leaves()) {
                    final BinaryDocValues rawDv = ctx.reader().getBinaryDocValues(KEYED_FIELD);
                    assertNotNull(rawDv);
                    assertTrue("DV must be ColumnarKeyedBinaryDocValues", rawDv instanceof ColumnarKeyedBinaryDocValues);
                    final ColumnarKeyedBinaryDocValues columnar = (ColumnarKeyedBinaryDocValues) rawDv;

                    // A key not in the segment → ordinal == -1, every advanceExact returns false.
                    final KeyLookupArrayOrderBinaryDocValues absentLookup = new KeyLookupArrayOrderBinaryDocValues(
                        columnar,
                        new BytesRef("no_such_key")
                    );
                    for (int d = 0; d < numDocs; d++) {
                        assertFalse("absent key must return false at doc " + d, absentLookup.advanceExact(d));
                    }

                    // key0000 has two identical values per doc → after dedup exactly 1 value remains.
                    final KeyLookupArrayOrderBinaryDocValues key0Lookup = new KeyLookupArrayOrderBinaryDocValues(
                        columnar,
                        new BytesRef("key0000")
                    );
                    for (int d = 0; d < numDocs; d++) {
                        assertTrue("key0 present at doc " + d, key0Lookup.advanceExact(d));
                        assertEquals("dedup must collapse identical values to 1", 1, key0Lookup.docValueCount());
                        final BytesRef val = key0Lookup.nextValue();
                        assertEquals(
                            "key0 value mismatch at doc " + d,
                            new BytesRef(String.format(java.util.Locale.ROOT, "val-%d-0", d)),
                            val
                        );
                    }

                    // key0002: single value per doc.
                    final KeyLookupArrayOrderBinaryDocValues key2Lookup = new KeyLookupArrayOrderBinaryDocValues(
                        columnar,
                        new BytesRef("key0002")
                    );
                    for (int d = 0; d < numDocs; d++) {
                        assertTrue("key2 present at doc " + d, key2Lookup.advanceExact(d));
                        assertEquals("key2 single value count", 1, key2Lookup.docValueCount());
                        assertEquals(
                            "key2 value at doc " + d,
                            new BytesRef(String.format(java.util.Locale.ROOT, "val-%d-2", d)),
                            key2Lookup.nextValue()
                        );
                    }
                }
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // Column-wise merge tests
    // ---------------------------------------------------------------------------------

    /**
     * Regression test for the block-flush doc-atomicity invariant in {@link FieldBlockWriter}:
     * when a document's slots exceed {@code targetBlockBytes} across two or more slots, the
     * old code split the document across two blocks. The reader's {@code findBlockFor(docId)}
     * returns the <em>last</em> block whose {@code firstDocId <= docId}, so the slots left in
     * the earlier block became unreachable — silent data loss.
     *
     * <p>This test uses a tiny {@code targetBlockBytes} (32) so that even a single document's
     * three 30-byte slots crosses the threshold.
     */
    public void testDocAtomicityBugFix() throws IOException {
        // 32-byte block target: a doc with 3 × 30-byte values = 90+ bytes → guaranteed split
        // in the old code; must be written and read as a single doc.
        final FlattenedDocValuesFormat fmt = new FlattenedDocValuesFormat(32, 1024, 1, Integer.MAX_VALUE);
        final String key = "key0000";
        final int numValues = 5;
        final byte[][] values = new byte[numValues][];
        for (int v = 0; v < numValues; v++) {
            values[v] = new byte[30];
            Arrays.fill(values[v], (byte) ('a' + v));
        }

        final LuceneDocument luceneDoc = new LuceneDocument();
        for (byte[] val : values) {
            final String kv = key + "\0" + new String(val, StandardCharsets.UTF_8);
            MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull.recordValue(luceneDoc, KEYED_FIELD, new BytesRef(kv));
        }
        final byte[] blob = extractBlob(luceneDoc);

        try (Directory dir = newDirectory()) {
            IndexWriterConfig cfg = new IndexWriterConfig();
            cfg.setCodec(TestUtil.alwaysDocValuesFormat(fmt));
            try (IndexWriter writer = new IndexWriter(dir, cfg)) {
                writer.addDocument(docWithBlob(blob));
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues(KEYED_FIELD);
                assertNotNull(dv);
                assertTrue("doc 0 must have DV", dv.advanceExact(0));
                // Parse all slots from the blob and assert they all round-trip.
                final BytesRef result = dv.binaryValue();
                final Map<String, List<String>> parsed = parseBlob(result);
                final List<String> got = parsed.get(key);
                assertNotNull("key must be present", got);
                assertEquals("all slots must survive", numValues, got.size());
                for (int v = 0; v < numValues; v++) {
                    assertEquals("value " + v, new String(values[v], StandardCharsets.UTF_8), got.get(v));
                }
            }
        }
    }

    /**
     * Merges two columnar segments where the key sets partially overlap: segment A has
     * {@code key0000..key0003}, segment B has {@code key0002..key0005}. The k-way merge of
     * the key dictionaries must produce a correct merged lex-ordered set
     * ({@code key0000..key0005}), and every key that appears in only one source segment
     * must still be present in the merged output.
     *
     * <p>The column-wise merge path is exercised: both producers are
     * {@link FlattenedDocValuesProducer} instances (same columnar format).
     */
    public void testColumnWiseMergePartialKeyOverlap() throws IOException {
        final int docsPerSegment = 25;
        // Keys 0-3 in segment A, keys 2-5 in segment B → merged keys 0-5.
        final List<byte[]> blobsA = new ArrayList<>();
        final List<byte[]> blobsB = new ArrayList<>();
        for (int d = 0; d < docsPerSegment; d++) {
            blobsA.add(generateBlobForKeys(d, 0, 4)); // keys 0000..0003
            blobsB.add(generateBlobForKeys(d + docsPerSegment, 2, 6)); // keys 0002..0005
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig cfg = new IndexWriterConfig();
            cfg.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, cfg)) {
                for (byte[] blob : blobsA) {
                    writer.addDocument(docWithBlob(blob));
                }
                writer.commit();
                for (byte[] blob : blobsB) {
                    writer.addDocument(docWithBlob(blob));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("expected one merged segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues(KEYED_FIELD);
                assertNotNull(dv);

                // Build expected set (all blobs from both segments, sorted).
                final List<BytesRef> expected = new ArrayList<>();
                for (byte[] b : blobsA) {
                    expected.add(new BytesRef(b));
                }
                for (byte[] b : blobsB) {
                    expected.add(new BytesRef(b));
                }
                expected.sort(BytesRef::compareTo);

                final List<BytesRef> actual = new ArrayList<>();
                for (int d = 0; d < docsPerSegment * 2; d++) {
                    assertTrue("doc " + d + " must have DV", dv.advanceExact(d));
                    actual.add(BytesRef.deepCopyOf(dv.binaryValue()));
                }
                actual.sort(BytesRef::compareTo);
                assertEquals("all blobs (both key sets) must survive", expected, actual);
            }
        }
    }

    /**
     * Merges two segments where some documents are deleted. Deleted documents must not appear
     * in the merged output.
     */
    public void testMergeWithDeletes() throws IOException {
        final int numDocs = 30;
        final int numKeys = 3;
        final List<byte[]> blobs = new ArrayList<>();
        for (int d = 0; d < numDocs; d++) {
            blobs.add(generateBlob(numKeys, d));
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig cfg = new IndexWriterConfig();
            cfg.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, cfg)) {
                // Write all docs with an "id" term field for deletion.
                for (int d = 0; d < numDocs; d++) {
                    final Document doc = docWithBlob(blobs.get(d));
                    doc.add(new org.apache.lucene.document.StringField("id", "doc" + d, org.apache.lucene.document.Field.Store.NO));
                    writer.addDocument(doc);
                }
                writer.commit();
                // Delete every other doc.
                for (int d = 0; d < numDocs; d += 2) {
                    writer.deleteDocuments(new Term("id", "doc" + d));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("expected one merged segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals("half the docs should survive", numDocs / 2, leaf.numDocs());

                final BinaryDocValues dv = leaf.getBinaryDocValues(KEYED_FIELD);
                assertNotNull(dv);
                final List<BytesRef> actual = new ArrayList<>();
                for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                    actual.add(BytesRef.deepCopyOf(dv.binaryValue()));
                }
                assertEquals("surviving docs count", numDocs / 2, actual.size());

                // Surviving docs are odd-indexed (1,3,5,...).
                final List<BytesRef> expected = new ArrayList<>();
                for (int d = 1; d < numDocs; d += 2) {
                    expected.add(new BytesRef(blobs.get(d)));
                }
                expected.sort(BytesRef::compareTo);
                actual.sort(BytesRef::compareTo);
                assertEquals("surviving blobs must match", expected, actual);
            }
        }
    }

    /**
     * One segment has the keyed field; the other does not. After merge the field must be
     * present for the documents from the first segment and absent for the others.
     */
    public void testMergeSegmentMissingField() throws IOException {
        final int numWithField = 20;
        final int numWithout = 15;
        final int numKeys = 4;
        final List<byte[]> blobs = new ArrayList<>();
        for (int d = 0; d < numWithField; d++) {
            blobs.add(generateBlob(numKeys, d));
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig cfg = new IndexWriterConfig();
            cfg.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, cfg)) {
                // Segment 1: all docs have the field.
                for (int d = 0; d < numWithField; d++) {
                    writer.addDocument(docWithBlob(blobs.get(d)));
                }
                writer.commit();
                // Segment 2: no doc has the field.
                for (int d = 0; d < numWithout; d++) {
                    writer.addDocument(new Document());
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("expected one merged segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                assertEquals("total doc count", numWithField + numWithout, leaf.numDocs());

                final BinaryDocValues dv = leaf.getBinaryDocValues(KEYED_FIELD);
                assertNotNull("field must be present after merge", dv);

                int countWithField = 0;
                for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                    countWithField++;
                }
                assertEquals("only the docs from segment 1 should have the field", numWithField, countWithField);
            }
        }
    }

    /**
     * Semantic duel: merges with the column-wise path and verifies the output is semantically
     * identical to what a direct read of the source segments would yield. This validates that
     * the column-wise merge does not drop, duplicate, or corrupt slots.
     */
    public void testColumnWiseMergeSemanticCorrectness() throws IOException {
        final int numDocs = 50;
        final int numKeys = 6;
        final List<byte[]> blobs = new ArrayList<>();
        for (int d = 0; d < numDocs; d++) {
            blobs.add(generateBlob(numKeys, d));
        }

        // Build the reference map: key → sorted list of values (per document).
        final List<Map<String, List<String>>> reference = new ArrayList<>();
        for (byte[] blob : blobs) {
            reference.add(parseBlob(new BytesRef(blob)));
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig cfg = new IndexWriterConfig();
            cfg.setCodec(TestUtil.alwaysDocValuesFormat(new FlattenedDocValuesFormat()));
            try (IndexWriter writer = new IndexWriter(dir, cfg)) {
                // Two segments.
                for (int d = 0; d < numDocs / 2; d++) {
                    writer.addDocument(docWithBlob(blobs.get(d)));
                }
                writer.commit();
                for (int d = numDocs / 2; d < numDocs; d++) {
                    writer.addDocument(docWithBlob(blobs.get(d)));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("expected one merged segment", 1, reader.leaves().size());
                final LeafReader leaf = reader.leaves().get(0).reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues(KEYED_FIELD);
                assertNotNull(dv);

                // Collect merged results.
                final List<Map<String, List<String>>> merged = new ArrayList<>();
                for (int d = dv.nextDoc(); d != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; d = dv.nextDoc()) {
                    merged.add(parseBlob(dv.binaryValue()));
                }

                assertEquals("doc count after merge", numDocs, merged.size());
                // Sort both by content for comparison (merge order may differ from insertion order).
                final List<Map<String, List<String>>> sortedRef = reference.stream()
                    .sorted((a, b) -> a.toString().compareTo(b.toString()))
                    .collect(Collectors.toList());
                final List<Map<String, List<String>>> sortedMerged = merged.stream()
                    .sorted((a, b) -> a.toString().compareTo(b.toString()))
                    .collect(Collectors.toList());
                assertEquals("merged blobs must be semantically identical to originals", sortedRef, sortedMerged);
            }
        }
    }

    /**
     * Regression test for the external-sort endianness bug in {@link SortedSlotAccumulator}:
     * when accumulated slot records exceed {@code maxBufferBytes}, they are spilled to temp
     * run files using Lucene's {@link org.apache.lucene.store.IndexOutput#writeBytes}, then
     * read back with {@link org.apache.lucene.store.IndexInput#readInt}. The record headers
     * must be little-endian (matching Lucene's convention) to survive the round-trip.
     *
     * <p>This test forces the spill path by using a tiny {@code maxBufferedBytes} (1024),
     * then verifies that every document's values round-trip correctly through the columnar
     * format. Without the fix, this fails with {@code ArrayIndexOutOfBoundsException} during
     * the flush (the same crash observed in production with shard sizes of ~860 MiB).
     */
    public void testExternalSortSpillRoundTrip() throws IOException {
        // Tiny maxBufferedBytes forces the accumulator to spill to run files.
        final FlattenedDocValuesFormat fmt = new FlattenedDocValuesFormat(
            FlattenedDocValuesFormat.TARGET_BLOCK_BYTES_DEFAULT,
            FlattenedDocValuesFormat.MAX_DOCS_PER_BLOCK_DEFAULT,
            FlattenedDocValuesFormat.MIN_COMPRESS_BYTES_DEFAULT,
            /* maxBufferedBytes */ 1024
        );
        final int numDocs = 200;
        final int numKeys = 8;

        // Build reference: docIndex → key → value.
        final List<Map<String, String>> reference = new ArrayList<>();
        final List<byte[]> blobs = new ArrayList<>();
        for (int d = 0; d < numDocs; d++) {
            final Map<String, String> expected = new TreeMap<>();
            final byte[] blob = generateBlobForKeys(d, 0, numKeys);
            blobs.add(blob);
            for (int ki = 0; ki < numKeys; ki++) {
                expected.put(String.format(java.util.Locale.ROOT, "key%04d", ki), "val-" + d + "-" + ki);
            }
            reference.add(expected);
        }

        try (Directory dir = newDirectory()) {
            IndexWriterConfig cfg = new IndexWriterConfig();
            cfg.setCodec(TestUtil.alwaysDocValuesFormat(fmt));
            try (IndexWriter writer = new IndexWriter(dir, cfg)) {
                for (byte[] blob : blobs) {
                    writer.addDocument(docWithBlob(blob));
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReaderContext ctx = reader.leaves().get(0);
                final LeafReader leaf = ctx.reader();
                final BinaryDocValues dv = leaf.getBinaryDocValues(KEYED_FIELD);
                assertNotNull("field must have DV", dv);

                int docIdx = 0;
                for (int doc = dv.nextDoc(); doc != org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS; doc = dv.nextDoc()) {
                    final Map<String, List<String>> got = parseBlob(dv.binaryValue());
                    final Map<String, String> exp = reference.get(docIdx++);
                    for (final Map.Entry<String, String> entry : exp.entrySet()) {
                        final List<String> vals = got.get(entry.getKey());
                        assertNotNull("doc " + doc + " key " + entry.getKey() + " missing", vals);
                        assertEquals("doc " + doc + " key " + entry.getKey() + " value count", 1, vals.size());
                        assertEquals("doc " + doc + " key " + entry.getKey() + " value", entry.getValue(), vals.get(0));
                    }
                }
                assertEquals("all docs must be present", numDocs, docIdx);
            }
        }
    }

    // ---------------------------------------------------------------------------------
    // Helpers
    // ---------------------------------------------------------------------------------

    /**
     * Parses a flattened keyed blob ({@code [vint prefix][key bytes]\0[value bytes]} framing)
     * into a multimap of key → sorted list of values. Null slots are represented by an empty
     * string; absent values are distinct. Used for semantic equality checks.
     */
    private static Map<String, List<String>> parseBlob(BytesRef blob) {
        final Map<String, List<String>> map = new TreeMap<>();
        int pos = blob.offset;
        final int end = blob.offset + blob.length;
        while (pos < end) {
            int prefix = 0, shift = 0;
            while (true) {
                final int b = blob.bytes[pos++] & 0xFF;
                prefix |= (b & 0x7F) << shift;
                if ((b & 0x80) == 0) break;
                shift += 7;
            }
            final boolean isNull = (prefix == 0);
            final int valueLen = isNull ? 0 : (prefix - 1);

            // Read key bytes up to \0.
            final int keyStart = pos;
            int sep = keyStart;
            while (sep < end && blob.bytes[sep] != 0) {
                sep++;
            }
            final String key = new String(blob.bytes, keyStart, sep - keyStart, StandardCharsets.UTF_8);
            pos = sep + 1; // skip \0

            final String value = isNull ? "" : new String(blob.bytes, pos, valueLen, StandardCharsets.UTF_8);
            pos += valueLen;

            map.computeIfAbsent(key, k -> new ArrayList<>()).add(value);
        }
        // Sort value lists for deterministic comparison.
        map.values().forEach(java.util.Collections::sort);
        return map;
    }

    private static byte[] generateBlob(int numKeys, int docSeed) {
        return generateBlobForKeys(docSeed, 0, numKeys);
    }

    /**
     * Generates a blob for keys {@code key<keyStart>} (inclusive) through {@code key<keyEnd>}
     * (exclusive), each with a value that encodes both the doc seed and the key index so tests
     * can assert exact value round-trips.
     */
    private static byte[] generateBlobForKeys(int docSeed, int keyStart, int keyEnd) {
        final LuceneDocument doc = new LuceneDocument();
        for (int ki = keyStart; ki < keyEnd; ki++) {
            final String keyedValue = String.format(java.util.Locale.ROOT, "key%04d", ki) + "\0val-" + docSeed + "-" + ki;
            MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull.recordValue(doc, KEYED_FIELD, new BytesRef(keyedValue));
        }
        return extractBlob(doc);
    }

    private static byte[] extractBlob(LuceneDocument doc) {
        final MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull field =
            (MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull) doc.getField(KEYED_FIELD);
        if (field == null) {
            return null;
        }
        final BytesRef ref = field.binaryValue();
        return Arrays.copyOfRange(ref.bytes, ref.offset, ref.offset + ref.length);
    }

    private static Document docWithBlob(byte[] blob) {
        final Document doc = new Document();
        doc.add(new BinaryDocValuesField(KEYED_FIELD, new BytesRef(blob)));
        return doc;
    }
}
