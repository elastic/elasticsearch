/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LogDocMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.InfoStream;
import org.elasticsearch.columnar.numeric.NumericPipeline;
import org.elasticsearch.columnar.string.ColumnarStringBinaryDocValues;
import org.elasticsearch.columnar.string.DictionaryPolicy;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnOptions;
import org.elasticsearch.columnar.string.StringColumnOptionsSelector;
import org.elasticsearch.columnar.string.StringColumnWriter;
import org.elasticsearch.columnar.string.SummaryPolicy;
import org.elasticsearch.columnar.substrate.ChunkBounds;
import org.elasticsearch.columnar.substrate.ChunkCodec;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodecForField;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * A merge of plain string columns copies the chunks of every run of a segment's documents that lands in the merged
 * segment in its own order ({@link MergeStretches}), and the merged column reads back as the documents were written
 * either way. Chunks are kept small, so a run of a few dozen documents already holds whole ones.
 */
public class PlainRunMergeTests extends ESTestCase {

    private static final String FIELD = "keyword";
    private static final String ID = "id";
    private static final String SORT = "sort";

    /** Counts the runs the merge reports copying, and the most slots any one of them covered. */
    private static final class CopyCount extends InfoStream {
        volatile int copies;
        volatile long maxSlots;

        @Override
        public void message(String component, String message) {
            // "copied [slots] plain slots, ..."
            final long slots = Long.parseLong(message.substring(message.indexOf('[') + 1, message.indexOf(']')));
            maxSlots = Math.max(maxSlots, slots);
            copies++;
        }

        @Override
        public boolean isEnabled(String component) {
            return StringColumnWriter.INFO_STREAM_COMPONENT.equals(component);
        }

        @Override
        public void close() {}
    }

    /** Without an index sort a merge appends each segment whole, so its runs are copied, deletions or not. */
    public void testSegmentsAppendedWholeAreCopied() throws IOException {
        final boolean deleting = randomBoolean();
        final CopyCount copies = mergeAndCheck(
            between(1000, 3000),
            between(2, 6),
            null,
            deleting ? 200 : 0,
            (segment, doc, numDocs) -> doc
        );
        assertThat(copies.copies, greaterThan(0));
    }

    /** A format told not to copy chunks writes every one of them again, and the column reads back the same. */
    public void testCopyingTurnedOffCopiesNothing() throws IOException {
        final CopyCount copies = mergeAndCheck(between(1000, 3000), between(2, 6), null, 0, (segment, doc, numDocs) -> doc, false);
        assertEquals(0, copies.copies);
    }

    /**
     * Under an index sort that keeps each segment's documents together — each covers a range of the sort key of its
     * own, added in the reverse of key order — the segments are reordered but every one lands whole.
     */
    public void testAnIndexSortOverDisjointRangesIsCopied() throws IOException {
        final CopyCount copies = mergeAndCheck(between(1000, 3000), between(2, 6), sortOnKey(), 0, (segment, doc, numDocs) -> -doc);
        assertThat(copies.copies, greaterThan(0));
    }

    /**
     * Under an index sort that deals the segments' documents out in turn, no two of a segment's land together, so no
     * run spans more than one document. A lone document is still a run of its own, and a segment's last chunk, cut
     * short where its bytes ran out, may lie entirely inside its last document's bytes and be copied; nothing larger is.
     */
    public void testAnInterleavingIndexSortCopiesNoMoreThanADocument() throws IOException {
        final int numSegments = between(2, 6);
        final CopyCount copies = mergeAndCheck(
            between(1000, 3000),
            numSegments,
            sortOnKey(),
            0,
            (segment, doc, numDocs) -> (long) (doc - segment * (numDocs / numSegments)) * numSegments + segment
        );
        assertThat("copies of a segment's last chunk", copies.copies, lessThanOrEqualTo(numSegments));
        assertThat("slots a document holds at most", copies.maxSlots, lessThanOrEqualTo(3L));
    }

    private static Sort sortOnKey() {
        return new Sort(new SortField(SORT, SortField.Type.LONG));
    }

    /** The sort key document {@code doc} of {@code segment} is given. */
    private interface SortKey {
        long of(int segment, int doc, int numDocs);
    }

    /**
     * Indexes {@code numDocs} documents over {@code numSegments} segments, deletes about one in {@code deleteEvery}
     * (none at zero), force-merges, checks every surviving document reads back, and answers what the merge
     * copied.
     */
    private CopyCount mergeAndCheck(int approximateNumDocs, int numSegments, Sort sort, int deleteEvery, SortKey sortKey)
        throws IOException {
        return mergeAndCheck(approximateNumDocs, numSegments, sort, deleteEvery, sortKey, true);
    }

    private CopyCount mergeAndCheck(
        int approximateNumDocs,
        int numSegments,
        Sort sort,
        int deleteEvery,
        SortKey sortKey,
        boolean copyChunksOnMerge
    ) throws IOException {
        // Every segment the same size, so no segment has documents left over that the sort key does not interleave.
        final int perSegment = approximateNumDocs / numSegments;
        final int numDocs = perSegment * numSegments;
        final String[][] values = randomValues(numDocs);
        final boolean[] deleted = new boolean[numDocs];
        final CopyCount copies = new CopyCount();
        final FieldType type = columnarBinaryFieldType();
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodecForField(FIELD, format(copyChunksOnMerge)))
                .setMergePolicy(new LogDocMergePolicy())
                .setInfoStream(copies);
            if (sort != null) {
                iwc.setIndexSort(sort);
            }
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < numDocs; d++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(ID, d));
                    doc.add(new StringField(ID + "_term", Integer.toString(d), Field.Store.NO));
                    doc.add(new NumericDocValuesField(SORT, sortKey.of(d / perSegment, d, numDocs)));
                    doc.add(new Field(FIELD, encode(values[d]), type));
                    writer.addDocument(doc);
                    if ((d + 1) % perSegment == 0) {
                        writer.commit();
                    }
                }
                if (deleteEvery > 0) {
                    for (int d = 0; d < numDocs; d++) {
                        if (random().nextInt(deleteEvery) == 0) {
                            writer.deleteDocuments(new Term(ID + "_term", Integer.toString(d)));
                            deleted[d] = true;
                        }
                    }
                }
                // Only the merge counts: the flushes above write their columns value by value.
                copies.copies = 0;
                copies.maxSlots = 0;
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("force-merged to one segment", 1, reader.leaves().size());
                assertMerged(reader.leaves().get(0).reader(), values, deleted);
            }
        }
        return copies;
    }

    /** Every surviving document's slots, found by the id each one carries, since a sort reorders them. */
    private static void assertMerged(LeafReader leaf, String[][] values, boolean[] deleted) throws IOException {
        final BinaryDocValues column = leaf.getBinaryDocValues(FIELD);
        assertTrue("expected a columnar column, got " + column, column instanceof ColumnarStringBinaryDocValues);
        assertFalse("expected a plain column", ((ColumnarStringBinaryDocValues) column).reader().hasDictionary());
        final NumericDocValues ids = leaf.getNumericDocValues(ID);
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        int seen = 0;
        for (int doc = column.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = column.nextDoc()) {
            assertTrue(ids.advanceExact(doc));
            final int id = (int) ids.longValue();
            assertFalse("document " + id + " was deleted", deleted[id]);
            final List<String> slots = new ArrayList<>();
            for (int slot = decoder.reset(column.binaryValue()); slot > 0; slot--) {
                final BytesRef value = decoder.next();
                slots.add(value == null ? null : value.utf8ToString());
            }
            assertEquals("document " + id, Arrays.asList(values[id]), slots);
            seen++;
        }
        int surviving = 0;
        for (boolean d : deleted) {
            if (d == false) {
                surviving++;
            }
        }
        assertEquals("documents with a value", surviving, seen);
    }

    /** A plain column in small chunks: no dictionary or summary, so a merge has nothing to take one from either. */
    private static ColumNARDocValuesFormat format(boolean copyChunksOnMerge) {
        final StringColumnOptions.Sizes defaults = StringColumnOptions.DEFAULT_SIZES;
        final StringColumnOptions.Sizes sizes = new StringColumnOptions.Sizes(
            defaults.valuesPerBlock(),
            ChunkBounds.ofBytes(randomFrom(64, 256, 1024)),
            defaults.escapeChunks(),
            defaults.packedOrdinalBlockSize(),
            defaults.compressedOrdinalBlockSize(),
            defaults.escapeRankBlockSize(),
            defaults.slotCountsBlockSize(),
            defaults.lengthBlockSize()
        );
        final StringColumnOptions options = new StringColumnOptions(
            DictionaryPolicy.NONE,
            SummaryPolicy.NONE,
            randomFrom(ChunkCodec.IDENTITY, ChunkCodec.ZSTD),
            sizes
        );
        return new ColumNARDocValuesFormat(
            (f, t) -> NumericPipeline::defaultPipeline,
            f -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(options),
            copyChunksOnMerge
        );
    }

    /** One to three slots a document, often repeating the slot before and sometimes null. */
    private static String[][] randomValues(int numDocs) {
        final String[] pool = new String[between(2, 10)];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = randomAlphaOfLength(between(1, 20));
        }
        final String[][] values = new String[numDocs][];
        String previous = pool[0];
        for (int d = 0; d < numDocs; d++) {
            final String[] slots = new String[between(1, 3)];
            for (int s = 0; s < slots.length; s++) {
                if (random().nextInt(10) == 0) {
                    continue;
                }
                previous = random().nextInt(3) == 0 ? previous : randomFrom(pool);
                slots[s] = previous;
            }
            values[d] = slots;
        }
        return values;
    }

    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }
}
