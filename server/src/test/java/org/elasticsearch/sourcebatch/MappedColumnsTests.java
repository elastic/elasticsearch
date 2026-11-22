/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.sourcebatch;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.column.BinaryColumn;
import org.apache.lucene.document.column.Column;
import org.apache.lucene.document.column.ColumnBatch;
import org.apache.lucene.document.column.LongColumn;
import org.apache.lucene.document.column.LongTupleCursor;
import org.apache.lucene.document.column.ObjectTupleCursor;
import org.apache.lucene.document.column.TokenStreamColumn;
import org.apache.lucene.document.column.VectorColumn;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.analysis.CannedTokenStream;
import org.apache.lucene.tests.analysis.Token;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.escf.EscfColumn;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class MappedColumnsTests extends ESTestCase {

    private static final FieldType LONG_FIELD_TYPE;
    private static final FieldType BINARY_FIELD_TYPE;

    static {
        LONG_FIELD_TYPE = new FieldType();
        LONG_FIELD_TYPE.setDocValuesType(DocValuesType.NUMERIC);
        LONG_FIELD_TYPE.freeze();

        BINARY_FIELD_TYPE = new FieldType();
        BINARY_FIELD_TYPE.setDocValuesType(DocValuesType.BINARY);
        BINARY_FIELD_TYPE.freeze();
    }

    private static byte[] longBytes(long... values) {
        byte[] buf = new byte[values.length * 8];
        for (int i = 0; i < values.length; i++) {
            ByteUtils.writeLongLE(values[i], buf, i * 8);
        }
        return buf;
    }

    public void testSliceOfSliceLongColumn() {
        // 6 docs with values 10..60; double-slice down to original rows 3 and 4 (values 40, 50).
        BytesRef data = new BytesRef(longBytes(10L, 20L, 30L, 40L, 50L, 60L));
        BytesRef seqNos = new BytesRef(new byte[6 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            6,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.longColumn(data, "val", LONG_FIELD_TYPE, LongColumn.NumericKind.LONG))
        );

        // [2, 6) → rows 2..5; then [1, 3) of that → original rows 3..4
        MappedColumns sliced = mc.slice(2, 6).slice(1, 3);
        assertEquals(2, sliced.docCount());

        MappedColumns.RowCursor cursor = sliced.rowCursor();
        cursor.advance();
        assertEquals(40L, cursor.fields().get(0).numericValue().longValue());
        cursor.advance();
        assertEquals(50L, cursor.fields().get(0).numericValue().longValue());
    }

    public void testSliceOfSliceBinaryColumn() {
        // 6 docs with values "a".."f"; double-slice down to original rows 2 and 3 ("c", "d").
        BytesRef[] values = {
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("c"),
            new BytesRef("d"),
            new BytesRef("e"),
            new BytesRef("f") };
        BytesRef seqNos = new BytesRef(new byte[6 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            6,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        // [1, 5) → rows 1..4; then [1, 3) of that → original rows 2..3
        MappedColumns sliced = mc.slice(1, 5).slice(1, 3);
        assertEquals(2, sliced.docCount());

        MappedColumns.RowCursor cursor = sliced.rowCursor();
        cursor.advance();
        assertEquals(new BytesRef("c"), cursor.fields().get(0).binaryValue());
        cursor.advance();
        assertEquals(new BytesRef("d"), cursor.fields().get(0).binaryValue());
    }

    public void testRowCursorSkipsExcludedColumnsAndKeepsTheOthersAligned() {
        BytesRef seqNos = new BytesRef(new byte[3 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[3 * 8]);
        BytesRef versions = new BytesRef(new byte[3 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            3,
            seqNos,
            primaryTerms,
            versions,
            List.of(
                MappedColumns.longColumn(new BytesRef(longBytes(1L, 2L, 3L)), "kept_long", LONG_FIELD_TYPE, LongColumn.NumericKind.LONG),
                MappedColumns.binaryColumn(
                    new BytesRef[] { new BytesRef("x"), new BytesRef("y"), new BytesRef("z") },
                    "dropped_binary",
                    BINARY_FIELD_TYPE
                ),
                MappedColumns.longColumn(
                    new BytesRef(longBytes(10L, 20L, 30L)),
                    "kept_long_2",
                    LONG_FIELD_TYPE,
                    LongColumn.NumericKind.LONG
                )
            )
        );

        MappedColumns.RowCursor cursor = mc.rowCursor(column -> column.toLuceneColumn().name().equals("dropped_binary") == false);
        for (int doc = 0; doc < 3; doc++) {
            cursor.advance();
            List<IndexableField> fields = cursor.fields();
            assertEquals("row " + doc, 2, fields.size());
            assertEquals("kept_long", fields.get(0).name());
            assertEquals(doc + 1L, fields.get(0).numericValue().longValue());
            assertEquals("kept_long_2", fields.get(1).name());
            assertEquals((doc + 1L) * 10, fields.get(1).numericValue().longValue());
        }

        // Without a predicate nothing is left out.
        MappedColumns.RowCursor all = mc.rowCursor();
        all.advance();
        assertEquals(3, all.fields().size());
    }

    // -------------------------------------------------------------------------
    // withFilter — WindowedBinaryColumn
    // -------------------------------------------------------------------------

    private static Map<Integer, BytesRef> drainBinaryTuples(MappedColumns mc) {
        Map<Integer, BytesRef> result = new LinkedHashMap<>();
        ObjectTupleCursor<BytesRef> cursor = ((BinaryColumn) mc.toColumnBatch().columns().iterator().next()).tuples();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.put(doc, BytesRef.deepCopyOf(cursor.value()));
        }
        return result;
    }

    private static Map<Integer, BytesRef> drainBinaryRowCursor(MappedColumns mc) {
        Map<Integer, BytesRef> result = new LinkedHashMap<>();
        MappedColumns.RowCursor cursor = mc.rowCursor();
        int doc = 0;
        while (doc < mc.docCount()) {
            cursor.advance();
            List<IndexableField> fields = cursor.fields();
            if (fields.isEmpty() == false) {
                result.put(doc, BytesRef.deepCopyOf(fields.get(0).binaryValue()));
            }
            doc++;
        }
        return result;
    }

    private static Map<Integer, Long> drainLongTuples(MappedColumns mc) {
        Map<Integer, Long> result = new LinkedHashMap<>();
        LongTupleCursor cursor = ((LongColumn) mc.toColumnBatch().columns().iterator().next()).tuples();
        int doc;
        while ((doc = cursor.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
            result.put(doc, cursor.longValue());
        }
        return result;
    }

    public void testWithFilterBinaryColumnFiltersCorrectly() {
        // 4 docs "a".."d"; filter passes docs 1 and 3
        BytesRef[] values = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") };
        BytesRef seqNos = new BytesRef(new byte[4 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[4 * 8]);
        BytesRef versions = new BytesRef(new byte[4 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            4,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(4);
        filter.set(1);
        filter.set(3);
        MappedColumns filtered = mc.withFilter(filter);

        // The batch for Lucene holds the two remaining docs only, renumbered; the row cursor keeps the numbering of the window.
        assertEquals(4, filtered.docCount());
        assertEquals(2, filtered.toColumnBatch().numDocs());
        assertEquals(Map.of(0, new BytesRef("b"), 1, new BytesRef("d")), drainBinaryTuples(filtered));
        assertEquals(Map.of(1, new BytesRef("b"), 3, new BytesRef("d")), drainBinaryRowCursor(filtered));
    }

    public void testWithFilterBinaryColumnForcesSparse() {
        // All docs present with all-set filter — should still be SPARSE (filter non-null → SPARSE).
        BytesRef[] values = { new BytesRef("x"), new BytesRef("y") };
        BytesRef seqNos = new BytesRef(new byte[2 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[2 * 8]);
        BytesRef versions = new BytesRef(new byte[2 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            2,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(2);
        filter.set(0);
        filter.set(1);
        MappedColumns filtered = mc.withFilter(filter);
        assertEquals(Column.Density.SPARSE, filtered.toColumnBatch().columns().iterator().next().density());
    }

    public void testWithFilterNullReturnsSelf() {
        BytesRef[] values = { new BytesRef("a") };
        BytesRef seqNos = new BytesRef(new byte[8]);
        BytesRef primaryTerms = new BytesRef(new byte[8]);
        BytesRef versions = new BytesRef(new byte[8]);
        MappedColumns mc = new MappedColumns(
            0,
            1,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );
        assertSame(mc, mc.withFilter(null));
    }

    public void testWithFilterLongColumn() {
        // 5 docs with values 10..50; filter passes docs 0 and 2
        BytesRef data = new BytesRef(longBytes(10L, 20L, 30L, 40L, 50L));
        BytesRef seqNos = new BytesRef(new byte[5 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[5 * 8]);
        BytesRef versions = new BytesRef(new byte[5 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            5,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.longColumn(data, "val", LONG_FIELD_TYPE, LongColumn.NumericKind.LONG))
        );

        FixedBitSet filter = new FixedBitSet(5);
        filter.set(0);
        filter.set(2);
        MappedColumns filtered = mc.withFilter(filter);

        // Doc 2 moves up to the doc id that the excluded doc 1 would have taken.
        assertEquals(2, filtered.toColumnBatch().numDocs());
        assertEquals(Map.of(0, 10L, 1, 30L), drainLongTuples(filtered));
    }

    public void testWithFilterSlicePreservesFilter() {
        // 6 docs "a".."f"; filter passes {1, 3, 5}; slice [2, 6) → filter windowed to {1, 3}.
        BytesRef[] values = {
            new BytesRef("a"),
            new BytesRef("b"),
            new BytesRef("c"),
            new BytesRef("d"),
            new BytesRef("e"),
            new BytesRef("f") };
        BytesRef seqNos = new BytesRef(new byte[6 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            6,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(6);
        filter.set(1);
        filter.set(3);
        filter.set(5);
        // slice(2, 6) → docs 2,3,4,5 become local 0,1,2,3; only original docs 3 and 5 pass filter
        MappedColumns sliced = mc.withFilter(filter).slice(2, 6);

        assertEquals(4, sliced.docCount());
        assertEquals(2, sliced.toColumnBatch().numDocs());
        assertEquals(Map.of(0, new BytesRef("d"), 1, new BytesRef("f")), drainBinaryTuples(sliced));
        assertEquals(Map.of(1, new BytesRef("d"), 3, new BytesRef("f")), drainBinaryRowCursor(sliced));
    }

    public void testWithFilterAllSetWindowBecomesNull() {
        // All 4 docs present; filter passes all 4; windowed to slice [1, 3) — both set → filter becomes null.
        BytesRef[] values = { new BytesRef("a"), new BytesRef("b"), new BytesRef("c"), new BytesRef("d") };
        BytesRef seqNos = new BytesRef(new byte[4 * 8]);
        BytesRef primaryTerms = new BytesRef(new byte[4 * 8]);
        BytesRef versions = new BytesRef(new byte[4 * 8]);
        MappedColumns mc = new MappedColumns(
            0,
            4,
            seqNos,
            primaryTerms,
            versions,
            List.of(MappedColumns.binaryColumn(values, "field", BINARY_FIELD_TYPE))
        );

        FixedBitSet filter = new FixedBitSet(4);
        filter.set(0);
        filter.set(1);
        filter.set(2);
        filter.set(3);
        // slice(1, 3) covers docs 1,2 — both set in filter → windowFilter returns null → DENSE
        MappedColumns sliced = mc.withFilter(filter).slice(1, 3);
        assertEquals(Column.Density.DENSE, sliced.toColumnBatch().columns().iterator().next().density());
        assertEquals(Map.of(0, new BytesRef("b"), 1, new BytesRef("c")), drainBinaryTuples(sliced));
    }

    // -------------------------------------------------------------------------
    // withFilter — indexing the filtered batch
    // -------------------------------------------------------------------------

    /**
     * Adds a filtered batch through {@code IndexWriter#addBatch}, the call the engine makes, and checks that the remaining documents
     * alone are indexed and that each one holds its own values in every kind of column. Lucene adds one document per doc id of the
     * batch: a filtered-out document that kept its doc id would be indexed as a document without fields, and columns that did not
     * renumber the remaining documents alike would put values on the wrong document.
     */
    public void testFilteredBatchIndexesRemainingDocsOnly() throws IOException {
        final int docCount = between(2, 50);
        // The filter keeps one document for sure and excludes another, the rest are random. Every column has a value for the kept one.
        final int kept = between(0, docCount - 1);
        final int excluded = randomValueOtherThan(kept, () -> between(0, docCount - 1));
        final long[] longs = new long[docCount];
        final BytesRef[] binaries = new BytesRef[docCount];
        final String[] terms = new String[docCount];
        final float[][] vectors = new float[docCount][];
        final boolean[] keep = new boolean[docCount];
        for (int doc = 0; doc < docCount; doc++) {
            keep[doc] = doc == kept || (doc != excluded && randomBoolean());
            longs[doc] = doc;
            binaries[doc] = doc == kept || randomInt(3) > 0 ? new BytesRef("binary-" + doc) : null;
            terms[doc] = doc == kept || randomInt(3) > 0 ? "term-" + doc : null;
            vectors[doc] = doc == kept || randomInt(3) > 0 ? new float[] { doc, -doc } : null;
        }
        final BytesRef seqNos = new BytesRef(new byte[docCount * 8]);
        final BytesRef primaryTerms = new BytesRef(new byte[docCount * 8]);
        final BytesRef versions = new BytesRef(new byte[docCount * 8]);
        final MappedColumns mc = new MappedColumns(
            0,
            docCount,
            seqNos,
            primaryTerms,
            versions,
            List.of(
                MappedColumns.longColumn(new BytesRef(longBytes(longs)), "long", LONG_FIELD_TYPE, LongColumn.NumericKind.LONG),
                MappedColumns.binaryColumn(binaries, "binary", BINARY_FIELD_TYPE),
                new TermColumn(terms, 0, docCount, null),
                new FloatVectorColumn(vectors, 0, docCount, null)
            )
        );

        // A window around the kept document, filtered either before or after it is cut out. The engine filters a slice.
        final int from = between(0, kept);
        final int to = between(kept + 1, docCount);
        final MappedColumns filtered = randomBoolean()
            ? mc.slice(from, to).withFilter(filterOf(keep, from, to))
            : mc.withFilter(filterOf(keep, 0, docCount)).slice(from, to);
        final List<Integer> remaining = new ArrayList<>();
        for (int doc = from; doc < to; doc++) {
            if (keep[doc]) {
                remaining.add(doc);
            }
        }

        final ColumnBatch batch = filtered.toColumnBatch();
        assertEquals(to - from, filtered.docCount());
        assertEquals(remaining.size(), batch.numDocs());
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addBatch(batch);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                // A single batch in a new index: the Lucene doc ids are the doc ids of the batch.
                final LeafReader leaf = getOnlyLeafReader(reader);
                assertEquals(remaining.size(), leaf.maxDoc());
                final NumericDocValues longValues = leaf.getNumericDocValues("long");
                final BinaryDocValues binaryValues = leaf.getBinaryDocValues("binary");
                final FloatVectorValues vectorValues = leaf.getFloatVectorValues("vector");
                final KnnVectorValues.DocIndexIterator vectorDocs = vectorValues.iterator();
                int vectorDoc = vectorDocs.nextDoc();
                for (int docId = 0; docId < remaining.size(); docId++) {
                    final int doc = remaining.get(docId);
                    assertTrue(longValues.advanceExact(docId));
                    assertEquals(doc, longValues.longValue());
                    assertEquals(binaries[doc] != null, binaryValues.advanceExact(docId));
                    if (binaries[doc] != null) {
                        assertEquals(binaries[doc], binaryValues.binaryValue());
                    }
                    if (vectors[doc] != null) {
                        assertEquals(docId, vectorDoc);
                        assertArrayEquals(vectors[doc], vectorValues.vectorValue(vectorDocs.index()), 0f);
                        vectorDoc = vectorDocs.nextDoc();
                    }
                }
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, vectorDoc);
                for (int doc = 0; doc < docCount; doc++) {
                    if (terms[doc] == null) {
                        continue;
                    }
                    final PostingsEnum postings = leaf.postings(new Term("text", terms[doc]));
                    final int docId = remaining.indexOf(doc);
                    if (docId < 0) {
                        assertNull("term of the filtered-out doc [" + doc + "]", postings);
                    } else {
                        assertEquals(docId, postings.nextDoc());
                        assertEquals(DocIdSetIterator.NO_MORE_DOCS, postings.nextDoc());
                    }
                }
            }
        }
    }

    private static FixedBitSet filterOf(boolean[] keep, int from, int to) {
        final FixedBitSet filter = new FixedBitSet(to - from);
        for (int doc = from; doc < to; doc++) {
            if (keep[doc]) {
                filter.set(doc - from);
            }
        }
        return filter;
    }

    /**
     * The doc ids of a window {@code [from, from + count)} over {@code values} that hold a value and pass {@code filter}, which is
     * how the token stream and vector columns of the mappers apply a filter.
     */
    private static <T> ObjectTupleCursor<T> filteredTuples(T[] values, int from, int count, FixedBitSet filter) {
        return new ObjectTupleCursor<>() {
            private int doc = -1;

            @Override
            public int nextDoc() {
                do {
                    doc++;
                } while (doc < count && (values[from + doc] == null || (filter != null && filter.get(doc) == false)));
                return doc < count ? doc : DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public T value() {
                return values[from + doc];
            }
        };
    }

    /**
     * A token stream column in place of the one {@code TsidExtractingIdFieldMapper} keeps to itself: one term per document, or
     * {@code null} for none. It serves {@code IndexWriter#addBatch} only, the row path has no part in these tests.
     */
    private static final class TermColumn extends TokenStreamColumn implements LuceneColumn {

        private final String[] terms;
        private final int from;
        private final int count;
        private final FixedBitSet filter;

        TermColumn(String[] terms, int from, int count, FixedBitSet filter) {
            super("text", TextField.TYPE_NOT_STORED, Density.SPARSE);
            this.terms = terms;
            this.from = from;
            this.count = count;
            this.filter = filter;
        }

        @Override
        public ObjectTupleCursor<TokenStream> tuples() {
            final ObjectTupleCursor<String> in = filteredTuples(terms, from, count, filter);
            return new ObjectTupleCursor<>() {
                @Override
                public int nextDoc() {
                    return in.nextDoc();
                }

                @Override
                public TokenStream value() {
                    return new CannedTokenStream(new Token(in.value(), 0, in.value().length()));
                }
            };
        }

        @Override
        public TermColumn slice(int from, int count) {
            return new TermColumn(terms, this.from + from, count, EscfColumn.windowValidity(filter, from, count));
        }

        @Override
        public TermColumn withFilter(FixedBitSet newFilter) {
            return new TermColumn(terms, from, count, LuceneColumn.singleFilter(filter, newFilter));
        }

        @Override
        public Column toLuceneColumn() {
            return this;
        }

        @Override
        public Column toLuceneColumn(int[] compactedDocIds) {
            return CompactedColumns.of(this, compactedDocIds);
        }

        @Override
        public RowFieldCursor rowFieldCursor() {
            throw new UnsupportedOperationException("only the column batch is under test");
        }
    }

    /**
     * A vector column in place of the package-private one of the {@code dense_vector} mapper: one vector per document, or
     * {@code null} for none. It serves {@code IndexWriter#addBatch} only, the row path has no part in these tests.
     */
    private static final class FloatVectorColumn extends VectorColumn<float[]> implements LuceneColumn {

        private static final FieldType FIELD_TYPE = KnnFloatVectorField.createFieldType(2, VectorSimilarityFunction.EUCLIDEAN);

        private final float[][] vectors;
        private final int from;
        private final int count;
        private final FixedBitSet filter;

        FloatVectorColumn(float[][] vectors, int from, int count, FixedBitSet filter) {
            super("vector", FIELD_TYPE, Density.SPARSE);
            this.vectors = vectors;
            this.from = from;
            this.count = count;
            this.filter = filter;
        }

        @Override
        public ObjectTupleCursor<float[]> tuples() {
            return filteredTuples(vectors, from, count, filter);
        }

        @Override
        public FloatVectorColumn slice(int from, int count) {
            return new FloatVectorColumn(vectors, this.from + from, count, EscfColumn.windowValidity(filter, from, count));
        }

        @Override
        public FloatVectorColumn withFilter(FixedBitSet newFilter) {
            return new FloatVectorColumn(vectors, from, count, LuceneColumn.singleFilter(filter, newFilter));
        }

        @Override
        public Column toLuceneColumn() {
            return this;
        }

        @Override
        public Column toLuceneColumn(int[] compactedDocIds) {
            return CompactedColumns.of(this, compactedDocIds);
        }

        @Override
        public RowFieldCursor rowFieldCursor() {
            throw new UnsupportedOperationException("only the column batch is under test");
        }
    }

    public void testSliceOfSliceSeqNoOffset() {
        BytesRef seqNos = new BytesRef(new byte[6 * 8]); // zero-initialised
        BytesRef primaryTerms = new BytesRef(new byte[6 * 8]);
        BytesRef versions = new BytesRef(new byte[6 * 8]);
        MappedColumns mc = new MappedColumns(0, 6, seqNos, primaryTerms, versions, List.of());

        // [2, 6) then [1, 3) → offset = 3 in the backing array
        MappedColumns sliced = mc.slice(2, 6).slice(1, 3);
        sliced.setSeqNo(0, 100L);
        sliced.setSeqNo(1, 200L);

        assertEquals(100L, ByteUtils.readLongLE(seqNos.bytes, 3 * 8));
        assertEquals(200L, ByteUtils.readLongLE(seqNos.bytes, 4 * 8));
        // neighbours must be untouched
        assertEquals(0L, ByteUtils.readLongLE(seqNos.bytes, 2 * 8));
        assertEquals(0L, ByteUtils.readLongLE(seqNos.bytes, 5 * 8));
    }
}
