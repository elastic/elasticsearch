/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.columnar;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.RegExp;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.MultiValuedSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.SortableBinaryDocValues;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.SingleValuedColumnarBinaryDocValuesField;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.ByteLengthFromBytesRefDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.Utf8CodePointsFromOrdsBlockLoader;
import org.elasticsearch.lucene.queries.BinaryDocValuesQueries;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;

/**
 * A keyword pushdown over a columnar keyword is answered by the column, in either framing the field is written in.
 *
 * <p>Every pushdown also has a route that asks the doc values for each document's blob, which gives the same answers.
 * So the column is put behind doc values that refuse to hand over a blob, and a pushdown that takes that route for a
 * page of documents fails here.
 */
public class ColumnarKeywordPushdownGuardTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final CircuitBreaker NOOP = new NoopCircuitBreaker("test");

    /** How a keyword's documents are written, and the readers the mapper picks for that framing. */
    private enum Framing {
        PAYLOAD(BinaryDocValuesFormat.COLUMNAR_PAYLOAD) {
            @Override
            Field field(String value) {
                final FieldType type = new FieldType();
                type.setDocValuesType(DocValuesType.BINARY);
                type.freeze();
                return new Field(FIELD, payload(value), type);
            }

            @Override
            BlockDocValuesReader.DocValuesBlockLoader valueLoader() {
                return new BytesRefsFromBinaryMultiSeparateCountBlockLoader(FIELD, format);
            }

            @Override
            SortableBinaryDocValues fieldData(LeafReader leaf) throws IOException {
                return ColumnarPayloadSortableBinaryDocValues.from(leaf, FIELD);
            }
        },
        SINGLE_VALUED(BinaryDocValuesFormat.PLAIN) {
            @Override
            Field field(String value) {
                return new SingleValuedColumnarBinaryDocValuesField(FIELD, new BytesRef(value));
            }

            @Override
            BlockDocValuesReader.DocValuesBlockLoader valueLoader() {
                return new BytesRefsFromBinaryBlockLoader(FIELD);
            }

            @Override
            SortableBinaryDocValues fieldData(LeafReader leaf) throws IOException {
                return MultiValuedSortableBinaryDocValues.fromPlain(leaf, FIELD);
            }
        };

        final BinaryDocValuesFormat format;

        Framing(BinaryDocValuesFormat format) {
            this.format = format;
        }

        abstract Field field(String value);

        abstract BlockDocValuesReader.DocValuesBlockLoader valueLoader();

        abstract SortableBinaryDocValues fieldData(LeafReader leaf) throws IOException;
    }

    public void testValues() throws IOException {
        for (Framing framing : Framing.values()) {
            assertLoaderReadsTheColumn(framing, framing.valueLoader(), false, value -> new BytesRef(value));
        }
    }

    /** Answered from the lengths the column stores, so no value is read for a page or for a lone document. */
    public void testByteLength() throws IOException {
        for (Framing framing : Framing.values()) {
            assertLoaderReadsTheColumn(
                framing,
                new ByteLengthFromBytesRefDocValuesBlockLoader(new MockWarnings(), FIELD, framing.format),
                true,
                value -> new BytesRef(value).length
            );
        }
    }

    public void testMvMaxAndMvMin() throws IOException {
        for (Framing framing : Framing.values()) {
            assertLoaderReadsTheColumn(
                framing,
                new MvMaxBytesRefsFromBinaryBlockLoader(FIELD, framing.format),
                false,
                value -> new BytesRef(value)
            );
            assertLoaderReadsTheColumn(
                framing,
                new MvMinBytesRefsFromBinaryBlockLoader(FIELD, framing.format),
                false,
                value -> new BytesRef(value)
            );
        }
    }

    /**
     * The length in code points over a payload. A {@code multi_value: false} field is left out: counting code points
     * needs the value, and its blob is the value.
     */
    public void testLengthOverPayload() throws IOException {
        assertLoaderReadsTheColumn(
            Framing.PAYLOAD,
            new Utf8CodePointsFromOrdsBlockLoader(new MockWarnings(), FIELD, ByteSizeValue.ofKb(1), Framing.PAYLOAD.format),
            false,
            value -> value.codePointCount(0, value.length())
        );
    }

    /**
     * A column says that its documents hold at most one value and whether every document holds one. A query that checks
     * a field is single-valued is dropped where both hold, and otherwise runs beside every filter on the field.
     */
    public void testFieldDataReportsSingleValuedAndDensity() throws IOException {
        for (Framing framing : Framing.values()) {
            final String[] dense = values(randomBoolean());
            for (int d = 0; d < dense.length; d++) {
                if (dense[d] == null) {
                    dense[d] = "term-0";
                }
            }
            withSegment(dense, framing, leaf -> {
                final SortableBinaryDocValues fieldData = framing.fieldData(leaf.reader());
                assertEquals(framing.toString(), SortableBinaryDocValues.ValueMode.SINGLE_VALUED, fieldData.getValueMode());
                assertEquals(framing.toString(), SortableBinaryDocValues.Sparsity.DENSE, fieldData.getSparsity());
            });
            withSegment(values(randomBoolean()), framing, leaf -> {
                final SortableBinaryDocValues fieldData = framing.fieldData(leaf.reader());
                assertEquals(framing.toString(), SortableBinaryDocValues.ValueMode.SINGLE_VALUED, fieldData.getValueMode());
                assertEquals(framing.toString(), SortableBinaryDocValues.Sparsity.SPARSE, fieldData.getSparsity());
            });
        }
    }

    /**
     * A column whose documents each hold one value hands over the documents holding one as its own iterator, which is
     * what a query checking for a single value then runs on, in place of asking about each document.
     */
    public void testFieldDataHandsOverSingleValuedDocuments() throws IOException {
        for (Framing framing : Framing.values()) {
            final String[] values = values(randomBoolean());
            withSegment(values, framing, leaf -> {
                final DocIdSetIterator docs = framing.fieldData(leaf.reader()).singleValuedDocs();
                assertNotNull(framing.toString(), docs);
                for (int d = 0; d < values.length; d++) {
                    if (values[d] != null) {
                        assertEquals(framing + " document", d, docs.nextDoc());
                    }
                }
                assertEquals(framing.toString(), DocIdSetIterator.NO_MORE_DOCS, docs.nextDoc());
            });
        }
    }

    /** A document whose one slot is null holds no value, so it is not among the documents holding one. */
    public void testNullSlotIsNotASingleValuedDocument() throws IOException {
        withPayloads(d -> d % 7 == 3 ? new String[] { null } : new String[] { "term-" + (d % 5) }, (leaf, docs) -> {
            final SortableBinaryDocValues fieldData = Framing.PAYLOAD.fieldData(leaf.reader());
            // Every document has a slot, but not every document holds a value, so the column is not reported dense.
            assertEquals(SortableBinaryDocValues.Sparsity.UNKNOWN, fieldData.getSparsity());
            final DocIdSetIterator single = fieldData.singleValuedDocs();
            assertNotNull(single);
            for (int d = 0; d < docs; d++) {
                if (d % 7 != 3) {
                    assertEquals(d, single.nextDoc());
                }
            }
            assertEquals(DocIdSetIterator.NO_MORE_DOCS, single.nextDoc());
        });
    }

    /** Where a document holds several values, holding a value does not say how many, so the documents holding one stay unknown. */
    public void testSeveralValuesKeepSingleValuedDocumentsUnknown() throws IOException {
        withPayloads(
            d -> d % 7 == 3 ? new String[] { "a", "b" } : new String[] { "term-" + (d % 5) },
            (leaf, docs) -> assertNull(Framing.PAYLOAD.fieldData(leaf.reader()).singleValuedDocs())
        );
    }

    /** One payload-framed segment whose documents hold the slots {@code slots} gives each. */
    private void withPayloads(IntFunction<String[]> slots, CheckedBiConsumer<LeafReaderContext, Integer, IOException> check)
        throws IOException {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        final int docs = between(50, 400);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (int d = 0; d < docs; d++) {
                    final List<BytesRef> refs = new ArrayList<>();
                    for (String slot : slots.apply(d)) {
                        refs.add(slot == null ? null : new BytesRef(slot));
                    }
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs)), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                check.accept(reader.leaves().get(0), docs);
            }
        }
    }

    /** Each query shape matches the same documents over the guarded column as over the column itself. */
    public void testQueries() throws IOException {
        for (Framing framing : Framing.values()) {
            final BinaryDocValuesQueries queries = BinaryDocValuesQueries.forFormat(framing.format);
            final Map<String, Query> shapes = new LinkedHashMap<>();
            shapes.put("term", queries.term(FIELD, new BytesRef("term-3")));
            shapes.put("terms", queries.terms(FIELD, List.of(new BytesRef("term-1"), new BytesRef("term-5"))));
            shapes.put("range", queries.range(FIELD, new BytesRef("term-2"), new BytesRef("term-4"), true, false));
            shapes.put("prefix", queries.prefix(FIELD, "term-", false));
            shapes.put("prefix, case insensitive", queries.prefix(FIELD, "TERM-", true));
            shapes.put("fuzzy", queries.fuzzy(FIELD, "term-9", 1, 0, true));
            shapes.put("case insensitive term", queries.caseInsensitiveTerm(FIELD, "TERM-3"));
            shapes.put("wildcard", queries.wildcard(FIELD, "*erm-3", false));
            shapes.put("wildcard, contained", queries.wildcard(FIELD, "*rm-*", false));
            shapes.put("wildcard, case insensitive", queries.wildcard(FIELD, "TERM-?", true));
            shapes.put("automaton", queries.automaton(FIELD, Automata.makeString("term-2"), "term-2"));
            shapes.put("regexp", queries.regexp(FIELD, "term-[0-3]", RegExp.ALL, 0, 10_000, null));
            for (boolean repeating : new boolean[] { true, false }) {
                final String[] values = values(repeating);
                withSegment(values, framing, leaf -> {
                    final IndexSearcher column = new IndexSearcher(leaf.reader());
                    final IndexSearcher guarded = new IndexSearcher(guarded(leaf, false).reader());
                    for (Map.Entry<String, Query> shape : shapes.entrySet()) {
                        final String label = framing + " " + shape.getKey();
                        final int expected = column.count(shape.getValue());
                        assertTrue(label + " matches some documents and not all of them", expected > 0 && expected < values.length);
                        assertEquals(label, expected, guarded.count(shape.getValue()));
                    }
                });
            }
        }
    }

    /**
     * The reader of bare values hands back a payload-framed column's payloads. A page of that column's values would be
     * its slots, which is not what that reader is asked for.
     */
    public void testPayloadColumnIsNotReadAsValues() throws IOException {
        final String[] values = values(true);
        withSegment(values, Framing.PAYLOAD, leaf -> {
            final var loader = new BytesRefsFromBinaryBlockLoader(FIELD);
            final TestBlock block = (TestBlock) loader.reader(NOOP, leaf).read(TestBlock.factory(), docs(0, values.length), 0, false);
            for (int d = 0; d < values.length; d++) {
                assertEquals("document " + d, values[d] == null ? null : payload(values[d]), block.get(d));
            }
        });
    }

    /** What a reader charges for its pages is given back when it is closed. */
    public void testPageStorageIsReleased() throws IOException {
        final String[] values = values(randomBoolean());
        for (Framing framing : Framing.values()) {
            final List<BlockDocValuesReader.DocValuesBlockLoader> loaders = List.of(
                framing.valueLoader(),
                new ByteLengthFromBytesRefDocValuesBlockLoader(new MockWarnings(), FIELD, framing.format)
            );
            withSegment(values, framing, leaf -> {
                for (BlockDocValuesReader.DocValuesBlockLoader loader : loaders) {
                    final CountingBreaker breaker = new CountingBreaker();
                    try (BlockLoader.ColumnAtATimeReader reader = loader.reader(breaker, leaf)) {
                        final long opened = breaker.getUsed();
                        reader.read(TestBlock.factory(), docs(0, values.length), 0, false);
                        assertThat(framing + " " + loader + " charges for its page", breaker.getUsed(), greaterThan(opened));
                    }
                    assertEquals(framing + " " + loader, 0, breaker.getUsed());
                }
            });
        }
    }

    /**
     * Reads {@code loader} over the guarded column in pages of several sizes and in a page naming each document twice,
     * as a lookup or a top-n does. A lone document is not a page, so it is read from the column itself unless
     * {@code lengthsOnly} says the loader never reads a value.
     */
    private void assertLoaderReadsTheColumn(
        Framing framing,
        BlockDocValuesReader.DocValuesBlockLoader loader,
        boolean lengthsOnly,
        Function<String, Object> expected
    ) throws IOException {
        for (boolean repeating : new boolean[] { true, false }) {
            final String[] values = values(repeating);
            final String label = framing + " " + loader;
            withSegment(values, framing, leaf -> {
                final LeafReaderContext guarded = guarded(leaf, lengthsOnly);
                for (int page : new int[] { 2, 7, 128, values.length }) {
                    for (int from = 0; from < values.length; from += page) {
                        final int[] wanted = new int[Math.min(page, values.length - from)];
                        for (int i = 0; i < wanted.length; i++) {
                            wanted[i] = from + i;
                        }
                        final boolean lone = wanted.length == 1 && lengthsOnly == false;
                        assertBlock(label + " page of " + page, loader, lone ? leaf : guarded, wanted, values, expected);
                    }
                }
                final int[] twice = new int[values.length * 2];
                for (int d = 0; d < values.length; d++) {
                    twice[2 * d] = d;
                    twice[2 * d + 1] = d;
                }
                assertBlock(label + " repeated", loader, guarded, twice, values, expected);
                for (int d = 0; d < values.length; d += between(1, 20)) {
                    assertBlock(label + " alone", loader, lengthsOnly ? guarded : leaf, new int[] { d }, values, expected);
                }
            });
        }
    }

    private static void assertBlock(
        String label,
        BlockDocValuesReader.DocValuesBlockLoader loader,
        LeafReaderContext leaf,
        int[] wanted,
        String[] values,
        Function<String, Object> expected
    ) throws IOException {
        try (BlockLoader.ColumnAtATimeReader reader = loader.reader(NOOP, leaf)) {
            assertRead(label, reader, wanted, values, expected);
        }
    }

    private static void assertRead(
        String label,
        BlockLoader.ColumnAtATimeReader reader,
        int[] wanted,
        String[] values,
        Function<String, Object> expected
    ) throws IOException {
        final TestBlock block = (TestBlock) reader.read(TestBlock.factory(), docs(wanted), 0, false);
        assertEquals(label + " positions", wanted.length, block.size());
        for (int i = 0; i < wanted.length; i++) {
            final String value = values[wanted[i]];
            assertEquals(label + " document " + wanted[i], value == null ? null : expected.apply(value), block.get(i));
        }
    }

    /**
     * One reader over several pages, as a query reads them: what the reader and the column keep between pages carries
     * from each to the next. A page may start at or before the last document of the one before it, and may be larger
     * than any before it.
     */
    public void testOneReaderAcrossPages() throws IOException {
        for (Framing framing : Framing.values()) {
            assertOneReaderAcrossPages(framing, framing.valueLoader(), false, value -> new BytesRef(value));
            assertOneReaderAcrossPages(
                framing,
                new ByteLengthFromBytesRefDocValuesBlockLoader(new MockWarnings(), FIELD, framing.format),
                true,
                value -> new BytesRef(value).length
            );
        }
    }

    private void assertOneReaderAcrossPages(
        Framing framing,
        BlockDocValuesReader.DocValuesBlockLoader loader,
        boolean lengthsOnly,
        Function<String, Object> expected
    ) throws IOException {
        for (boolean repeating : new boolean[] { true, false }) {
            final String[] values = values(repeating);
            final int n = values.length;
            // { from, count }: growing, starting on the last document of the page before, starting before it, every
            // document at once, and a small page after the largest.
            final int[][] pages = { { 0, 5 }, { 5, 40 }, { 44, 30 }, { 20, 60 }, { 0, n }, { n - 3, 3 }, { between(0, n - 2), 2 } };
            withSegment(values, framing, leaf -> {
                try (BlockLoader.ColumnAtATimeReader reader = loader.reader(NOOP, guarded(leaf, lengthsOnly))) {
                    for (int[] page : pages) {
                        final int[] wanted = new int[page[1]];
                        for (int i = 0; i < wanted.length; i++) {
                            wanted[i] = page[0] + i;
                        }
                        assertRead(framing + " " + loader + " page from " + page[0] + " of " + page[1], reader, wanted, values, expected);
                    }
                }
            });
        }
    }

    /** One value a document, null for a document without the field: few distinct values, or mostly distinct ones. */
    private static String[] values(boolean repeating) {
        final String[] values = new String[between(200, 1500)];
        for (int d = 0; d < values.length; d++) {
            if (d % 6 == 3) {
                values[d] = null;
            } else if (repeating || d % 4 == 0) {
                values[d] = "term-" + (d % 7);
            } else {
                values[d] = "unique-value-" + d + "-" + randomAlphaOfLength(d % 9);
            }
        }
        return values;
    }

    /** Indexes {@code values} as one columnar segment in {@code framing} and hands over its leaf. */
    private void withSegment(String[] values, Framing framing, CheckedConsumer<LeafReaderContext, IOException> check) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (String value : values) {
                    final Document doc = new Document();
                    if (value != null) {
                        doc.add(framing.field(value));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                final LeafReaderContext leaf = reader.leaves().get(0);
                assertThat("the field is a column", leaf.reader().getBinaryDocValues(FIELD), instanceOf(StringColumnSource.class));
                check.accept(leaf);
            }
        }
    }

    /** The leaf with its column behind a {@link GuardedColumn}. */
    private static LeafReaderContext guarded(LeafReaderContext leaf, boolean lengthsOnly) {
        return new FilterLeafReader(leaf.reader()) {
            @Override
            public BinaryDocValues getBinaryDocValues(String field) throws IOException {
                final BinaryDocValues values = in.getBinaryDocValues(field);
                return values == null ? null : new GuardedColumn(values, lengthsOnly);
            }

            @Override
            public CacheHelper getCoreCacheHelper() {
                return in.getCoreCacheHelper();
            }

            @Override
            public CacheHelper getReaderCacheHelper() {
                return in.getReaderCacheHelper();
            }
        }.getContext();
    }

    /**
     * A column's doc values that never hand over a document's blob. With {@code lengthsOnly} they hand over no value of
     * a document at all, only what the column records about it.
     */
    private static final class GuardedColumn extends BinaryDocValues implements StringColumnSource {
        private final BinaryDocValues values;
        private final StringColumnSource column;
        private final boolean lengthsOnly;

        GuardedColumn(BinaryDocValues values, boolean lengthsOnly) {
            this.values = values;
            this.column = (StringColumnSource) values;
            this.lengthsOnly = lengthsOnly;
        }

        @Override
        public BytesRef binaryValue() {
            throw new AssertionError("asked for the blob of document [" + values.docID() + "] rather than the column");
        }

        private void readsAValue() {
            if (lengthsOnly) {
                throw new AssertionError("read the value of document [" + values.docID() + "] rather than its length");
            }
        }

        @Override
        public BytesRef extreme(boolean max, BytesRef dst) throws IOException {
            readsAValue();
            return column.extreme(max, dst);
        }

        @Override
        public int nonNullValues(BytesRef dst) throws IOException {
            readsAValue();
            return column.nonNullValues(dst);
        }

        @Override
        public BytesRef slotAt(int slot) throws IOException {
            readsAValue();
            return column.slotAt(slot);
        }

        @Override
        public StringColumnReader reader() {
            return column.reader();
        }

        @Override
        public boolean singleValued() {
            return column.singleValued();
        }

        @Override
        public int nonNullValueCount() throws IOException {
            return column.nonNullValueCount();
        }

        @Override
        public int slotCount() throws IOException {
            return column.slotCount();
        }

        @Override
        public int nonNullLength(int[] length) throws IOException {
            return column.nonNullLength(length);
        }

        @Override
        public boolean advanceExact(int target) throws IOException {
            return values.advanceExact(target);
        }

        @Override
        public int docID() {
            return values.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return values.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            return values.advance(target);
        }

        @Override
        public long cost() {
            return values.cost();
        }
    }

    private static BlockLoader.Docs docs(int from, int count) {
        final int[] wanted = new int[count];
        for (int i = 0; i < count; i++) {
            wanted[i] = from + i;
        }
        return docs(wanted);
    }

    private static BlockLoader.Docs docs(int[] wanted) {
        return new BlockLoader.Docs() {
            @Override
            public int count() {
                return wanted.length;
            }

            @Override
            public int get(int i) {
                return wanted[i];
            }

            @Override
            public boolean mayContainDuplicates() {
                return true;
            }
        };
    }

    /** A breaker that never trips and keeps count of what it holds. */
    private static final class CountingBreaker extends NoopCircuitBreaker {
        private long used;

        CountingBreaker() {
            super("test");
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) {
            used += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            used += bytes;
        }

        @Override
        public long getUsed() {
            return used;
        }
    }

    /** Every doc-values field through the columnar format, which is what makes the field a column. */
    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        return new FilterCodec(base.getName(), base) {
            private final DocValuesFormat perField = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return columnar;
                }
            };

            @Override
            public DocValuesFormat docValuesFormat() {
                return perField;
            }
        };
    }

    private static BytesRef payload(String value) {
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(List.of(new BytesRef(value))));
    }
}
