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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.columnar.string.StringBlockSink;
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.docvalues.BytesRefsFromBinaryMultiSeparateCountBlockLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;

/**
 * The block a page of a columnar keyword column becomes, against the block the same documents become when read one at a
 * time. The page path is a different implementation of the same answer - it can hand back ordinals into the page's own
 * distinct values rather than the values themselves - so the only thing that says it is right is that the two agree.
 *
 * <p>Worth its own test because nothing else reaches it: the ESQL columnar suites run in columnar index mode without the
 * columnar codec, so they never build a {@code BINARY_COLUMNAR_PAYLOAD} field at all.
 */
public class ColumnarKeywordBlockLoaderTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final CircuitBreaker NOOP = new NoopCircuitBreaker("test");

    public void testSingleValued() throws IOException {
        assertPageMatchesPerDocument(docs -> {
            for (int d = 0; d < docs.length; d++) {
                docs[d] = new String[] { "term-" + (d % 5) };
            }
        });
    }

    public void testMultiValued() throws IOException {
        assertPageMatchesPerDocument(docs -> {
            for (int d = 0; d < docs.length; d++) {
                docs[d] = switch (d % 4) {
                    case 0 -> new String[] { "a-" + (d % 7), "b-" + (d % 3) };
                    case 1 -> new String[] { "a-" + (d % 7) };
                    case 2 -> new String[] { "a-" + (d % 7), "b-" + (d % 3), "c" };
                    default -> new String[] { "c" };
                };
            }
        });
    }

    public void testNullsAndEmptyArrays() throws IOException {
        assertPageMatchesPerDocument(docs -> {
            for (int d = 0; d < docs.length; d++) {
                docs[d] = switch (d % 6) {
                    case 0 -> new String[] { "a-" + (d % 5), null, "b" };
                    case 1 -> new String[] { null };          // a document holding no value
                    case 2 -> new String[0];                  // an empty array, likewise
                    case 3 -> new String[] { null, null };
                    case 4 -> new String[] { "" };            // the empty string is a value
                    default -> new String[] { "a-" + (d % 5) };
                };
            }
        });
    }

    /** Values distinct enough that a page has nothing worth naming, so it comes back as values rather than ordinals. */
    public void testPageThatDoesNotRepeat() throws IOException {
        assertPageMatchesPerDocument(docs -> {
            for (int d = 0; d < docs.length; d++) {
                docs[d] = new String[] { "unique-value-" + d };
            }
        });
    }

    private interface Documents {
        void fill(String[][] docs);
    }

    private void assertPageMatchesPerDocument(Documents documents) throws IOException {
        final String[][] docs = new String[between(200, 1500)][];
        documents.fill(docs);

        final FieldType type = columnarBinaryFieldType();
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig().setCodec(columnarCodec()))) {
                for (String[] slots : docs) {
                    final Document doc = new Document();
                    doc.add(new Field(FIELD, encode(slots), type));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("one segment", 1, reader.leaves().size());
                final LeafReaderContext leaf = reader.leaves().get(0);
                final var loader = new BytesRefsFromBinaryMultiSeparateCountBlockLoader(FIELD, BinaryDocValuesFormat.COLUMNAR_PAYLOAD);

                // The comparison below is between the page path and the per-document path, and would agree for the
                // wrong reason if the column declined every page and both sides read a document at a time. So the
                // column is asked directly whether it serves these shapes at all.
                final BinaryDocValues values = leaf.reader().getBinaryDocValues(FIELD);
                assertThat("the field is a column", values, instanceOf(StringColumnSource.class));
                final int[] everything = new int[docs.length];
                for (int i = 0; i < docs.length; i++) {
                    everything[i] = i;
                }
                assertTrue(
                    "the column serves a page of these documents",
                    ((StringColumnSource) values).reader().readBlock(everything, 0, everything.length, NOOP_SINK)
                );

                // Read in several pages, so a boundary falls inside the run of documents rather than only at its ends.
                for (int page : new int[] { 1, 7, 128, docs.length }) {
                    for (int from = 0; from < docs.length; from += page) {
                        final int count = Math.min(page, docs.length - from);
                        final BlockLoader.Docs wanted = docs(from, count);

                        final TestBlock asPage = (TestBlock) loader.reader(NOOP, leaf).read(TestBlock.factory(), wanted, 0, false);
                        final TestBlock perDocument = readOneAtATime(loader, leaf, wanted);

                        assertEquals("positions at " + from + " of " + page, perDocument.size(), asPage.size());
                        for (int i = 0; i < asPage.size(); i++) {
                            assertEquals("page of " + page + " document " + (from + i), perDocument.get(i), asPage.get(i));
                        }
                    }
                }
            }
        }
    }

    /** The same documents through the row-stride path, which reads and decodes a payload apiece. */
    private static TestBlock readOneAtATime(
        BytesRefsFromBinaryMultiSeparateCountBlockLoader loader,
        LeafReaderContext leaf,
        BlockLoader.Docs wanted
    ) throws IOException {
        final BlockLoader.RowStrideReader rows = loader.rowStrideReader(NOOP, leaf);
        try (BlockLoader.Builder builder = loader.builder(TestBlock.factory(), wanted.count())) {
            for (int i = 0; i < wanted.count(); i++) {
                rows.read(wanted.get(i), null, builder);
            }
            return (TestBlock) builder.build();
        }
    }

    private static BlockLoader.Docs docs(int from, int count) {
        return new BlockLoader.Docs() {
            @Override
            public int count() {
                return count;
            }

            @Override
            public int get(int i) {
                return from + i;
            }

            @Override
            public boolean mayContainDuplicates() {
                return false;
            }
        };
    }

    /** Every doc-values field through the columnar format, which is what makes the field a column. */
    private static Codec columnarCodec() {
        final Codec base = TestUtil.getDefaultCodec();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat();
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

    private static FieldType columnarBinaryFieldType() {
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.putAttribute(ColumNARDocValuesFormat.TYPE_ATTRIBUTE, ColumnarFieldType.STRING.name());
        type.freeze();
        return type;
    }

    /** Takes a page and does nothing with it, for a test asking only whether one is served. */
    private static final StringBlockSink NOOP_SINK = new StringBlockSink() {
        @Override
        public void appendOrdinals(int[] ordinals, int valueCount, int[] valueCounts, int docCount, BytesRef[] dict, int dictSize) {}

        @Override
        public void appendValues(BytesRef[] values, int valueCount, int[] valueCounts, int docCount) {}
    };

    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }
}
