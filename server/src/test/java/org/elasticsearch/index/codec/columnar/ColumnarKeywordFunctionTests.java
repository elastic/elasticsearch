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
import org.elasticsearch.columnar.string.StringColumnSource;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.mapper.BinaryDocValuesFormat;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.index.mapper.blockloader.MockWarnings;
import org.elasticsearch.index.mapper.blockloader.docvalues.BlockDocValuesReader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.ByteLengthFromBytesRefDocValuesBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMaxBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.MvMinBytesRefsFromBinaryBlockLoader;
import org.elasticsearch.index.mapper.blockloader.docvalues.fn.Utf8CodePointsFromOrdsBlockLoader;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.Matchers.instanceOf;

/**
 * The functions that read one value a document from a keyword column, against what a scan over the values the document
 * holds would decide. The column answers them from what it already records rather than from the document's payload, so
 * the only thing that says it is right is that the two agree.
 */
public class ColumnarKeywordFunctionTests extends ESTestCase {

    private static final String FIELD = "kw";
    private static final CircuitBreaker NOOP = new NoopCircuitBreaker("test");

    /**
     * The extreme value of each document, which a dictionary column decides over ordinals rather than by comparing
     * terms. Escapes and nulls are the two ordinals that are not terms, so both shapes are here: an escaped value
     * sorts by its bytes wherever its ordinal sits, and a null is no value to be the extreme of.
     */
    public void testMvMaxAndMvMin() throws IOException {
        final String[][] docs = new String[between(200, 1200)][];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = switch (d % 7) {
                case 0 -> new String[] { "b-" + (d % 5), "a-" + (d % 3), "c" };
                case 1 -> new String[] { "a-" + (d % 3) };
                // Rare enough to escape a dictionary built from what the column repeats, and sorting below every
                // term it holds, so an ordinal comparison would get it wrong.
                case 2 -> new String[] { "b-" + (d % 5), "AAA-escapes-" + d };
                case 3 -> new String[] { null, "c" };
                case 4 -> new String[] { null };
                case 5 -> new String[0];
                default -> new String[] { "c", "b-" + (d % 5) };
            };
        }
        for (boolean max : new boolean[] { true, false }) {
            assertLoaderMatches(
                docs,
                fieldName -> max
                    ? new MvMaxBytesRefsFromBinaryBlockLoader(fieldName, BinaryDocValuesFormat.COLUMNAR_PAYLOAD)
                    : new MvMinBytesRefsFromBinaryBlockLoader(fieldName, BinaryDocValuesFormat.COLUMNAR_PAYLOAD),
                extremes(docs, max)
            );
        }
    }

    /** What a scan over the values would decide, which is what the ordinals have to agree with. */
    private static List<Object> extremes(String[][] docs, boolean max) {
        final List<Object> expected = new ArrayList<>();
        for (String[] slots : docs) {
            String best = null;
            for (String slot : slots) {
                if (slot == null) {
                    continue;
                }
                if (best == null || (max ? slot.compareTo(best) > 0 : slot.compareTo(best) < 0)) {
                    best = slot;
                }
            }
            expected.add(best == null ? null : new BytesRef(best));
        }
        return expected;
    }

    /**
     * BYTE_LENGTH and LENGTH, which answer only for a document holding exactly one value and warn for one holding
     * more. The arity comes from the column - how many slots a document has and which of them are null - so a payload
     * is never decoded to count. Every arity is here: none, one, and several, by both nulls and real values.
     */
    public void testLengthFunctions() throws IOException {
        final String[][] docs = new String[between(200, 800)][];
        for (int d = 0; d < docs.length; d++) {
            docs[d] = switch (d % 8) {
                case 0 -> new String[] { "abc" };                     // one value
                case 1 -> new String[] { "\u00e9\u00e8" };                    // two code points, four bytes
                case 2 -> new String[] { null, "one-left" };          // one value after the nulls go
                case 3 -> new String[] { "a", "b" };                  // several: no answer, and a warning
                case 4 -> new String[] { null };                      // none
                case 5 -> new String[0];                              // none
                case 6 -> new String[] { "" };                        // one value, of no bytes
                default -> new String[] { "term-" + (d % 5) };
            };
        }
        for (boolean bytes : new boolean[] { true, false }) {
            final List<Object> expected = new ArrayList<>();
            for (String[] slots : docs) {
                String only = null;
                int nonNull = 0;
                for (String slot : slots) {
                    if (slot != null) {
                        nonNull++;
                        only = slot;
                    }
                }
                expected.add(nonNull != 1 ? null : bytes ? new BytesRef(only).length : only.codePointCount(0, only.length()));
            }
            assertLoaderMatches(
                docs,
                fieldName -> bytes
                    ? new ByteLengthFromBytesRefDocValuesBlockLoader(new MockWarnings(), fieldName, BinaryDocValuesFormat.COLUMNAR_PAYLOAD)
                    : new Utf8CodePointsFromOrdsBlockLoader(
                        new MockWarnings(),
                        fieldName,
                        ByteSizeValue.ofKb(1),
                        BinaryDocValuesFormat.COLUMNAR_PAYLOAD
                    ),
                expected
            );
        }
    }

    private void assertLoaderMatches(
        String[][] docs,
        Function<String, BlockDocValuesReader.DocValuesBlockLoader> loaders,
        List<Object> expected
    ) throws IOException {
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
                final LeafReaderContext leaf = reader.leaves().get(0);
                assertThat("the field is a column", leaf.reader().getBinaryDocValues(FIELD), instanceOf(StringColumnSource.class));
                final var loader = loaders.apply(FIELD);
                final TestBlock block = (TestBlock) loader.reader(NOOP, leaf).read(TestBlock.factory(), docs(0, docs.length), 0, false);
                assertEquals("positions", docs.length, block.size());
                for (int d = 0; d < docs.length; d++) {
                    assertEquals("document " + d + " of " + Arrays.toString(docs[d]), expected.get(d), block.get(d));
                }
            }
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

    private static BytesRef encode(String[] slots) {
        final List<BytesRef> refs = new ArrayList<>(slots.length);
        for (String slot : slots) {
            refs.add(slot == null ? null : new BytesRef(slot));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(refs));
    }
}
