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
