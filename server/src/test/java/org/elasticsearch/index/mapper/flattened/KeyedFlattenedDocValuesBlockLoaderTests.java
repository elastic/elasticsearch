/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper.flattened;

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
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.fielddata.MultiValuedSortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField;
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

    // -------------------------------------------------------------------------
    // Filtering-semantics tests for KeyedFlattenedBinaryDocValues
    // -------------------------------------------------------------------------

    /** Open a key-filtered binary doc-values view on the first (and only) leaf. */
    private static SortedBinaryDocValues filteredBinaryView(LeafReaderContext leaf, String key) throws IOException {
        MultiValuedSortedBinaryDocValues dv = MultiValuedSortedBinaryDocValues.fromMultiValued(leaf.reader(), KEYED_FIELD);
        return BinaryKeyedFlattenedLeafFieldData.getKeyFilteredSortedBinaryDocValues(dv, key);
    }

    /** Add a document with no values for the keyed field. */
    private static void addEmptyBinaryDoc(RandomIndexWriter writer) throws IOException {
        writer.addDocument(new Document());
    }

    /**
     * Target key is the lexicographically first key in the doc.
     * The single-pass path must not break early before seeing the first entry.
     */
    public void testKeyFilteredBinaryDvKeyFirstInDoc() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, KEY + "\0server-a", "zzz.z\0last");
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertTrue(dv.advanceExact(0));
                assertEquals(1, dv.docValueCount());
                assertEquals(new BytesRef("server-a"), dv.nextValue());
                assertNull(dv.nextValue());
            }
        }
    }

    /**
     * Target key is the lexicographically last key in the doc.
     * The two-pass implementation would call advanceExact twice and re-walk all prior entries;
     * the single-pass path buffers the value during the first scan.
     */
    public void testKeyFilteredBinaryDvKeyLastInDoc() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, "aaa.x\0first", KEY + "\0server-d");
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertTrue(dv.advanceExact(0));
                assertEquals(1, dv.docValueCount());
                assertEquals(new BytesRef("server-d"), dv.nextValue());
            }
        }
    }

    /**
     * Target key is in the middle; other keys sort both below and above it.
     */
    public void testKeyFilteredBinaryDvKeyMiddleInDoc() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, "aaa.x\0before", KEY + "\0server-b", "zzz.z\0after");
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertTrue(dv.advanceExact(0));
                assertEquals(1, dv.docValueCount());
                assertEquals(new BytesRef("server-b"), dv.nextValue());
            }
        }
    }

    /**
     * The target key appears twice in one doc.
     * Both values must be returned in sorted order and docValueCount() must be 2.
     */
    public void testKeyFilteredBinaryDvMultipleMatchingValues() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, KEY + "\0server-e", KEY + "\0server-f", "zzz.z\0other");
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertTrue(dv.advanceExact(0));
                assertEquals(2, dv.docValueCount());
                assertEquals(new BytesRef("server-e"), dv.nextValue());
                assertEquals(new BytesRef("server-f"), dv.nextValue());
                assertNull(dv.nextValue());
            }
        }
    }

    /**
     * The target key is absent and all present keys sort above it.
     * The single-pass loop breaks immediately on the first entry (comparison negative).
     */
    public void testKeyFilteredBinaryDvKeyAbsentBreaksEarly() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, "zzz.z\0only");
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertFalse(dv.advanceExact(0));
            }
        }
    }

    /**
     * The target key is absent and all present keys sort below it.
     * The loop exhausts the doc's values without finding a match.
     */
    public void testKeyFilteredBinaryDvKeyAbsentAllSortBelow() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, "aaa.x\0val1", "aaa.y\0val2");
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertFalse(dv.advanceExact(0));
            }
        }
    }

    /**
     * A doc with no values for the keyed field at all; advanceExact must return false.
     */
    public void testKeyFilteredBinaryDvDocWithNoValues() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, KEY + "\0server-x");  // doc 0 has a value so the field exists in the segment
            addEmptyBinaryDoc(writer);                  // doc 1 has no values
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertTrue(dv.advanceExact(0));
                assertFalse(dv.advanceExact(1));
            }
        }
    }

    /**
     * Advance over multiple docs with different match counts on the same reader instance to
     * verify that count and index are correctly reset between advanceExact calls.
     * Pattern: 2 matches → 1 match → 0 matches → 2 matches again.
     */
    public void testKeyFilteredBinaryDvBufferReuseAcrossDocs() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            addDoc(writer, true, KEY + "\0val-a", KEY + "\0val-b");  // doc 0: 2 matches
            addDoc(writer, true, KEY + "\0val-c");                    // doc 1: 1 match
            addDoc(writer, true, "aaa.x\0other");                     // doc 2: 0 matches
            addDoc(writer, true, KEY + "\0val-d", KEY + "\0val-e");  // doc 3: 2 matches again
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);

                assertTrue(dv.advanceExact(0));
                assertEquals(2, dv.docValueCount());
                assertEquals(new BytesRef("val-a"), dv.nextValue());
                assertEquals(new BytesRef("val-b"), dv.nextValue());
                assertNull(dv.nextValue());

                assertTrue(dv.advanceExact(1));
                assertEquals(1, dv.docValueCount());
                assertEquals(new BytesRef("val-c"), dv.nextValue());
                assertNull(dv.nextValue());

                assertFalse(dv.advanceExact(2));

                assertTrue(dv.advanceExact(3));
                assertEquals(2, dv.docValueCount());
                assertEquals(new BytesRef("val-d"), dv.nextValue());
                assertEquals(new BytesRef("val-e"), dv.nextValue());
            }
        }
    }

    /**
     * "host.name" is a strict prefix of "host.name.inner" stored in the same doc.
     * Only the exact-key value must be returned, with the key prefix stripped.
     * The entry for "host.name.inner" must not be matched — compare returns negative,
     * triggering an early break.
     */
    public void testKeyFilteredBinaryDvPrefixStripping() throws IOException {
        try (Directory dir = newDirectory(); RandomIndexWriter writer = newRandomIndexWriter(dir, true)) {
            // "host.name\0bare-value" sorts before "host.name.inner\0inner-value"
            // because '\0' (0) < '.' (46) at position 9.
            addDoc(writer, true, KEY + "\0bare-value", KEY + ".inner\0inner-value");
            try (IndexReader reader = openReader(writer)) {
                SortedBinaryDocValues dv = filteredBinaryView(reader.leaves().get(0), KEY);
                assertTrue(dv.advanceExact(0));
                assertEquals(1, dv.docValueCount());
                assertEquals(new BytesRef("bare-value"), dv.nextValue());
                assertNull(dv.nextValue());
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

    private static IndexReader openReader(RandomIndexWriter writer) throws IOException {
        // forceMerge(1) keeps the docs in a single segment so canReuse is exercised across
        // pages of the same segment, which is the operator-level path the bug regresses.
        writer.forceMerge(1);
        return ElasticsearchDirectoryReader.wrap(DirectoryReader.open(writer.w), new ShardId("test", "_na_", 0));
    }
}
