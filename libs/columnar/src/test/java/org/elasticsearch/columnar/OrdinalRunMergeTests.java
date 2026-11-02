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
import org.apache.lucene.index.LeafReaderContext;
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
import org.elasticsearch.columnar.string.StringColumnReader;
import org.elasticsearch.columnar.string.StringColumnWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.columnar.ColumnarTestUtils.columnarBinaryFieldType;
import static org.elasticsearch.columnar.ColumnarTestUtils.columnarCodecForField;
import static org.hamcrest.Matchers.greaterThan;

/**
 * A merge of dictionary string columns reads the ordinals of every run of a segment's documents that lands in the
 * merged segment in its own order ({@link MergeStretches}) a block at a time, and the merged column reads back as the
 * documents were written either way, its recorded order included.
 */
public class OrdinalRunMergeTests extends ESTestCase {

    private static final String FIELD = "keyword";
    private static final String ID = "id";
    private static final String SORT = "sort";

    /** Every term worth a place in the dictionary, so a segment's dictionary holds every value it saw. */
    private static final StringColumnOptions OPTIONS = StringColumnOptions.DEFAULT.withPolicies(
        new DictionaryPolicy(512 * 1024, 0.0, Double.MAX_VALUE),
        StringColumnOptions.DEFAULT_SUMMARY
    );

    /** Adds up the dictionary slots the merge reports taking in ordinal runs. */
    private static final class RunCount extends InfoStream {
        volatile long slots;
        /** Values the segments let escape their dictionaries, as they were before the merge. */
        volatile long sourceEscapes;

        @Override
        public void message(String component, String message) {
            // "took [slots] of [total] dictionary slots in [runs] ordinal runs"
            if (message.endsWith("ordinal runs")) {
                slots += Long.parseLong(message.substring(message.indexOf('[') + 1, message.indexOf(']')));
            }
        }

        @Override
        public boolean isEnabled(String component) {
            return StringColumnWriter.INFO_STREAM_COMPONENT.equals(component);
        }

        @Override
        public void close() {}
    }

    /** Without an index sort a merge appends each segment whole, so its slots are taken in runs, deletions or not. */
    public void testSegmentsAppendedWholeAreTakenInRuns() throws IOException {
        final int numDocs = between(1000, 3000);
        final RunCount runs = mergeAndCheck(
            randomValues(numDocs),
            between(2, 6),
            null,
            randomBoolean() ? 200 : 0,
            (doc, values) -> doc,
            true
        );
        assertThat(runs.slots, greaterThan(0L));
    }

    /** A format told not to copy runs takes every slot on its own, and the column reads back the same. */
    public void testRunsTurnedOffTakeNothing() throws IOException {
        final RunCount runs = mergeAndCheck(randomValues(between(1000, 3000)), between(2, 6), null, 0, (doc, values) -> doc, false);
        assertEquals(0L, runs.slots);
    }

    /**
     * Under an index sort on the value itself every segment's slots land in term order, interleaved with the other
     * segments'. Runs meet where one segment's slots give way to another's, and the merged column is still recorded as
     * sorted, which is decided at those edges as much as inside a run. Sometimes a document holds a null instead, which
     * sorts first and leaves the column unsorted however its terms fall.
     */
    public void testAColumnSortedOnItsValuesStaysSorted() throws IOException {
        final String[] pool = pool();
        Arrays.sort(pool);
        final String[][] values = new String[between(1000, 3000)][];
        final boolean withNulls = randomBoolean();
        for (int d = 0; d < values.length; d++) {
            values[d] = new String[] { withNulls && random().nextInt(100) == 0 ? null : randomFrom(pool) };
        }
        final RunCount runs = mergeAndCheck(
            values,
            between(2, 6),
            sortOnKey(),
            randomBoolean() ? 200 : 0,
            (doc, v) -> v[doc][0] == null ? -1 : Arrays.binarySearch(pool, v[doc][0]),
            true
        );
        assertThat(runs.slots, greaterThan(0L));
    }

    /**
     * Under an index sort that deals the segments' documents out in turn, no two of a segment's land together, so
     * every run is one document's slots. The column still reads back as written.
     */
    public void testAnInterleavingIndexSortTakesDocumentsOneAtATime() throws IOException {
        final int numSegments = between(2, 6);
        final String[][] values = randomValues(between(1000, 3000));
        final int perSegment = values.length / numSegments;
        final RunCount runs = mergeAndCheck(
            values,
            numSegments,
            sortOnKey(),
            0,
            (doc, v) -> (long) (doc % perSegment) * numSegments + doc / perSegment,
            true
        );
        assertThat(runs.slots, greaterThan(0L));
    }

    /**
     * A segment with a value its dictionary does not name has slots no ordinal stands for, so none of it is taken in a
     * run; the segments without one still are.
     */
    public void testASegmentWithEscapesIsTakenValueByValue() throws IOException {
        final int numSegments = between(2, 6);
        final String[][] values = randomValues(numSegments * between(300, 600));
        final int perSegment = values.length / numSegments;
        // A few values seen once, which a segment's dictionary leaves out.
        final int escaping = between(0, numSegments - 1);
        for (int i = 0; i < 3; i++) {
            final int d = escaping * perSegment + between(0, perSegment - 1);
            values[d] = new String[] { "once-" + d };
        }
        final RunCount runs = mergeAndCheck(values, numSegments, null, 0, (doc, v) -> doc, true);
        assertThat("a segment let a value escape", runs.sourceEscapes, greaterThan(0L));
        assertThat(runs.slots, greaterThan(0L));
    }

    private static Sort sortOnKey() {
        return new Sort(new SortField(SORT, SortField.Type.LONG));
    }

    /** The sort key document {@code doc} is given. */
    private interface SortKey {
        long of(int doc, String[][] values);
    }

    /**
     * Indexes {@code values} over {@code numSegments} segments, deletes about one in {@code deleteEvery} (none at zero),
     * force-merges, checks every surviving document reads back and the column is recorded as sorted exactly when its
     * slots are, and answers how many slots the merge took in runs.
     */
    private RunCount mergeAndCheck(String[][] values, int numSegments, Sort sort, int deleteEvery, SortKey sortKey, boolean copyRuns)
        throws IOException {
        final int perSegment = values.length / numSegments;
        final int numDocs = perSegment * numSegments;
        final boolean[] deleted = new boolean[numDocs];
        final RunCount runs = new RunCount();
        final FieldType type = columnarBinaryFieldType();
        try (Directory dir = newDirectory()) {
            final IndexWriterConfig iwc = new IndexWriterConfig().setCodec(columnarCodecForField(FIELD, format(copyRuns)))
                .setMergePolicy(new LogDocMergePolicy())
                .setInfoStream(runs);
            if (sort != null) {
                iwc.setIndexSort(sort);
            }
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                for (int d = 0; d < numDocs; d++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(ID, d));
                    doc.add(new StringField(ID + "_term", Integer.toString(d), Field.Store.NO));
                    doc.add(new NumericDocValuesField(SORT, sortKey.of(d, values)));
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
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        final StringColumnReader source = reader(leaf.reader());
                        assertTrue("expected a dictionary column in every segment", source.hasDictionary());
                        runs.sourceEscapes += source.escapeCount();
                    }
                }
                // Only the merge counts: the flushes above write their columns value by value.
                runs.slots = 0;
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals("force-merged to one segment", 1, reader.leaves().size());
                assertMerged(reader.leaves().get(0).reader(), values, deleted);
            }
        }
        return runs;
    }

    private static StringColumnReader reader(LeafReader leaf) throws IOException {
        final BinaryDocValues column = leaf.getBinaryDocValues(FIELD);
        assertTrue("expected a columnar column, got " + column, column instanceof ColumnarStringBinaryDocValues);
        return ((ColumnarStringBinaryDocValues) column).reader();
    }

    /**
     * Every surviving document's slots, found by the id each one carries, since a sort reorders them; and the column is
     * recorded as sorted exactly when its slots, read in order, are in term order with no null and no escaped value.
     */
    private static void assertMerged(LeafReader leaf, String[][] values, boolean[] deleted) throws IOException {
        final StringColumnReader merged = reader(leaf);
        assertTrue("expected a dictionary column", merged.hasDictionary());
        final BinaryDocValues column = leaf.getBinaryDocValues(FIELD);
        final NumericDocValues ids = leaf.getNumericDocValues(ID);
        final StringBinaryPayload.Decoder decoder = new StringBinaryPayload.Decoder();
        boolean sorted = merged.escapeCount() == 0;
        String previous = null;
        int seen = 0;
        for (int doc = column.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = column.nextDoc()) {
            assertTrue(ids.advanceExact(doc));
            final int id = (int) ids.longValue();
            assertFalse("document " + id + " was deleted", deleted[id]);
            final List<String> slots = new ArrayList<>();
            for (int slot = decoder.reset(column.binaryValue()); slot > 0; slot--) {
                final BytesRef value = decoder.next();
                final String string = value == null ? null : value.utf8ToString();
                if (string == null || (previous != null && new BytesRef(previous).compareTo(value) > 0)) {
                    sorted = false;
                }
                previous = string;
                slots.add(string);
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
        assertEquals("recorded as sorted", sorted, merged.valuesSorted());
    }

    private static ColumNARDocValuesFormat format(boolean copyRuns) {
        return new ColumNARDocValuesFormat(
            (f, t) -> NumericPipeline::defaultPipeline,
            f -> ColumnarFieldType.STRING,
            ColumNARDocValuesFormat.DEFAULT_BLOCK_SIZE,
            StringColumnOptionsSelector.always(OPTIONS),
            copyRuns
        );
    }

    private static String[] pool() {
        final String[] pool = new String[between(2, 10)];
        for (int i = 0; i < pool.length; i++) {
            pool[i] = randomAlphaOfLength(between(1, 20));
        }
        return pool;
    }

    /** One to three slots a document, often repeating the slot before and sometimes null. */
    private static String[][] randomValues(int numDocs) {
        final String[] pool = pool();
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
