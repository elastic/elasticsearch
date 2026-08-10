/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.DEFAULT_SKIP_INDEX_INTERVAL_SIZE;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL;

/** End-to-end tests for the run-table SortedSet ordinal layout wired into the ES95 format behind the setting. */
public class RunTableSortedSetCodecTests extends ESTestCase {

    private static final String DIM_FIELD = "dim";

    public void testDenseSortedSetRunTableRoundTrip() throws IOException {
        // Piecewise-constant multi-valued sets per series, run length well above the average-of-two bar.
        final int runLength = randomIntBetween(8, 64);
        final int numSeries = randomIntBetween(4, 32);
        final int numDocs = runLength * numSeries;
        final String[][] termsByDoc = piecewiseConstantSets(numDocs, runLength);

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(document(termsByDoc[i]));
                    // Force two segments so the merge path re-encodes through the run-table selection.
                    if (i == numDocs / 2) {
                        writer.commit();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                final LeafReaderContext leaf = reader.leaves().get(0);
                assertSetValues(leaf, termsByDoc);
                assertEquals(distinctTerms(termsByDoc).size(), leaf.reader().getSortedSetDocValues(DIM_FIELD).getValueCount());
                assertTermsDictSorted(leaf);
            }
        }

        // The merge must re-select the run-table layout: the merged segment must be smaller than the default.
        final long runTableBytes = docValuesBytes(termsByDoc, runTableFormat());
        final long defaultBytes = docValuesBytes(termsByDoc, new ES819TSDBDocValuesFormat());
        assertTrue(
            "run-table bytes " + runTableBytes + " should be smaller than default bytes " + defaultBytes,
            runTableBytes < defaultBytes
        );
    }

    public void testSparseContiguousSortedSetRunTableRoundTrip() throws IOException {
        // Whole series spans are absent (contiguous, empty set), the shape run-table sparse support targets.
        final int runLength = randomIntBetween(8, 64);
        final int numSeries = randomIntBetween(6, 32);
        final int numDocs = runLength * numSeries;
        final String[][] termsByDoc = new String[numDocs][];
        for (int s = 0; s < numSeries; s++) {
            // Every third series is entirely absent, so empty sets come in contiguous runLength-long spans.
            final String[] set = s % 3 == 1
                ? new String[0]
                : new String[] { String.format(Locale.ROOT, "series-%05d-a", s), String.format(Locale.ROOT, "series-%05d-b", s) };
            for (int i = 0; i < runLength; i++) {
                termsByDoc[s * runLength + i] = set;
            }
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(document(termsByDoc[i]));
                    if (i == numDocs / 2) {
                        writer.commit();
                    }
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                final LeafReaderContext leaf = reader.leaves().get(0);
                assertSetValues(leaf, termsByDoc);
                final SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(DIM_FIELD);
                assertEquals(distinctTerms(termsByDoc).size(), ssdv.getValueCount());
                assertTermsDictSorted(leaf);
            }
        }

        final long runTableBytes = docValuesBytes(termsByDoc, runTableFormat());
        final long defaultBytes = docValuesBytes(termsByDoc, new ES819TSDBDocValuesFormat());
        assertTrue(
            "sparse run-table bytes " + runTableBytes + " should be smaller than default bytes " + defaultBytes,
            runTableBytes < defaultBytes
        );
    }

    public void testScatteredSparseSortedSetFallsBackAndRoundTrips() throws IOException {
        // Random per-doc sets make runs approach docs, so the writer delegates to the default layout; it must
        // still round-trip through the standard multi-valued SortedSetDocValues.
        final int numDocs = randomIntBetween(512, 2048);
        final String[][] termsByDoc = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                final TreeSet<String> set = new TreeSet<>();
                final int size = randomIntBetween(1, 3);
                for (int j = 0; j < size; j++) {
                    set.add(String.format(Locale.ROOT, "term-%02d", randomIntBetween(0, 15)));
                }
                termsByDoc[i] = set.toArray(new String[0]);
            } else {
                termsByDoc[i] = new String[0];
            }
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(document(termsByDoc[i]));
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (final LeafReaderContext leaf : reader.leaves()) {
                    assertSetValues(leaf, termsByDoc);
                }
            }
        }
    }

    public void testDegenerateChurnSortedSetFallsBackAndRoundTrips() throws IOException {
        // A distinct set per doc means runs == docs, below the selection bar; the writer must delegate.
        final int numDocs = randomIntBetween(256, 1024);
        final String[][] termsByDoc = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            termsByDoc[i] = new String[] { String.format(Locale.ROOT, "term-%05d-a", i), String.format(Locale.ROOT, "term-%05d-b", i) };
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    writer.addDocument(document(termsByDoc[i]));
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (final LeafReaderContext leaf : reader.leaves()) {
                    assertSetValues(leaf, termsByDoc);
                }
            }
        }

        // Fallback adds the discriminator byte but never produces a smaller file than the default layout.
        final long runTableBytes = docValuesBytes(termsByDoc, runTableFormat());
        final long defaultBytes = docValuesBytes(termsByDoc, new ES819TSDBDocValuesFormat());
        assertTrue("churn fallback " + runTableBytes + " must be >= default " + defaultBytes, runTableBytes >= defaultBytes);
    }

    public void testRunTableDropsAddressesTableAtScale() throws IOException {
        // Same run set, more docs per run: the run-table layout stores one entry per run, so once the
        // per-doc addresses table is dropped it carries no O(docs) structure and stays flat as docs grow,
        // while the default layout (per-doc ordinals plus the addresses table) grows with the doc count.
        // Set sizes vary run to run so the default addresses table costs real bits per doc.
        final int numSeries = 64;
        final String[][] small = scaledSets(numSeries, 4); // 256 docs, 64 runs
        final String[][] large = scaledSets(numSeries, 400); // 25600 docs, 64 runs

        final long runTableSmall = docValuesBytes(small, runTableFormat());
        final long runTableLarge = docValuesBytes(large, runTableFormat());
        final long defaultSmall = docValuesBytes(small, new ES819TSDBDocValuesFormat());
        final long defaultLarge = docValuesBytes(large, new ES819TSDBDocValuesFormat());

        final long runTableGrowth = runTableLarge - runTableSmall;
        final long defaultGrowth = defaultLarge - defaultSmall;

        // The default layout grows sharply as docs-per-series rise while the run set stays fixed.
        assertTrue("default layout grew by only " + defaultGrowth + " bytes", defaultGrowth > 5000);
        // The run-table layout must stay flat in the doc count: its growth is a small fraction of the
        // default's. A remaining O(docs) addresses table would make this growth track the default's.
        assertTrue("run-table grew by " + runTableGrowth + " bytes vs default " + defaultGrowth, runTableGrowth * 10 < defaultGrowth);
        // At scale the multi-valued run-table field is dramatically smaller than the default layout.
        assertTrue("run-table " + runTableLarge + " should be far below default " + defaultLarge, runTableLarge * 3 < defaultLarge);

        // The merged, addresses-free run-table field still round-trips.
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAll(dir, runTableFormat(), large);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                assertSetValues(reader.leaves().get(0), large);
            }
        }
    }

    public void testDuelAgainstES819() throws IOException {
        // The run-table-written field must read back identical values to the same data written by ES819.
        final int runLength = randomIntBetween(8, 32);
        final int numSeries = randomIntBetween(8, 32);
        final int numDocs = runLength * numSeries;
        final String[][] termsByDoc = piecewiseConstantSets(numDocs, runLength);

        try (Directory runTableDir = new ByteBuffersDirectory(); Directory es819Dir = new ByteBuffersDirectory()) {
            writeAll(runTableDir, runTableFormat(), termsByDoc);
            writeAll(es819Dir, new ES819TSDBDocValuesFormat(), termsByDoc);

            try (DirectoryReader runTable = DirectoryReader.open(runTableDir); DirectoryReader es819 = DirectoryReader.open(es819Dir)) {
                final SortedSetDocValues a = runTable.leaves().get(0).reader().getSortedSetDocValues(DIM_FIELD);
                final SortedSetDocValues b = es819.leaves().get(0).reader().getSortedSetDocValues(DIM_FIELD);
                assertNotNull(a);
                assertNotNull(b);
                for (int doc = 0; doc < numDocs; doc++) {
                    assertTrue(a.advanceExact(doc));
                    assertTrue(b.advanceExact(doc));
                    assertEquals("doc " + doc + " count", b.docValueCount(), a.docValueCount());
                    for (int i = 0; i < b.docValueCount(); i++) {
                        assertEquals(
                            "doc " + doc + " ord " + i,
                            BytesRef.deepCopyOf(b.lookupOrd(b.nextOrd())),
                            BytesRef.deepCopyOf(a.lookupOrd(a.nextOrd()))
                        );
                    }
                }
            }
        }
    }

    private static String[][] piecewiseConstantSets(int numDocs, int runLength) {
        final String[][] termsByDoc = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            final int series = i / runLength;
            // Set size 2-4, deterministic per series so runs are piecewise-constant.
            final int size = 2 + (series % 3);
            final String[] set = new String[size];
            for (int j = 0; j < size; j++) {
                set[j] = String.format(Locale.ROOT, "series-%05d-%d", series, j);
            }
            termsByDoc[i] = set;
        }
        return termsByDoc;
    }

    private static String[][] scaledSets(int numSeries, int runLength) {
        final int numDocs = numSeries * runLength;
        final String[][] termsByDoc = new String[numDocs][];
        for (int s = 0; s < numSeries; s++) {
            // Set size 1-8 varies per series, so per-doc value counts differ run to run and the default
            // addresses table (cumulative counts) needs non-trivial bits per doc.
            final int size = 1 + (s % 8);
            final String[] set = new String[size];
            for (int j = 0; j < size; j++) {
                set[j] = String.format(Locale.ROOT, "series-%05d-%02d", s, j);
            }
            for (int i = 0; i < runLength; i++) {
                termsByDoc[s * runLength + i] = set;
            }
        }
        return termsByDoc;
    }

    private static Document document(final String[] terms) {
        final Document doc = new Document();
        for (final String term : terms) {
            doc.add(new SortedSetDocValuesField(DIM_FIELD, new BytesRef(term)));
        }
        return doc;
    }

    private static Set<String> distinctTerms(final String[][] termsByDoc) {
        final Set<String> distinct = new HashSet<>();
        for (final String[] set : termsByDoc) {
            for (final String term : set) {
                distinct.add(term);
            }
        }
        return distinct;
    }

    private static void writeAll(final Directory dir, final DocValuesFormat format, final String[][] termsByDoc) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
            for (final String[] terms : termsByDoc) {
                writer.addDocument(document(terms));
            }
            writer.forceMerge(1);
        }
    }

    private static long docValuesBytes(final String[][] termsByDoc, final DocValuesFormat format) throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAll(dir, format, termsByDoc);
            long total = 0;
            for (final String name : dir.listAll()) {
                if (name.endsWith(".dvd") || name.endsWith(".dvm")) {
                    total += dir.fileLength(name);
                }
            }
            return total;
        }
    }

    private static void assertSetValues(final LeafReaderContext leaf, final String[][] termsByDoc) throws IOException {
        final SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(DIM_FIELD);
        assertNotNull(ssdv);
        for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
            final String[] expected = expectedSorted(termsByDoc[leaf.docBase + doc]);
            if (expected.length == 0) {
                assertFalse("doc " + (leaf.docBase + doc) + " must be absent", ssdv.advanceExact(doc));
            } else {
                assertTrue("doc " + (leaf.docBase + doc), ssdv.advanceExact(doc));
                assertEquals("doc " + (leaf.docBase + doc) + " count", expected.length, ssdv.docValueCount());
                long previous = -1;
                for (int i = 0; i < expected.length; i++) {
                    final long ord = ssdv.nextOrd();
                    assertTrue("ords must be strictly ascending", ord > previous);
                    previous = ord;
                    assertEquals(new BytesRef(expected[i]), BytesRef.deepCopyOf(ssdv.lookupOrd(ord)));
                }
            }
        }
    }

    private static String[] expectedSorted(final String[] terms) {
        final TreeSet<String> sorted = new TreeSet<>();
        for (final String term : terms) {
            sorted.add(term);
        }
        return sorted.toArray(new String[0]);
    }

    private static void assertTermsDictSorted(final LeafReaderContext leaf) throws IOException {
        final SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(DIM_FIELD);
        assertNotNull(ssdv);
        BytesRef previous = null;
        for (long ord = 0; ord < ssdv.getValueCount(); ord++) {
            final BytesRef term = BytesRef.deepCopyOf(ssdv.lookupOrd(ord));
            if (previous != null) {
                assertTrue("terms dictionary must be sorted at ord " + ord, previous.compareTo(term) < 0);
            }
            previous = term;
        }
    }

    private static DocValuesFormat runTableFormat() {
        return new ES95RunTableTSDBDocValuesFormat(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            NUMERIC_BLOCK_SHIFT,
            false,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder,
            (name, bs) -> new FieldContext(bs, name, null, null, true)
        );
    }

    private static IndexWriterConfig writerConfig(final DocValuesFormat format) {
        return RunTableCodecTestUtils.writerConfig(format);
    }
}
