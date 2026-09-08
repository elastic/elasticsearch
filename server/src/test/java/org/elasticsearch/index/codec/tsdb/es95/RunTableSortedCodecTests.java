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
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedDocValues;
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

import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.DEFAULT_SKIP_INDEX_INTERVAL_SIZE;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL;

/** End-to-end tests for the run-table Sorted ordinal layout wired into the ES95 format behind the setting. */
public class RunTableSortedCodecTests extends ESTestCase {

    private static final String DIM_FIELD = "dim";

    public void testDenseSortedRunTableRoundTrip() throws IOException {
        // Piecewise-constant, multiple series, run length well above the average-of-two selection bar.
        final int runLength = randomIntBetween(8, 64);
        final int numSeries = randomIntBetween(4, 32);
        final int numDocs = runLength * numSeries;
        final String[] termByDoc = piecewiseConstant(numDocs, runLength);

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat(NUMERIC_BLOCK_SHIFT)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(DIM_FIELD, new BytesRef(termByDoc[i])));
                    writer.addDocument(doc);
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
                assertSortedValues(leaf, termByDoc);
                assertTermsDictSorted(leaf);
            }
        }

        // The merge must re-select the run-table layout: the merged segment must be smaller than the default.
        final long runTableBytes = docValuesBytes(termByDoc, runTableFormat(NUMERIC_BLOCK_SHIFT));
        final long defaultBytes = docValuesBytes(termByDoc, new ES819TSDBDocValuesFormat());
        assertTrue(
            "merged run-table bytes " + runTableBytes + " must be smaller than default " + defaultBytes,
            runTableBytes < defaultBytes
        );
    }

    public void testSingleSeriesRunTableRoundTrip() throws IOException {
        final int numDocs = randomIntBetween(64, 512);
        final String[] termByDoc = piecewiseConstant(numDocs, numDocs);

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat(NUMERIC_BLOCK_SHIFT)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(DIM_FIELD, new BytesRef(termByDoc[i])));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                assertSortedValues(reader.leaves().get(0), termByDoc);
            }
        }
    }

    public void testSparseContiguousRunTableRoundTrip() throws IOException {
        // Whole series spans are absent (contiguous), the shape run-table sparse support targets. The layout
        // must round-trip: absent docs report no value, present docs read the right term, and the terms
        // dictionary holds only the real values (getValueCount == distinct present terms, no sentinel).
        final int runLength = randomIntBetween(8, 64);
        final int numSeries = randomIntBetween(6, 32);
        final int numDocs = runLength * numSeries;
        final String[] termByDoc = new String[numDocs];
        final Set<String> distinctTerms = new HashSet<>();
        for (int s = 0; s < numSeries; s++) {
            // Every third series is entirely absent, so nulls come in contiguous runLength-long spans.
            final String term = s % 3 == 1 ? null : String.format(Locale.ROOT, "series-%05d", s);
            for (int i = 0; i < runLength; i++) {
                termByDoc[s * runLength + i] = term;
            }
            if (term != null) {
                distinctTerms.add(term);
            }
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat(NUMERIC_BLOCK_SHIFT)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    if (termByDoc[i] != null) {
                        doc.add(new SortedDocValuesField(DIM_FIELD, new BytesRef(termByDoc[i])));
                    }
                    writer.addDocument(doc);
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
                assertSparseValues(leaf, termByDoc);
                final SortedDocValues sdv = leaf.reader().getSortedDocValues(DIM_FIELD);
                assertEquals(distinctTerms.size(), sdv.getValueCount());
                assertTermsDictSorted(leaf);
            }
        }

        // The run-table layout must actually be selected: it is strictly smaller than the default layout.
        final long runTableBytes = docValuesBytes(termByDoc, runTableFormat(NUMERIC_BLOCK_SHIFT));
        final long defaultBytes = docValuesBytes(termByDoc, new ES819TSDBDocValuesFormat());
        assertTrue(
            "sparse run-table bytes " + runTableBytes + " should be smaller than default bytes " + defaultBytes,
            runTableBytes < defaultBytes
        );
    }

    public void testScatteredSparseFallsBackAndRoundTrips() throws IOException {
        // Random per-doc presence makes runs approach docs, so the writer delegates to the default layout; it
        // must still round-trip through the standard sparse SortedDocValues.
        final int numDocs = randomIntBetween(512, 2048);
        final String[] termByDoc = new String[numDocs];
        for (int i = 0; i < numDocs; i++) {
            if (randomBoolean()) {
                termByDoc[i] = String.format(Locale.ROOT, "term-%02d", randomIntBetween(0, 15));
            }
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat(NUMERIC_BLOCK_SHIFT)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    if (termByDoc[i] != null) {
                        doc.add(new SortedDocValuesField(DIM_FIELD, new BytesRef(termByDoc[i])));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (final LeafReaderContext leaf : reader.leaves()) {
                    assertSparseValues(leaf, termByDoc);
                }
            }
        }
    }

    public void testDegenerateChurnFallsBackAndRoundTrips() throws IOException {
        // A distinct ordinal per doc means runs == docs, below the selection bar; the writer must delegate.
        final int numDocs = randomIntBetween(256, 1024);
        final String[] termByDoc = new String[numDocs];
        for (int i = 0; i < numDocs; i++) {
            termByDoc[i] = String.format(Locale.ROOT, "term-%05d", i);
        }

        try (Directory dir = new ByteBuffersDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(runTableFormat(NUMERIC_BLOCK_SHIFT)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(DIM_FIELD, new BytesRef(termByDoc[i])));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (final LeafReaderContext leaf : reader.leaves()) {
                    assertSortedValues(leaf, termByDoc);
                }
            }
        }

        // Fallback adds the discriminator byte but never produces a smaller file than the default layout.
        final long runTableBytes = docValuesBytes(termByDoc, runTableFormat(NUMERIC_BLOCK_SHIFT));
        final long defaultBytes = docValuesBytes(termByDoc, new ES819TSDBDocValuesFormat());
        assertTrue("churn fallback " + runTableBytes + " must be >= default " + defaultBytes, runTableBytes >= defaultBytes);
    }

    public void testRunTableSmallerThanDefaultLayout() throws IOException {
        // Same piecewise-constant dimension written both ways; the run-table doc-values files must be smaller.
        final int runLength = 32;
        final int numSeries = 64;
        final int numDocs = runLength * numSeries;
        final String[] termByDoc = piecewiseConstant(numDocs, runLength);

        final long runTableBytes = docValuesBytes(termByDoc, runTableFormat(NUMERIC_BLOCK_SHIFT));
        final long defaultBytes = docValuesBytes(termByDoc, new ES819TSDBDocValuesFormat());
        assertTrue(
            "run-table bytes " + runTableBytes + " should be smaller than default bytes " + defaultBytes,
            runTableBytes < defaultBytes
        );
    }

    public void testDuelAgainstES819() throws IOException {
        // The run-table-written field must read back identical values to the same data written by ES819.
        final int runLength = randomIntBetween(8, 32);
        final int numSeries = randomIntBetween(8, 32);
        final int numDocs = runLength * numSeries;
        final String[] termByDoc = piecewiseConstant(numDocs, runLength);

        try (Directory runTableDir = new ByteBuffersDirectory(); Directory es819Dir = new ByteBuffersDirectory()) {
            writeAll(runTableDir, runTableFormat(NUMERIC_BLOCK_SHIFT), termByDoc);
            writeAll(es819Dir, new ES819TSDBDocValuesFormat(), termByDoc);

            try (DirectoryReader runTable = DirectoryReader.open(runTableDir); DirectoryReader es819 = DirectoryReader.open(es819Dir)) {
                final SortedDocValues a = runTable.leaves().get(0).reader().getSortedDocValues(DIM_FIELD);
                final SortedDocValues b = es819.leaves().get(0).reader().getSortedDocValues(DIM_FIELD);
                assertNotNull(a);
                assertNotNull(b);
                for (int doc = 0; doc < numDocs; doc++) {
                    assertTrue(a.advanceExact(doc));
                    assertTrue(b.advanceExact(doc));
                    assertEquals(
                        "doc " + doc,
                        BytesRef.deepCopyOf(b.lookupOrd(b.ordValue())),
                        BytesRef.deepCopyOf(a.lookupOrd(a.ordValue()))
                    );
                }
            }
        }
    }

    private static String[] piecewiseConstant(int numDocs, int runLength) {
        final String[] termByDoc = new String[numDocs];
        for (int i = 0; i < numDocs; i++) {
            termByDoc[i] = String.format(Locale.ROOT, "series-%05d", i / runLength);
        }
        return termByDoc;
    }

    private static void writeAll(final Directory dir, final DocValuesFormat format, final String[] termByDoc) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
            for (final String term : termByDoc) {
                final Document doc = new Document();
                if (term != null) {
                    doc.add(new SortedDocValuesField(DIM_FIELD, new BytesRef(term)));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static long docValuesBytes(final String[] termByDoc, final DocValuesFormat format) throws IOException {
        try (Directory dir = new ByteBuffersDirectory()) {
            writeAll(dir, format, termByDoc);
            long total = 0;
            for (final String name : dir.listAll()) {
                if (name.endsWith(".dvd") || name.endsWith(".dvm")) {
                    total += dir.fileLength(name);
                }
            }
            return total;
        }
    }

    private static void assertSortedValues(final LeafReaderContext leaf, final String[] termByDoc) throws IOException {
        final SortedDocValues sdv = leaf.reader().getSortedDocValues(DIM_FIELD);
        assertNotNull(sdv);
        int count = 0;
        while (sdv.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
            final BytesRef actual = BytesRef.deepCopyOf(sdv.lookupOrd(sdv.ordValue()));
            assertEquals("doc " + (leaf.docBase + count), new BytesRef(termByDoc[leaf.docBase + count]), actual);
            count++;
        }
        assertEquals(leaf.reader().maxDoc(), count);
    }

    private static void assertSparseValues(final LeafReaderContext leaf, final String[] termByDoc) throws IOException {
        final SortedDocValues sdv = leaf.reader().getSortedDocValues(DIM_FIELD);
        assertNotNull(sdv);
        for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
            final String expected = termByDoc[leaf.docBase + doc];
            if (expected == null) {
                assertFalse("doc " + (leaf.docBase + doc), sdv.advanceExact(doc));
            } else {
                assertTrue("doc " + (leaf.docBase + doc), sdv.advanceExact(doc));
                assertEquals(new BytesRef(expected), BytesRef.deepCopyOf(sdv.lookupOrd(sdv.ordValue())));
            }
        }
    }

    private static void assertTermsDictSorted(final LeafReaderContext leaf) throws IOException {
        final SortedDocValues sdv = leaf.reader().getSortedDocValues(DIM_FIELD);
        assertNotNull(sdv);
        BytesRef previous = null;
        for (int ord = 0; ord < sdv.getValueCount(); ord++) {
            final BytesRef term = BytesRef.deepCopyOf(sdv.lookupOrd(ord));
            if (previous != null) {
                assertTrue("terms dictionary must be sorted at ord " + ord, previous.compareTo(term) < 0);
            }
            previous = term;
        }
    }

    private static DocValuesFormat runTableFormat(int blockShift) {
        return new ES95RunTableTSDBDocValuesFormat(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            blockShift,
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
