/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es95;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesFormatTests;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Locale;

import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.DEFAULT_SKIP_INDEX_INTERVAL_SIZE;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.NUMERIC_LARGE_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat.ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL;

public class ES95TSDBDocValuesFormatTests extends AbstractTSDBDocValuesFormatTests {

    private final Codec codec = new Elasticsearch93Lucene104Codec() {

        final DocValuesFormat docValuesFormat = new ES95TSDBDocValuesFormat(
            ESTestCase.randomIntBetween(2, 4096),
            ESTestCase.randomIntBetween(1, 512),
            random().nextBoolean(),
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            random().nextBoolean() ? NUMERIC_LARGE_BLOCK_SHIFT : NUMERIC_BLOCK_SHIFT,
            random().nextBoolean(),
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder,
            null
        );

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return docValuesFormat;
        }
    };

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testAddIndices() throws IOException {
        doTestAddIndices(
            List.of(
                new TestES87TSDBDocValuesFormat(random().nextInt(4, 16)),
                new ES819TSDBDocValuesFormat(),
                new ES819Version3TSDBDocValuesFormat(),
                new ES95TSDBDocValuesFormat(),
                new Lucene90DocValuesFormat()
            )
        );
    }

    public void testMonotonicTimestamps() throws IOException {
        final long base = BASE_TIMESTAMP;
        final long interval = 10_000L;
        final int numDocs = 1024;

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES95TSDBDocValuesFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("ts", base + (long) i * interval));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final NumericDocValues ndv = leaf.reader().getNumericDocValues("ts");
                    assertNotNull(ndv);
                    int count = 0;
                    while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                        assertEquals(base + (long) (leaf.docBase + count) * interval, ndv.longValue());
                        count++;
                    }
                    assertEquals(leaf.reader().maxDoc(), count);
                }
            }
        }
    }

    public void testConstantValues() throws IOException {
        final long constantValue = random().nextLong();
        final int numDocs = 1024;

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES95TSDBDocValuesFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("constant", constantValue));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final NumericDocValues ndv = leaf.reader().getNumericDocValues("constant");
                    assertNotNull(ndv);
                    int count = 0;
                    while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                        assertEquals(constantValue, ndv.longValue());
                        count++;
                    }
                    assertEquals(leaf.reader().maxDoc(), count);
                }
            }
        }
    }

    public void testGcdFriendlyValues() throws IOException {
        final long gcd = 1000L;
        final int numDocs = 1024;
        final long[] expected = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            expected[i] = (long) ESTestCase.randomIntBetween(0, 10_000) * gcd;
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES95TSDBDocValuesFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("gcd_field", expected[i]));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final NumericDocValues ndv = leaf.reader().getNumericDocValues("gcd_field");
                    assertNotNull(ndv);
                    int count = 0;
                    while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                        assertEquals(expected[leaf.docBase + count], ndv.longValue());
                        count++;
                    }
                    assertEquals(leaf.reader().maxDoc(), count);
                }
            }
        }
    }

    public void testMergeES819IntoES95() throws IOException {
        final int numDocs = 512;
        final long[] values = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            values[i] = random().nextLong();
        }

        try (Directory dir = newDirectory()) {
            final IndexWriterConfig es819Config = writerConfig(new ES819Version3TSDBDocValuesFormat());
            try (IndexWriter writer = new IndexWriter(dir, es819Config)) {
                for (int i = 0; i < numDocs / 2; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("val", values[i]));
                    writer.addDocument(doc);
                }
                writer.commit();
            }

            final IndexWriterConfig es95Config = writerConfig(new ES95TSDBDocValuesFormat());
            try (IndexWriter writer = new IndexWriter(dir, es95Config)) {
                for (int i = numDocs / 2; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("val", values[i]));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }

            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                final NumericDocValues ndv = reader.leaves().get(0).reader().getNumericDocValues("val");
                assertNotNull(ndv);
                int count = 0;
                while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                    assertEquals(values[count], ndv.longValue());
                    count++;
                }
                assertEquals(numDocs, count);
            }
        }
    }

    public void testBothBlockSizes() throws IOException {
        final int numDocs = 1024;
        final long[] values = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            values[i] = random().nextLong();
        }

        for (int blockShift : new int[] { NUMERIC_BLOCK_SHIFT, NUMERIC_LARGE_BLOCK_SHIFT }) {
            try (Directory dir = newDirectory()) {
                final DocValuesFormat format = new ES95TSDBDocValuesFormat(
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
                    null
                );
                try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        doc.add(new NumericDocValuesField("field", values[i]));
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        final NumericDocValues ndv = leaf.reader().getNumericDocValues("field");
                        assertNotNull(ndv);
                        int count = 0;
                        while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                            assertEquals(
                                "blockShift=" + blockShift + " doc=" + (leaf.docBase + count),
                                values[leaf.docBase + count],
                                ndv.longValue()
                            );
                            count++;
                        }
                        assertEquals(leaf.reader().maxDoc(), count);
                    }
                }
            }
        }
    }

    public void testSortedNumericWithPipeline() throws IOException {
        final int numDocs = 1024;
        final long[][] expected = new long[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            final int valueCount = ESTestCase.randomIntBetween(1, 5);
            expected[i] = new long[valueCount];
            for (int v = 0; v < valueCount; v++) {
                expected[i][v] = random().nextLong();
            }
            java.util.Arrays.sort(expected[i]);
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES95TSDBDocValuesFormat()))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    for (long val : expected[i]) {
                        doc.add(new SortedNumericDocValuesField("multi", val));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final SortedNumericDocValues sndv = leaf.reader().getSortedNumericDocValues("multi");
                    assertNotNull(sndv);
                    int count = 0;
                    while (sndv.nextDoc() != SortedNumericDocValues.NO_MORE_DOCS) {
                        final int docIndex = leaf.docBase + count;
                        assertEquals(expected[docIndex].length, sndv.docValueCount());
                        for (int v = 0; v < sndv.docValueCount(); v++) {
                            assertEquals(expected[docIndex][v], sndv.nextValue());
                        }
                        count++;
                    }
                    assertEquals(leaf.reader().maxDoc(), count);
                }
            }
        }
    }

    public void testPipelinePathIsUsedForNumericFields() throws IOException {
        final DocValuesFormat format = new ES95TSDBDocValuesFormat(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            ESTestCase.randomBoolean() ? NUMERIC_BLOCK_SHIFT : NUMERIC_LARGE_BLOCK_SHIFT,
            false,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            NumericCodecFactory.DEFAULT,
            blockSize -> (input, values, count) -> {
                throw new AssertionError("fallback decoder should not be reached for pipeline-encoded numeric fields");
            },
            null
        );

        final int numDocs = ESTestCase.randomIntBetween(128, 4096);
        final long[] expected = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            expected[i] = ESTestCase.randomLong();
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField("field", expected[i]));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (final LeafReaderContext leaf : reader.leaves()) {
                    final NumericDocValues ndv = leaf.reader().getNumericDocValues("field");
                    int count = 0;
                    while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                        assertEquals(expected[leaf.docBase + count], ndv.longValue());
                        count++;
                    }
                    assertTrue(count > 0);
                }
            }
        }
    }

    public void testSortedBlockSizeRoundTrip() throws IOException {
        // blockSize*2 docs guarantees at least one full block at every block size.
        for (int blockSize : BLOCK_SIZE_SWEEP) {
            final int blockShift = Integer.numberOfTrailingZeros(blockSize);
            final int numDocs = blockSize * 2;
            final String[] docTerms = new String[numDocs];
            for (int i = 0; i < numDocs; i++) {
                docTerms[i] = String.format(Locale.ROOT, "term-%05d", i);
            }
            try (Directory dir = newDirectory()) {
                try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildOrdinalFormat(blockShift)))) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        doc.add(new SortedDocValuesField(ORDINAL_FIELD, new BytesRef(docTerms[i])));
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        assertSortedSequence(leaf, ORDINAL_FIELD, d -> docTerms[d]);
                    }
                }
            }
        }
    }

    public void testSortedBlockSizePartialTrailingBlock() throws IOException {
        // 100 docs leaves a partial trailing block at every block size in the sweep.
        final int numDocs = 100;
        final String[] terms = new String[numDocs];
        for (int i = 0; i < numDocs; i++) {
            terms[i] = String.format(Locale.ROOT, "term-%03d", i);
        }
        for (int blockSize : BLOCK_SIZE_SWEEP) {
            final int blockShift = Integer.numberOfTrailingZeros(blockSize);
            try (Directory dir = newDirectory()) {
                try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildOrdinalFormat(blockShift)))) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        doc.add(new SortedDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i])));
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        assertSortedSequence(leaf, ORDINAL_FIELD, d -> terms[d]);
                    }
                }
            }
        }
    }

    public void testSortedSetBlockSizeSweep() throws IOException {
        // blockSize*2 docs guarantees at least one full block at every block size.
        final int termsPerDoc = 3;
        for (int blockSize : BLOCK_SIZE_SWEEP) {
            final int blockShift = Integer.numberOfTrailingZeros(blockSize);
            final int numDocs = blockSize * 2;
            final String[][] terms = new String[numDocs][termsPerDoc];
            for (int i = 0; i < numDocs; i++) {
                for (int j = 0; j < termsPerDoc; j++) {
                    terms[i][j] = String.format(Locale.ROOT, "term-%05d-%d", i, j);
                }
            }
            try (Directory dir = newDirectory()) {
                try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildOrdinalFormat(blockShift)))) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        for (int j = 0; j < termsPerDoc; j++) {
                            doc.add(new SortedSetDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i][j])));
                        }
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        assertSortedSetSequence(leaf, ORDINAL_FIELD, termsPerDoc, (d, j) -> terms[d][j]);
                    }
                }
            }
        }
    }

    public void testSortedSetBlockSizePartialTrailingBlock() throws IOException {
        // 100 docs leaves a partial trailing block for sorted-set at every block size in the sweep.
        final int numDocs = 100;
        final int termsPerDoc = 3;
        final String[][] terms = new String[numDocs][termsPerDoc];
        for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < termsPerDoc; j++) {
                terms[i][j] = String.format(Locale.ROOT, "term-%03d-%d", i, j);
            }
        }
        for (int blockSize : BLOCK_SIZE_SWEEP) {
            final int blockShift = Integer.numberOfTrailingZeros(blockSize);
            try (Directory dir = newDirectory()) {
                try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildOrdinalFormat(blockShift)))) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        for (int j = 0; j < termsPerDoc; j++) {
                            doc.add(new SortedSetDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i][j])));
                        }
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        assertSortedSetSequence(leaf, ORDINAL_FIELD, termsPerDoc, (d, j) -> terms[d][j]);
                    }
                }
            }
        }
    }

    public void testSortedSetBlockSizeVariableTermsPerDoc() throws IOException {
        for (int blockSize : BLOCK_SIZE_SWEEP) {
            final int blockShift = Integer.numberOfTrailingZeros(blockSize);
            final int numDocs = blockSize * 2;
            final String[][] terms = new String[numDocs][];
            for (int i = 0; i < numDocs; i++) {
                final int count = ESTestCase.randomIntBetween(1, 4);
                terms[i] = new String[count];
                for (int j = 0; j < count; j++) {
                    terms[i][j] = String.format(Locale.ROOT, "term-%05d-%d", i, j);
                }
            }
            try (Directory dir = newDirectory()) {
                try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildOrdinalFormat(blockShift)))) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        for (final String term : terms[i]) {
                            doc.add(new SortedSetDocValuesField(ORDINAL_FIELD, new BytesRef(term)));
                        }
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (final LeafReaderContext leaf : reader.leaves()) {
                        final SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(ORDINAL_FIELD);
                        assertNotNull(ssdv);
                        int docCount = 0;
                        while (ssdv.nextDoc() != SortedSetDocValues.NO_MORE_DOCS) {
                            final int globalDoc = leaf.docBase + docCount;
                            assertEquals(terms[globalDoc].length, ssdv.docValueCount());
                            for (int j = 0; j < terms[globalDoc].length; j++) {
                                assertEquals(new BytesRef(terms[globalDoc][j]), BytesRef.deepCopyOf(ssdv.lookupOrd(ssdv.nextOrd())));
                            }
                            docCount++;
                        }
                        assertEquals(leaf.reader().maxDoc(), docCount);
                    }
                }
            }
        }
    }

    public void testPerFieldSortedBlockSizeRoundTrip() throws IOException {
        for (int customBlockSize : BLOCK_SIZE_SWEEP) {
            doTestPerFieldOrdinalBlockSizeRoundTrip(NUMERIC_BLOCK_SHIFT, customBlockSize, 4096);
        }
    }

    public void testPerFieldSortedBlockSizePartialTrailingBlock() throws IOException {
        for (int customBlockSize : BLOCK_SIZE_SWEEP) {
            doTestPerFieldOrdinalBlockSizeRoundTrip(NUMERIC_BLOCK_SHIFT, customBlockSize, 100);
        }
    }

    public void testPerFieldSortedSetBlockSizeMultiValue() throws IOException {
        final int numDocs = 2048;
        final int termsPerDoc = 3;
        for (int customBlockSize : BLOCK_SIZE_SWEEP) {
            try (Directory dir = newDirectory()) {
                try (
                    IndexWriter writer = new IndexWriter(
                        dir,
                        writerConfig(buildPerFieldBlockSizeFormat(NUMERIC_BLOCK_SHIFT, customBlockSize))
                    )
                ) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        for (int j = 0; j < termsPerDoc; j++) {
                            doc.add(
                                new SortedSetDocValuesField(
                                    CUSTOM_BS_SORTED_FIELD,
                                    new BytesRef(String.format(Locale.ROOT, "term-%05d-%d", i, j))
                                )
                            );
                            doc.add(
                                new SortedSetDocValuesField(
                                    DEFAULT_BS_SORTED_FIELD,
                                    new BytesRef(String.format(Locale.ROOT, "term-%05d-%d", i, j))
                                )
                            );
                        }
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        assertSortedSetSequence(
                            leaf,
                            CUSTOM_BS_SORTED_FIELD,
                            termsPerDoc,
                            (i, j) -> String.format(Locale.ROOT, "term-%05d-%d", i, j)
                        );
                        assertSortedSetSequence(
                            leaf,
                            DEFAULT_BS_SORTED_FIELD,
                            termsPerDoc,
                            (i, j) -> String.format(Locale.ROOT, "term-%05d-%d", i, j)
                        );
                    }
                }
            }
        }
    }

    public void testPerFieldSortedBlockSizeOverridesFormatHeader() throws IOException {
        final int numDocs = 4096;
        final int promotedBlockSize = 1024;
        try (Directory dir = newDirectory()) {
            try (
                IndexWriter writer = new IndexWriter(
                    dir,
                    writerConfig(buildPerFieldBlockSizeFormat(NUMERIC_LARGE_BLOCK_SHIFT, promotedBlockSize))
                )
            ) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(CUSTOM_BS_SORTED_FIELD, new BytesRef(String.format(Locale.ROOT, "term-%05d", i))));
                    doc.add(new SortedDocValuesField(DEMOTED_BS_SORTED_FIELD, new BytesRef(String.format(Locale.ROOT, "term-%05d", i))));
                    doc.add(new SortedDocValuesField(DEFAULT_BS_SORTED_FIELD, new BytesRef(String.format(Locale.ROOT, "term-%05d", i))));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertSortedSequence(leaf, CUSTOM_BS_SORTED_FIELD, i -> String.format(Locale.ROOT, "term-%05d", i));
                    assertSortedSequence(leaf, DEMOTED_BS_SORTED_FIELD, i -> String.format(Locale.ROOT, "term-%05d", i));
                    assertSortedSequence(leaf, DEFAULT_BS_SORTED_FIELD, i -> String.format(Locale.ROOT, "term-%05d", i));
                }
            }
        }
    }

    private void doTestPerFieldOrdinalBlockSizeRoundTrip(int formatShift, int customBlockSize, int numDocs) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildPerFieldBlockSizeFormat(formatShift, customBlockSize)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(CUSTOM_BS_SORTED_FIELD, new BytesRef(String.format(Locale.ROOT, "term-%05d", i))));
                    doc.add(new SortedDocValuesField(DEFAULT_BS_SORTED_FIELD, new BytesRef(String.format(Locale.ROOT, "term-%05d", i))));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertSortedSequence(leaf, CUSTOM_BS_SORTED_FIELD, i -> String.format(Locale.ROOT, "term-%05d", i));
                    assertSortedSequence(leaf, DEFAULT_BS_SORTED_FIELD, i -> String.format(Locale.ROOT, "term-%05d", i));
                }
            }
        }
    }

    public void testPerFieldBlockSizeRoundTrip() throws IOException {
        for (int customBlockSize : BLOCK_SIZE_SWEEP) {
            doTestPerFieldBlockSizeRoundTrip(NUMERIC_BLOCK_SHIFT, customBlockSize, 4096);
        }
    }

    public void testPerFieldBlockSizePartialTrailingBlock() throws IOException {
        // 100 docs leaves a single partial block at every BS in the sweep.
        for (int customBlockSize : BLOCK_SIZE_SWEEP) {
            doTestPerFieldBlockSizeRoundTrip(NUMERIC_BLOCK_SHIFT, customBlockSize, 100);
        }
    }

    public void testPerFieldBlockSizeSortedNumericMultiValue() throws IOException {
        // NOTE: multi-valued sorted numeric flows through getValues() rather than the
        // anonymous iterators in getNumeric(), so it covers a distinct per-entry BS site.
        final int numDocs = 2048;
        final int valuesPerDoc = 3;
        for (int customBlockSize : BLOCK_SIZE_SWEEP) {
            try (Directory dir = newDirectory()) {
                try (
                    IndexWriter writer = new IndexWriter(
                        dir,
                        writerConfig(buildPerFieldBlockSizeFormat(NUMERIC_BLOCK_SHIFT, customBlockSize))
                    )
                ) {
                    for (int i = 0; i < numDocs; i++) {
                        final Document doc = new Document();
                        for (int j = 0; j < valuesPerDoc; j++) {
                            doc.add(new SortedNumericDocValuesField(CUSTOM_BS_FIELD, ((long) i * valuesPerDoc + j) * 23L));
                            doc.add(new SortedNumericDocValuesField(DEFAULT_BS_FIELD, ((long) i * valuesPerDoc + j) * 6L));
                        }
                        writer.addDocument(doc);
                    }
                }
                try (DirectoryReader reader = DirectoryReader.open(dir)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        assertSortedNumericAscending(leaf, CUSTOM_BS_FIELD, valuesPerDoc, (i, j) -> ((long) i * valuesPerDoc + j) * 23L);
                        assertSortedNumericAscending(leaf, DEFAULT_BS_FIELD, valuesPerDoc, (i, j) -> ((long) i * valuesPerDoc + j) * 6L);
                    }
                }
            }
        }
    }

    public void testPerFieldBlockSizeOverridesFormatHeader() throws IOException {
        // NOTE: locks the property that the format-level header byte is not load-bearing
        // for navigation. The format header carries NUMERIC_LARGE_BLOCK_SHIFT (TSDB
        // default = 512), but the resolver promotes one field above the header (BS=1024)
        // and demotes another below it (BS=128). Both fields must round trip at their
        // resolver-assigned BS sourced from the per-field PipelineDescriptor.
        final int numDocs = 4096;
        final int promotedBlockSize = 1024;
        try (Directory dir = newDirectory()) {
            try (
                IndexWriter writer = new IndexWriter(
                    dir,
                    writerConfig(buildPerFieldBlockSizeFormat(NUMERIC_LARGE_BLOCK_SHIFT, promotedBlockSize))
                )
            ) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(CUSTOM_BS_FIELD, (long) i * 17L));
                    doc.add(new NumericDocValuesField(DEMOTED_BS_FIELD, (long) i * 11L));
                    doc.add(new NumericDocValuesField(DEFAULT_BS_FIELD, (long) i * 5L));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertNumericSequence(leaf, CUSTOM_BS_FIELD, i -> (long) i * 17L);
                    assertNumericSequence(leaf, DEMOTED_BS_FIELD, i -> (long) i * 11L);
                    assertNumericSequence(leaf, DEFAULT_BS_FIELD, i -> (long) i * 5L);
                }
            }
        }
    }

    private void doTestPerFieldBlockSizeRoundTrip(int formatShift, int customBlockSize, int numDocs) throws IOException {
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildPerFieldBlockSizeFormat(formatShift, customBlockSize)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(CUSTOM_BS_FIELD, (long) i * 17L));
                    doc.add(new NumericDocValuesField(DEFAULT_BS_FIELD, (long) i * 5L));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertNumericSequence(leaf, CUSTOM_BS_FIELD, i -> (long) i * 17L);
                    assertNumericSequence(leaf, DEFAULT_BS_FIELD, i -> (long) i * 5L);
                }
            }
        }
    }

    @FunctionalInterface
    private interface ExpectedValue {
        long at(int globalIndex);
    }

    @FunctionalInterface
    private interface ExpectedMultiValue {
        long at(int globalDocIndex, int valueIndex);
    }

    @FunctionalInterface
    private interface ExpectedTermAtDoc {
        String at(int globalDocIndex);
    }

    @FunctionalInterface
    private interface ExpectedTerm {
        String at(int globalDocIndex, int valueIndex);
    }

    private static void assertNumericSequence(LeafReaderContext leaf, String field, ExpectedValue expected) throws IOException {
        final NumericDocValues ndv = leaf.reader().getNumericDocValues(field);
        assertNotNull("missing doc values for " + field, ndv);
        int count = 0;
        while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
            assertEquals(
                "field " + field + " mismatch at doc " + (leaf.docBase + count),
                expected.at(leaf.docBase + count),
                ndv.longValue()
            );
            count++;
        }
        assertEquals(leaf.reader().maxDoc(), count);
    }

    private static void assertSortedNumericAscending(LeafReaderContext leaf, String field, int valuesPerDoc, ExpectedMultiValue expected)
        throws IOException {
        // The test writes values in strictly ascending order per doc, so sorted numeric
        // returns them in the same order with no need to pre-sort.
        final SortedNumericDocValues sndv = leaf.reader().getSortedNumericDocValues(field);
        assertNotNull("missing sorted numeric doc values for " + field, sndv);
        int docCount = 0;
        while (sndv.nextDoc() != SortedNumericDocValues.NO_MORE_DOCS) {
            assertEquals(
                "doc value count mismatch on field " + field + " at doc " + (leaf.docBase + docCount),
                valuesPerDoc,
                sndv.docValueCount()
            );
            final int globalDocIndex = leaf.docBase + docCount;
            for (int j = 0; j < valuesPerDoc; j++) {
                assertEquals("field " + field + " doc " + globalDocIndex + " value " + j, expected.at(globalDocIndex, j), sndv.nextValue());
            }
            docCount++;
        }
        assertEquals(leaf.reader().maxDoc(), docCount);
    }

    private static void assertSortedSequence(LeafReaderContext leaf, String field, ExpectedTermAtDoc expected) throws IOException {
        final SortedDocValues sdv = leaf.reader().getSortedDocValues(field);
        assertNotNull("missing sorted doc values for " + field, sdv);
        int count = 0;
        while (sdv.nextDoc() != SortedDocValues.NO_MORE_DOCS) {
            final BytesRef actual = BytesRef.deepCopyOf(sdv.lookupOrd(sdv.ordValue()));
            assertEquals("field " + field + " doc " + (leaf.docBase + count), new BytesRef(expected.at(leaf.docBase + count)), actual);
            count++;
        }
        assertEquals(leaf.reader().maxDoc(), count);
    }

    private static void assertSortedSetSequence(LeafReaderContext leaf, String field, int termsPerDoc, ExpectedTerm expected)
        throws IOException {
        // Terms within each doc are stored and returned in lexicographic order.
        // Test terms are formatted so that j=0..termsPerDoc-1 are already in sorted order.
        final SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(field);
        assertNotNull("missing sorted-set doc values for " + field, ssdv);
        int docCount = 0;
        while (ssdv.nextDoc() != SortedSetDocValues.NO_MORE_DOCS) {
            final int globalDoc = leaf.docBase + docCount;
            assertEquals("doc value count for " + field + " doc " + globalDoc, termsPerDoc, ssdv.docValueCount());
            for (int j = 0; j < termsPerDoc; j++) {
                final long ord = ssdv.nextOrd();
                final BytesRef actual = BytesRef.deepCopyOf(ssdv.lookupOrd(ord));
                assertEquals("field " + field + " doc " + globalDoc + " term " + j, new BytesRef(expected.at(globalDoc, j)), actual);
            }
            docCount++;
        }
        assertEquals(leaf.reader().maxDoc(), docCount);
    }

    private static DocValuesFormat buildOrdinalFormat(int blockShift) {
        return new ES95TSDBDocValuesFormat(
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
            null
        );
    }

    private static DocValuesFormat buildPerFieldBlockSizeFormat(int formatShift, int customBlockSize) {
        final FieldContextResolver perFieldResolver = (fieldName, defaultBlockSize) -> {
            final int blockSize;
            if (CUSTOM_BS_FIELD.equals(fieldName) || CUSTOM_BS_SORTED_FIELD.equals(fieldName)) {
                blockSize = customBlockSize;
            } else if (DEMOTED_BS_FIELD.equals(fieldName) || DEMOTED_BS_SORTED_FIELD.equals(fieldName)) {
                blockSize = 128;
            } else {
                blockSize = defaultBlockSize;
            }
            return new FieldContext(blockSize, fieldName, null, null, null, false);
        };
        return new ES95TSDBDocValuesFormat(
            DEFAULT_SKIP_INDEX_INTERVAL_SIZE,
            ORDINAL_RANGE_ENCODING_MIN_DOC_PER_ORDINAL,
            true,
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            formatShift,
            false,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_BYTES_THRESHOLD_DEFAULT,
            ES95TSDBDocValuesFormat.BINARY_DV_BLOCK_COUNT_THRESHOLD_DEFAULT,
            NumericCodecFactory.DEFAULT,
            ES95NumericFieldReader::defaultFallbackDecoder,
            perFieldResolver
        );
    }

    private static final int[] BLOCK_SIZE_SWEEP = { 128, 256, 512, 1024, 2048, 4096 };
    private static final String CUSTOM_BS_FIELD = "custom_bs_field";
    private static final String DEFAULT_BS_FIELD = "default_bs_field";
    private static final String DEMOTED_BS_FIELD = "demoted_bs_field";
    private static final String ORDINAL_FIELD = "keyword";
    private static final String CUSTOM_BS_SORTED_FIELD = "custom_bs_sorted";
    private static final String DEFAULT_BS_SORTED_FIELD = "default_bs_sorted";
    private static final String DEMOTED_BS_SORTED_FIELD = "demoted_bs_sorted";

    private static IndexWriterConfig writerConfig(final DocValuesFormat format) {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(TestUtil.alwaysDocValuesFormat(format));
        config.setMergePolicy(new LogByteSizeMergePolicy());
        return config;
    }
}
