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
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesFormatTests;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819Version3TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContext;
import org.elasticsearch.index.codec.tsdb.pipeline.FieldContextResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.MetricRole;
import org.elasticsearch.index.codec.tsdb.pipeline.PipelineDescriptor.DataType;
import org.elasticsearch.index.codec.tsdb.pipeline.StaticPipelineConfigResolver;
import org.elasticsearch.index.codec.tsdb.pipeline.numeric.NumericCodecFactory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
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

    public void testMergeES819IntoES95Sorted() throws IOException {
        final int numDocs = ESTestCase.randomIntBetween(1024, 4096);
        final String[] terms = new String[numDocs];
        for (int i = 0; i < numDocs; i++) {
            terms[i] = String.format(Locale.ROOT, "term-%05d", i);
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES819Version3TSDBDocValuesFormat()))) {
                for (int i = 0; i < numDocs / 2; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i])));
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES95TSDBDocValuesFormat()))) {
                for (int i = numDocs / 2; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new SortedDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i])));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                assertSortedSequence(reader.leaves().get(0), ORDINAL_FIELD, d -> terms[d]);
            }
        }
    }

    public void testMergeES819IntoES95SortedSet() throws IOException {
        final int numDocs = ESTestCase.randomIntBetween(1024, 4096);
        final int termsPerDoc = 3;
        final String[][] terms = new String[numDocs][termsPerDoc];
        for (int i = 0; i < numDocs; i++) {
            for (int j = 0; j < termsPerDoc; j++) {
                terms[i][j] = String.format(Locale.ROOT, "term-%05d-%d", i, j);
            }
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES819Version3TSDBDocValuesFormat()))) {
                for (int i = 0; i < numDocs / 2; i++) {
                    final Document doc = new Document();
                    for (int j = 0; j < termsPerDoc; j++) {
                        doc.add(new SortedSetDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i][j])));
                    }
                    writer.addDocument(doc);
                }
                writer.commit();
            }
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(new ES95TSDBDocValuesFormat()))) {
                for (int i = numDocs / 2; i < numDocs; i++) {
                    final Document doc = new Document();
                    for (int j = 0; j < termsPerDoc; j++) {
                        doc.add(new SortedSetDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i][j])));
                    }
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                assertEquals(1, reader.leaves().size());
                assertSortedSetSequence(reader.leaves().get(0), ORDINAL_FIELD, termsPerDoc, (d, j) -> terms[d][j]);
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
            Arrays.sort(expected[i]);
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

    public void testSparseNumericWithPipeline() throws IOException {
        final int numDocs = ESTestCase.randomIntBetween(1024, 4096);
        final long[] expected = new long[numDocs];
        final boolean[] present = new boolean[numDocs];
        for (int i = 0; i < numDocs; i++) {
            // NOTE: leave every third doc without a value to exercise the sparse IndexedDISI path.
            if (i % 3 != 0) {
                present[i] = true;
                expected[i] = random().nextLong();
            }
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildPerFieldBlockSizeFormat(randomBlockShift(), 256)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    if (present[i]) {
                        doc.add(new NumericDocValuesField("sparse_numeric", expected[i]));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final NumericDocValues ndv = leaf.reader().getNumericDocValues("sparse_numeric");
                    assertNotNull(ndv);
                    for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
                        final int globalDoc = leaf.docBase + docId;
                        if (present[globalDoc]) {
                            assertTrue(ndv.advanceExact(docId));
                            assertEquals(expected[globalDoc], ndv.longValue());
                        } else {
                            assertFalse(ndv.advanceExact(docId));
                        }
                    }
                }
            }
        }
    }

    public void testSparseSortedNumericWithPipeline() throws IOException {
        final int numDocs = ESTestCase.randomIntBetween(1024, 4096);
        final long[][] expected = new long[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            // NOTE: leave every third doc without a value to exercise the sparse IndexedDISI path.
            if (i % 3 == 0) {
                expected[i] = new long[0];
            } else {
                final int valueCount = ESTestCase.randomIntBetween(1, 4);
                expected[i] = new long[valueCount];
                for (int v = 0; v < valueCount; v++) {
                    expected[i][v] = random().nextLong();
                }
                Arrays.sort(expected[i]);
            }
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildPerFieldBlockSizeFormat(randomBlockShift(), 256)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    for (long val : expected[i]) {
                        doc.add(new SortedNumericDocValuesField("sparse_multi", val));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    final SortedNumericDocValues sndv = leaf.reader().getSortedNumericDocValues("sparse_multi");
                    assertNotNull(sndv);
                    for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
                        final long[] docExpected = expected[leaf.docBase + docId];
                        final boolean present = sndv.advanceExact(docId);
                        if (docExpected.length == 0) {
                            assertFalse(present);
                        } else {
                            assertTrue(present);
                            assertEquals(docExpected.length, sndv.docValueCount());
                            for (int v = 0; v < docExpected.length; v++) {
                                assertEquals(docExpected[v], sndv.nextValue());
                            }
                        }
                    }
                }
            }
        }
    }

    public void testSparseSortedRoundTrip() throws IOException {
        final int blockShift = randomBlockShift();
        final int numDocs = (1 << blockShift) * 2 + ESTestCase.randomIntBetween(0, 1 << blockShift);
        final String[] terms = new String[numDocs];
        for (int i = 0; i < numDocs; i++) {
            // NOTE: leave every third doc without a value to exercise the sparse presence path.
            if (i % 3 != 0) {
                terms[i] = String.format(Locale.ROOT, "term-%05d", i);
            }
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildOrdinalFormat(blockShift)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    if (terms[i] != null) {
                        doc.add(new SortedDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i])));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertSparseSorted(leaf, ORDINAL_FIELD, g -> g % 3 != 0, d -> terms[d]);
                }
            }
        }
    }

    public void testSparseSortedSetRoundTrip() throws IOException {
        final int blockShift = randomBlockShift();
        final int numDocs = (1 << blockShift) * 2 + ESTestCase.randomIntBetween(0, 1 << blockShift);
        final int termsPerDoc = 3;
        final String[][] terms = new String[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            // NOTE: leave every third doc without a value to exercise the sparse presence path.
            if (i % 3 != 0) {
                terms[i] = new String[termsPerDoc];
                for (int j = 0; j < termsPerDoc; j++) {
                    terms[i][j] = String.format(Locale.ROOT, "term-%05d-%d", i, j);
                }
            }
        }

        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(buildOrdinalFormat(blockShift)))) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    if (terms[i] != null) {
                        for (int j = 0; j < termsPerDoc; j++) {
                            doc.add(new SortedSetDocValuesField(ORDINAL_FIELD, new BytesRef(terms[i][j])));
                        }
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertSparseSortedSet(leaf, ORDINAL_FIELD, g -> g % 3 != 0, termsPerDoc, (d, j) -> terms[d][j]);
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

    public void testDoubleGaugeRoundTripThroughFormat() throws IOException {
        final int blockShift = randomBlockShift();
        assertResolvesTo("alpDouble>delta>offset>gcd>bitPack", DOUBLE_GAUGE_FIELD, blockShift, DataType.DOUBLE, MetricRole.GAUGE);
        assertDoubleRoundTrip(DOUBLE_GAUGE_FIELD, blockShift, gaugeDoubles(blockShift));
    }

    public void testDoubleCounterRoundTripThroughFormat() throws IOException {
        final int blockShift = randomBlockShift();
        assertResolvesTo(
            "alpDouble>splitDelta>delta>offset>gcd>bitPack",
            DOUBLE_COUNTER_FIELD,
            blockShift,
            DataType.DOUBLE,
            MetricRole.COUNTER
        );
        assertDoubleRoundTrip(DOUBLE_COUNTER_FIELD, blockShift, counterDoubles(blockShift));
    }

    public void testLongCounterSplitDeltaRoundTripThroughFormat() throws IOException {
        final int blockShift = randomBlockShift();
        assertResolvesTo("splitDelta>delta>offset>gcd>bitPack", LONG_COUNTER_FIELD, blockShift, DataType.LONG, MetricRole.COUNTER);
        assertLongRoundTrip(LONG_COUNTER_FIELD, blockShift, monotonicLongs(blockShift));
    }

    public void testTimestampSplitDeltaRoundTripThroughFormat() throws IOException {
        final int blockShift = randomBlockShift();
        assertResolvesTo("splitDelta>delta>offset>gcd>bitPack", TIMESTAMP_FIELD, blockShift, null, null);
        assertLongRoundTrip(TIMESTAMP_FIELD, blockShift, monotonicLongs(blockShift));
    }

    public void testDoubleGaugeSortedNumericRoundTripThroughFormat() throws IOException {
        final int blockShift = randomBlockShift();
        final int numDocs = (1 << blockShift) * 2;
        final double[][] values = new double[numDocs][];
        for (int i = 0; i < numDocs; i++) {
            final int valueCount = ESTestCase.randomIntBetween(1, 5);
            values[i] = new double[valueCount];
            for (int v = 0; v < valueCount; v++) {
                values[i][v] = twoDecimalPlaces(ESTestCase.randomDoubleBetween(0.0, 1000.0, true));
            }
            Arrays.sort(values[i]);
        }
        assertSortedNumericDoubleRoundTrip(DOUBLE_GAUGE_FIELD, blockShift, values);
    }

    public void testDoubleGaugeSpecialValuesRoundTripThroughFormat() throws IOException {
        final int blockShift = randomBlockShift();
        final int numDocs = (1 << blockShift) * 2;
        final double[] values = gaugeDoubles(blockShift);
        values[0] = Double.POSITIVE_INFINITY;
        values[1] = Double.NEGATIVE_INFINITY;
        values[2] = -0.0;
        values[3] = Double.MAX_VALUE;
        values[4] = Double.NaN;
        values[numDocs - 1] = Double.MIN_NORMAL;
        assertDoubleRoundTrip(DOUBLE_GAUGE_FIELD, blockShift, values);
    }

    public void testDoubleGaugeForceMergeRoundTripThroughFormat() throws IOException {
        final int blockShift = randomBlockShift();
        final double[] values = gaugeDoubles(blockShift);
        final DocValuesFormat format = buildRolePipelineFormat(blockShift);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
                for (int i = 0; i < values.length; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(DOUBLE_GAUGE_FIELD, NumericUtils.doubleToSortableLong(values[i])));
                    writer.addDocument(doc);
                    if (i == values.length / 2) {
                        writer.commit();
                    }
                }
                writer.forceMerge(1);
            }
            assertDoubleValues(dir, DOUBLE_GAUGE_FIELD, values);
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

    public void testPerFieldBlockSizeWithMixedDensity() throws IOException {
        // Three numeric fields at three block sizes in one segment, with three densities: CUSTOM_BS_FIELD
        // dense, DEMOTED_BS_FIELD (128) one third missing, DEFAULT_BS_FIELD (512) half missing. Locks the
        // interaction of per-field block-size resolution with the sparse presence path across multiple blocks.
        // NOTE: random promoted block size (1024-4096), distinct from the 512 default and 128 demoted.
        final int promotedBlockSize = 1 << ESTestCase.randomIntBetween(10, 12);
        final int numDocs = promotedBlockSize * 2 + ESTestCase.randomIntBetween(0, promotedBlockSize);
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
                    if (i % 3 != 0) {
                        doc.add(new NumericDocValuesField(DEMOTED_BS_FIELD, (long) i * 11L));
                    }
                    if (i % 2 != 0) {
                        doc.add(new NumericDocValuesField(DEFAULT_BS_FIELD, (long) i * 5L));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertNumericSequence(leaf, CUSTOM_BS_FIELD, i -> (long) i * 17L);
                    assertSparseNumeric(leaf, DEMOTED_BS_FIELD, g -> g % 3 != 0, i -> (long) i * 11L);
                    assertSparseNumeric(leaf, DEFAULT_BS_FIELD, g -> g % 2 != 0, i -> (long) i * 5L);
                }
            }
        }
    }

    public void testPerFieldBlockSizeSortedNumericWithMixedDensity() throws IOException {
        // Three sorted numeric fields at three block sizes in one segment, with three densities:
        // CUSTOM_BS_SORTED_FIELD dense, DEMOTED_BS_SORTED_FIELD (128) one third missing,
        // DEFAULT_BS_SORTED_FIELD (512) half missing, each spanning multiple blocks.
        // NOTE: random promoted block size (1024-4096), distinct from the 512 default and 128 demoted.
        final int promotedBlockSize = 1 << ESTestCase.randomIntBetween(10, 12);
        final int numDocs = promotedBlockSize * 2 + ESTestCase.randomIntBetween(0, promotedBlockSize);
        final int valuesPerDoc = 3;
        try (Directory dir = newDirectory()) {
            try (
                IndexWriter writer = new IndexWriter(
                    dir,
                    writerConfig(buildPerFieldBlockSizeFormat(NUMERIC_LARGE_BLOCK_SHIFT, promotedBlockSize))
                )
            ) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    for (int j = 0; j < valuesPerDoc; j++) {
                        doc.add(new SortedNumericDocValuesField(CUSTOM_BS_SORTED_FIELD, ((long) i * valuesPerDoc + j) * 23L));
                        if (i % 3 != 0) {
                            doc.add(new SortedNumericDocValuesField(DEMOTED_BS_SORTED_FIELD, ((long) i * valuesPerDoc + j) * 11L));
                        }
                        if (i % 2 != 0) {
                            doc.add(new SortedNumericDocValuesField(DEFAULT_BS_SORTED_FIELD, ((long) i * valuesPerDoc + j) * 6L));
                        }
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertSortedNumericAscending(leaf, CUSTOM_BS_SORTED_FIELD, valuesPerDoc, (i, j) -> ((long) i * valuesPerDoc + j) * 23L);
                    assertSparseSortedNumeric(
                        leaf,
                        DEMOTED_BS_SORTED_FIELD,
                        g -> g % 3 != 0,
                        valuesPerDoc,
                        (i, j) -> ((long) i * valuesPerDoc + j) * 11L
                    );
                    assertSparseSortedNumeric(
                        leaf,
                        DEFAULT_BS_SORTED_FIELD,
                        g -> g % 2 != 0,
                        valuesPerDoc,
                        (i, j) -> ((long) i * valuesPerDoc + j) * 6L
                    );
                }
            }
        }
    }

    public void testMixedDocValuesTypesPerFieldBlockSizeAndDensity() throws IOException {
        // One segment carrying all four doc-values types with mixed density across multiple blocks.
        // Numeric and sorted-numeric fields take per-field block sizes via ES95NumericCodec; sorted and
        // sorted-set fields use the shared TSDBOrdinalBlockCodec at the format-level block.
        // NOTE: random promoted block size (1024-4096), distinct from the 512 default and 128 demoted.
        final int promotedBlockSize = 1 << ESTestCase.randomIntBetween(10, 12);
        final int numDocs = promotedBlockSize * 2 + ESTestCase.randomIntBetween(0, promotedBlockSize);
        final int termsPerDoc = 3;
        final long[] numericValues = new long[numDocs];
        for (int i = 0; i < numDocs; i++) {
            numericValues[i] = random().nextLong();
        }
        try (Directory dir = newDirectory()) {
            try (
                IndexWriter writer = new IndexWriter(
                    dir,
                    writerConfig(buildPerFieldBlockSizeFormat(NUMERIC_LARGE_BLOCK_SHIFT, promotedBlockSize))
                )
            ) {
                for (int i = 0; i < numDocs; i++) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(CUSTOM_BS_FIELD, numericValues[i]));
                    if (i % 3 != 0) {
                        doc.add(new SortedNumericDocValuesField(DEFAULT_BS_FIELD, (long) i * 10L));
                        doc.add(new SortedNumericDocValuesField(DEFAULT_BS_FIELD, (long) i * 10L + 1L));
                    }
                    if (i % 2 != 0) {
                        doc.add(new SortedDocValuesField(CUSTOM_BS_SORTED_FIELD, new BytesRef(String.format(Locale.ROOT, "s-%05d", i))));
                    }
                    addSortedSetTerms(doc, DEFAULT_BS_SORTED_FIELD, "t", i, termsPerDoc);
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                for (LeafReaderContext leaf : reader.leaves()) {
                    assertNumericSequence(leaf, CUSTOM_BS_FIELD, d -> numericValues[d]);
                    assertSparseSortedNumeric(leaf, DEFAULT_BS_FIELD, g -> g % 3 != 0, 2, (d, j) -> (long) d * 10L + j);
                    assertSparseSorted(leaf, CUSTOM_BS_SORTED_FIELD, g -> g % 2 != 0, d -> String.format(Locale.ROOT, "s-%05d", d));
                    assertSortedSetSequence(leaf, DEFAULT_BS_SORTED_FIELD, termsPerDoc, (d, j) -> sortedSetTerm("t", d, j));
                }
            }
        }
    }

    private static void addSortedSetTerms(Document doc, String field, String prefix, int docIndex, int termsPerDoc) {
        for (int j = 0; j < termsPerDoc; j++) {
            doc.add(new SortedSetDocValuesField(field, new BytesRef(sortedSetTerm(prefix, docIndex, j))));
        }
    }

    private static String sortedSetTerm(String prefix, int docIndex, int termIndex) {
        return String.format(Locale.ROOT, "%s-%05d-%d", prefix, docIndex, termIndex);
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

    @FunctionalInterface
    private interface DocPresence {
        boolean at(int globalDoc);
    }

    private static void assertSparseNumeric(LeafReaderContext leaf, String field, DocPresence present, ExpectedValue expected)
        throws IOException {
        final NumericDocValues ndv = leaf.reader().getNumericDocValues(field);
        assertNotNull("missing doc values for " + field, ndv);
        for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
            final int globalDoc = leaf.docBase + docId;
            if (present.at(globalDoc)) {
                assertTrue(ndv.advanceExact(docId));
                assertEquals("field " + field + " mismatch at doc " + globalDoc, expected.at(globalDoc), ndv.longValue());
            } else {
                assertFalse("field " + field + " should be missing at doc " + globalDoc, ndv.advanceExact(docId));
            }
        }
    }

    private static void assertSparseSortedNumeric(
        LeafReaderContext leaf,
        String field,
        DocPresence present,
        int valuesPerDoc,
        ExpectedMultiValue expected
    ) throws IOException {
        final SortedNumericDocValues sndv = leaf.reader().getSortedNumericDocValues(field);
        assertNotNull("missing sorted numeric doc values for " + field, sndv);
        for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
            final int globalDoc = leaf.docBase + docId;
            if (present.at(globalDoc)) {
                assertTrue(sndv.advanceExact(docId));
                assertEquals("doc value count mismatch on field " + field + " at doc " + globalDoc, valuesPerDoc, sndv.docValueCount());
                for (int j = 0; j < valuesPerDoc; j++) {
                    assertEquals("field " + field + " doc " + globalDoc + " value " + j, expected.at(globalDoc, j), sndv.nextValue());
                }
            } else {
                assertFalse("field " + field + " should be missing at doc " + globalDoc, sndv.advanceExact(docId));
            }
        }
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

    private static void assertSparseSorted(LeafReaderContext leaf, String field, DocPresence present, ExpectedTermAtDoc expected)
        throws IOException {
        final SortedDocValues sdv = leaf.reader().getSortedDocValues(field);
        assertNotNull("missing sorted doc values for " + field, sdv);
        for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
            final int globalDoc = leaf.docBase + docId;
            if (present.at(globalDoc)) {
                assertTrue(sdv.advanceExact(docId));
                final BytesRef actual = BytesRef.deepCopyOf(sdv.lookupOrd(sdv.ordValue()));
                assertEquals("field " + field + " doc " + globalDoc, new BytesRef(expected.at(globalDoc)), actual);
            } else {
                assertFalse("field " + field + " should be missing at doc " + globalDoc, sdv.advanceExact(docId));
            }
        }
    }

    private static void assertSparseSortedSet(
        LeafReaderContext leaf,
        String field,
        DocPresence present,
        int termsPerDoc,
        ExpectedTerm expected
    ) throws IOException {
        final SortedSetDocValues ssdv = leaf.reader().getSortedSetDocValues(field);
        assertNotNull("missing sorted-set doc values for " + field, ssdv);
        for (int docId = 0; docId < leaf.reader().maxDoc(); docId++) {
            final int globalDoc = leaf.docBase + docId;
            if (present.at(globalDoc)) {
                assertTrue(ssdv.advanceExact(docId));
                assertEquals("doc value count for " + field + " doc " + globalDoc, termsPerDoc, ssdv.docValueCount());
                for (int j = 0; j < termsPerDoc; j++) {
                    final BytesRef actual = BytesRef.deepCopyOf(ssdv.lookupOrd(ssdv.nextOrd()));
                    assertEquals("field " + field + " doc " + globalDoc + " term " + j, new BytesRef(expected.at(globalDoc, j)), actual);
                }
            } else {
                assertFalse("field " + field + " should be missing at doc " + globalDoc, ssdv.advanceExact(docId));
            }
        }
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
            return new FieldContext(blockSize, fieldName, null, null);
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

    private static final String DOUBLE_GAUGE_FIELD = "double_gauge";
    private static final String DOUBLE_COUNTER_FIELD = "double_counter";
    private static final String LONG_COUNTER_FIELD = "long_counter";
    private static final String TIMESTAMP_FIELD = "@timestamp";

    private static final FieldContextResolver ROLE_RESOLVER = (fieldName, blockSize) -> {
        if (DOUBLE_GAUGE_FIELD.equals(fieldName)) {
            return new FieldContext(blockSize, fieldName, DataType.DOUBLE, MetricRole.GAUGE);
        }
        if (DOUBLE_COUNTER_FIELD.equals(fieldName)) {
            return new FieldContext(blockSize, fieldName, DataType.DOUBLE, MetricRole.COUNTER);
        }
        if (LONG_COUNTER_FIELD.equals(fieldName)) {
            return new FieldContext(blockSize, fieldName, DataType.LONG, MetricRole.COUNTER);
        }
        return new FieldContext(blockSize, fieldName, null, null);
    };

    private void assertDoubleRoundTrip(final String field, int blockShift, final double[] values) throws IOException {
        final DocValuesFormat format = buildRolePipelineFormat(blockShift);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
                for (final double value : values) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(field, NumericUtils.doubleToSortableLong(value)));
                    writer.addDocument(doc);
                }
            }
            assertDoubleValues(dir, field, values);
        }
    }

    private static void assertDoubleValues(final Directory dir, final String field, final double[] values) throws IOException {
        try (DirectoryReader reader = DirectoryReader.open(dir)) {
            int total = 0;
            for (final LeafReaderContext leaf : reader.leaves()) {
                final NumericDocValues ndv = leaf.reader().getNumericDocValues(field);
                assertNotNull(ndv);
                int count = 0;
                while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                    final double expected = values[leaf.docBase + count];
                    final double actual = NumericUtils.sortableLongToDouble(ndv.longValue());
                    assertEquals(Double.doubleToRawLongBits(expected), Double.doubleToRawLongBits(actual));
                    count++;
                }
                total += count;
            }
            assertEquals(values.length, total);
        }
    }

    private void assertLongRoundTrip(final String field, int blockShift, final long[] values) throws IOException {
        final DocValuesFormat format = buildRolePipelineFormat(blockShift);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
                for (final long value : values) {
                    final Document doc = new Document();
                    doc.add(new NumericDocValuesField(field, value));
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                int total = 0;
                for (final LeafReaderContext leaf : reader.leaves()) {
                    final NumericDocValues ndv = leaf.reader().getNumericDocValues(field);
                    assertNotNull(ndv);
                    int count = 0;
                    while (ndv.nextDoc() != NumericDocValues.NO_MORE_DOCS) {
                        assertEquals(values[leaf.docBase + count], ndv.longValue());
                        count++;
                    }
                    total += count;
                }
                assertEquals(values.length, total);
            }
        }
    }

    private void assertSortedNumericDoubleRoundTrip(final String field, int blockShift, final double[][] values) throws IOException {
        final DocValuesFormat format = buildRolePipelineFormat(blockShift);
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, writerConfig(format))) {
                for (final double[] docValues : values) {
                    final Document doc = new Document();
                    for (final double value : docValues) {
                        doc.add(new SortedNumericDocValuesField(field, NumericUtils.doubleToSortableLong(value)));
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                int total = 0;
                for (final LeafReaderContext leaf : reader.leaves()) {
                    final SortedNumericDocValues sndv = leaf.reader().getSortedNumericDocValues(field);
                    assertNotNull(sndv);
                    int count = 0;
                    while (sndv.nextDoc() != SortedNumericDocValues.NO_MORE_DOCS) {
                        final double[] expected = values[leaf.docBase + count];
                        assertEquals(expected.length, sndv.docValueCount());
                        for (int v = 0; v < sndv.docValueCount(); v++) {
                            final double actual = NumericUtils.sortableLongToDouble(sndv.nextValue());
                            assertEquals(Double.doubleToRawLongBits(expected[v]), Double.doubleToRawLongBits(actual));
                        }
                        count++;
                    }
                    total += count;
                }
                assertEquals(values.length, total);
            }
        }
    }

    private static void assertResolvesTo(
        final String expectedStages,
        final String fieldName,
        int blockShift,
        final DataType dataType,
        final MetricRole metricRole
    ) {
        final FieldContext context = new FieldContext(1 << blockShift, fieldName, dataType, metricRole);
        assertEquals(expectedStages, StaticPipelineConfigResolver.INSTANCE.resolve(context).describeStages());
    }

    private static double[] gaugeDoubles(int blockShift) {
        final int numDocs = (1 << blockShift) * 2;
        final double[] values = new double[numDocs];
        for (int i = 0; i < numDocs; i++) {
            values[i] = twoDecimalPlaces(22.5 + i * 0.01);
        }
        return values;
    }

    private static double[] counterDoubles(int blockShift) {
        final int numDocs = (1 << blockShift) * 2;
        final double[] values = new double[numDocs];
        double running = 0.0;
        for (int i = 0; i < numDocs; i++) {
            running += 0.25;
            values[i] = twoDecimalPlaces(running);
        }
        return values;
    }

    private static long[] monotonicLongs(int blockShift) {
        final int numDocs = (1 << blockShift) * 2;
        final long[] values = new long[numDocs];
        values[0] = 1_700_000_000_000L;
        for (int i = 1; i < numDocs; i++) {
            values[i] = values[i - 1] + ESTestCase.randomLongBetween(1, 1000);
        }
        return values;
    }

    private static double twoDecimalPlaces(double value) {
        return Math.round(value * 100.0) / 100.0;
    }

    private static int randomBlockShift() {
        return ESTestCase.randomIntBetween(7, 12);
    }

    private static DocValuesFormat buildRolePipelineFormat(int blockShift) {
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
            blockSize -> (input, values, count) -> {
                throw new AssertionError("fallback decoder should not be reached for pipeline-encoded numeric fields");
            },
            ROLE_RESOLVER
        );
    }

    private static IndexWriterConfig writerConfig(final DocValuesFormat format) {
        final IndexWriterConfig config = new IndexWriterConfig();
        config.setCodec(TestUtil.alwaysDocValuesFormat(format));
        config.setMergePolicy(new LogByteSizeMergePolicy());
        return config;
    }
}
