/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb.es819;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesFormatTests;
import org.elasticsearch.index.codec.tsdb.AbstractTSDBDocValuesProducer.BaseSortedDocValues;
import org.elasticsearch.index.codec.tsdb.BinaryDVCompressionMode;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormatTests;
import org.elasticsearch.index.codec.tsdb.TSDBDocValuesTestUtil;
import org.elasticsearch.index.mapper.BlockLoader;
import org.elasticsearch.index.mapper.BlockLoader.OptionalColumnAtATimeReader;
import org.elasticsearch.index.mapper.TestBlock;
import org.elasticsearch.lucene.queries.BinaryDocValuesContainsTermQuery;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat.NUMERIC_LARGE_BLOCK_SHIFT;
import static org.elasticsearch.test.ESTestCase.randomAlphaOfLength;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.elasticsearch.test.ESTestCase.randomIntBetween;
import static org.elasticsearch.test.ESTestCase.randomUnicodeOfCodepointLengthBetween;
import static org.hamcrest.Matchers.equalTo;

public class ES819TSDBDocValuesFormatTests extends AbstractTSDBDocValuesFormatTests {

    private final Codec codec = new Elasticsearch93Lucene104Codec() {

        final DocValuesFormat docValuesFormat = new ES819Version3TSDBDocValuesFormat(
            ESTestCase.randomIntBetween(2, 4096),
            ESTestCase.randomIntBetween(1, 512),
            random().nextBoolean(),
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            true,
            random().nextBoolean() ? NUMERIC_LARGE_BLOCK_SHIFT : NUMERIC_BLOCK_SHIFT,
            random().nextBoolean()
        );

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return docValuesFormat;
        }
    };

    public static class TestES819TSDBDocValuesFormatVersion0 extends ES819TSDBDocValuesFormat {

        public TestES819TSDBDocValuesFormatVersion0() {
            super();
        }

        @Override
        public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
            return new ES819TSDBDocValuesConsumerVersion0(
                state,
                formatConfig.skipIndexIntervalSize(),
                formatConfig.minDocsPerOrdinalForRangeEncoding(),
                enableOptimizedMerge,
                DATA_CODEC,
                DATA_EXTENSION,
                META_CODEC,
                META_EXTENSION,
                NUMERIC_BLOCK_SHIFT
            );
        }
    }

    @Override
    protected Codec getCodec() {
        return codec;
    }

    public void testBinaryCompressionEnabled() {
        ES819TSDBDocValuesFormat docValueFormat = new ES819Version3TSDBDocValuesFormat();
        assertThat(docValueFormat.formatConfig.binaryCompressionMode(), equalTo(BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1));
    }

    public void testAddIndices() throws IOException {
        doTestAddIndices(
            List.of(
                new ES87TSDBDocValuesFormatTests.TestES87TSDBDocValuesFormat(random().nextInt(4, 16)),
                new ES819TSDBDocValuesFormat(),
                new ES819Version3TSDBDocValuesFormat(),
                new Lucene90DocValuesFormat()
            )
        );
    }

    // These tests require ES819-specific codec configuration (forced range encoding or specific
    // binary compression modes) and are not part of the shared test suite.

    public void testLoadKeywordFieldWithIndexSorts() throws IOException {
        String primaryField = "sorted_first";
        String secondField = "sorted_second";
        String unsortedField = "no_sort";
        String sparseField = "sparse";
        var config = getIndexWriterConfig(primaryField);
        Map<Integer, String> hostnames = new HashMap<>();
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            int numDocs = randomIntBetween(100, 5000);
            for (int i = 0; i < numDocs; i++) {
                hostnames.put(i, "h" + random().nextInt(10));
            }
            List<Integer> ids = new ArrayList<>(hostnames.keySet());
            Randomness.shuffle(ids);
            Set<Integer> sparseIds = new HashSet<>(ESTestCase.randomSubsetOf(ESTestCase.between(1, ids.size() / 2), ids));
            for (Integer id : ids) {
                var d = new Document();
                String hostname = hostnames.get(id);
                d.add(new NumericDocValuesField("id", id));
                d.add(new SortedDocValuesField(primaryField, new BytesRef(hostname)));
                d.add(new SortedDocValuesField(secondField, new BytesRef(hostname)));
                d.add(new SortedDocValuesField(unsortedField, new BytesRef(hostname)));
                if (sparseIds.contains(id)) {
                    d.add(new SortedDocValuesField(sparseField, new BytesRef(hostname)));
                }
                writer.addDocument(d);
                if (random().nextInt(100) < 10) {
                    writer.flush();
                }
            }
            for (int iter = 0; iter < 2; iter++) {
                var factory = TestBlock.factory();
                try (DirectoryReader reader = DirectoryReader.open(writer)) {
                    for (LeafReaderContext leaf : reader.leaves()) {
                        BlockLoader.Docs docs = TestBlock.docs(leaf);
                        var idReader = ESTestCase.asInstanceOf(OptionalColumnAtATimeReader.class, leaf.reader().getNumericDocValues("id"));
                        TestBlock idBlock = (TestBlock) idReader.tryRead(factory, docs, 0, false, null, false, false);
                        assertNotNull(idBlock);

                        {
                            var reader2 = (BaseSortedDocValues) ESTestCase.asInstanceOf(
                                OptionalColumnAtATimeReader.class,
                                leaf.reader().getSortedDocValues(secondField)
                            );
                            int randomOffset = ESTestCase.between(0, docs.count() - 1);
                            TestBlock block;
                            if (reader2.getValueCount() == 1) {
                                block = (TestBlock) reader2.tryReadAHead(factory, docs, randomOffset);
                            } else {
                                assertNull(reader2.tryReadAHead(factory, docs, randomOffset));
                                block = (TestBlock) reader2.tryRead(factory, docs, randomOffset, false, null, false, false);
                            }
                            assertNotNull(block);
                            assertThat(block.size(), equalTo(docs.count() - randomOffset));
                            for (int i = 0; i < block.size(); i++) {
                                String actualHostName = BytesRefs.toString(block.get(i));
                                int id = ((Number) idBlock.get(i + randomOffset)).intValue();
                                String expectedHostName = hostnames.get(id);
                                assertEquals(expectedHostName, actualHostName);
                            }
                        }
                        {
                            var reader3 = (BaseSortedDocValues) ESTestCase.asInstanceOf(
                                OptionalColumnAtATimeReader.class,
                                leaf.reader().getSortedDocValues(unsortedField)
                            );
                            int randomOffset = ESTestCase.between(0, docs.count() - 1);
                            TestBlock block;
                            if (reader3.getValueCount() == 1) {
                                block = (TestBlock) reader3.tryReadAHead(factory, docs, randomOffset);
                            } else {
                                assertNull(reader3.tryReadAHead(factory, docs, randomOffset));
                                block = (TestBlock) reader3.tryRead(factory, docs, randomOffset, false, null, false, false);
                            }
                            assertNotNull(reader3);
                            assertNotNull(block);
                            assertThat(block.size(), equalTo(docs.count() - randomOffset));
                            for (int i = 0; i < block.size(); i++) {
                                String actualHostName = BytesRefs.toString(block.get(i));
                                int id = ((Number) idBlock.get(i + randomOffset)).intValue();
                                String expectedHostName = hostnames.get(id);
                                assertEquals(expectedHostName, actualHostName);
                            }
                        }
                        for (int offset = 0; offset < idBlock.size(); offset += ESTestCase.between(1, numDocs)) {
                            int start = offset;
                            var reader1 = (BaseSortedDocValues) ESTestCase.asInstanceOf(
                                OptionalColumnAtATimeReader.class,
                                leaf.reader().getSortedDocValues(primaryField)
                            );
                            while (start < idBlock.size()) {
                                int end = start + random().nextInt(idBlock.size() - start);
                                TestBlock hostBlock = (TestBlock) reader1.tryReadAHead(factory, new BlockLoader.Docs() {
                                    @Override
                                    public int count() {
                                        return end + 1;
                                    }

                                    @Override
                                    public int get(int docId) {
                                        return docId;
                                    }

                                    @Override
                                    public boolean mayContainDuplicates() {
                                        return false;
                                    }
                                }, start);
                                assertNotNull(hostBlock);
                                assertThat(hostBlock.size(), equalTo(end - start + 1));
                                for (int i = 0; i < hostBlock.size(); i++) {
                                    String actualHostName = BytesRefs.toString(hostBlock.get(i));
                                    assertThat(actualHostName, equalTo(hostnames.get(((Number) idBlock.get(i + start)).intValue())));
                                }
                                if (start == idBlock.size() - 1) {
                                    break;
                                }
                                start = end + ESTestCase.between(0, 10);
                            }
                        }
                        writer.forceMerge(1);
                    }
                }
            }
        }
    }

    private static IndexWriterConfig getIndexWriterConfig(String primaryField) {
        var config = new IndexWriterConfig();
        config.setIndexSort(new Sort(new SortField(primaryField, SortField.Type.STRING, false)));
        config.setMergePolicy(new LogByteSizeMergePolicy());
        final var codec = new Elasticsearch93Lucene104Codec() {
            final ES819Version3TSDBDocValuesFormat docValuesFormat = new ES819Version3TSDBDocValuesFormat(
                randomIntBetween(2, 4096),
                1,
                random().nextBoolean(),
                TSDBDocValuesTestUtil.randomBinaryCompressionMode(),
                randomBoolean(),
                TSDBDocValuesTestUtil.randomNumericBlockSize(),
                randomBoolean()
            );

            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormat;
            }
        };
        config.setCodec(codec);
        return config;
    }

    public void testOptionalLengthReaderLengthIterator() throws Exception {
        final String timestampField = TIMESTAMP_FIELD;
        final String binaryField = "binary_field";
        long currentTimestamp = BASE_TIMESTAMP;

        // Use a few distinct lengths so that we get both matching and non-matching docs,
        // and consecutive runs of matching docs.
        final int[] possibleLengths = new int[] { 5, 10, 20 };
        final int targetLength = possibleLengths[randomIntBetween(0, possibleLengths.length - 1)];

        // tryLengthIterator is supported on all binary doc values implementation,
        // and so randomize between compressed and uncompressed implementation to test both implementations.
        var dvFormat = new ES819Version3TSDBDocValuesFormat(
            ESTestCase.randomIntBetween(2, 4096),
            ESTestCase.randomIntBetween(1, 512),
            random().nextBoolean(),
            randomBoolean() ? BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1 : BinaryDVCompressionMode.NO_COMPRESS,
            randomBoolean(),
            NUMERIC_LARGE_BLOCK_SHIFT,
            randomBoolean()
        );
        var compressedCodec = TestUtil.alwaysDocValuesFormat(dvFormat);

        var config = new IndexWriterConfig();
        config.setIndexSort(new Sort(new SortedNumericSortField(timestampField, SortField.Type.LONG, true)));
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(compressedCodec);

        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            int numDocs = 256 + random().nextInt(4096);
            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(SortedNumericDocValuesField.indexedField(timestampField, currentTimestamp));

                int length = possibleLengths[random().nextInt(possibleLengths.length)];
                d.add(new BinaryDocValuesField(binaryField, new BytesRef(randomAlphaOfLength(length))));

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                currentTimestamp += 1000L;
            }
            iw.commit();
            iw.forceMerge(1);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leafReader = reader.leaves().getFirst().reader();

                // Build expected set of matching doc IDs by reading actual binary values
                // (avoids issues with doc ID reordering from index sort)
                Set<Integer> expectedDocIds = new HashSet<>();
                {
                    var refDV = getTSDBBinaryValues(leafReader, binaryField);
                    for (int docId = 0; docId < numDocs; docId++) {
                        assertTrue(refDV.advanceExact(docId));
                        if (refDV.binaryValue().length == targetLength) {
                            expectedDocIds.add(docId);
                        }
                    }
                }

                // Test tryLengthIterator
                var binaryDV = getTSDBBinaryValues(leafReader, binaryField);
                DocIdSetIterator lengthIter = binaryDV.tryLengthIterator(targetLength);
                assertNotNull(lengthIter);
                assertEquals(-1, lengthIter.docID());

                // Collect all docs from the iterator and verify they match expected
                Set<Integer> actualDocIds = new HashSet<>();
                int doc;
                while ((doc = lengthIter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    assertTrue("Iterator returned unexpected doc " + doc, expectedDocIds.contains(doc));
                    actualDocIds.add(doc);

                    // Validate docIDRunEnd contract
                    int runEnd = lengthIter.docIDRunEnd();
                    assertTrue("docIDRunEnd (" + runEnd + ") must be > docID (" + doc + ")", runEnd > doc);

                    // All docs in [docID, docIDRunEnd) must match
                    for (int d = doc; d < runEnd; d++) {
                        assertTrue("doc " + d + " in run [" + doc + ", " + runEnd + ") should match", expectedDocIds.contains(d));
                    }

                    // Advance through the run to verify advance within run works correctly
                    for (int d = doc + 1; d < runEnd; d++) {
                        assertEquals(d, lengthIter.advance(d));
                        actualDocIds.add(d);
                        assertEquals("docIDRunEnd should be stable within a run", runEnd, lengthIter.docIDRunEnd());
                    }
                }

                assertEquals("Iterator should return exactly the matching docs", expectedDocIds, actualDocIds);

                // Test advance past existing docs
                binaryDV = getTSDBBinaryValues(leafReader, binaryField);
                lengthIter = binaryDV.tryLengthIterator(targetLength);
                assertNotNull(lengthIter);
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, lengthIter.advance(numDocs));

                // Test with a length that no doc has:iterator should be immediately exhausted
                binaryDV = getTSDBBinaryValues(leafReader, binaryField);
                lengthIter = binaryDV.tryLengthIterator(9999);
                assertNotNull(lengthIter);
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, lengthIter.nextDoc());
            }
        }
    }

    public void testContainsIterator() throws Exception {
        final String timestampField = TIMESTAMP_FIELD;
        final String binaryField = "binary_field";
        long currentTimestamp = BASE_TIMESTAMP;

        final String containsTerm = randomUnicodeOfCodepointLengthBetween(1, 10);
        int numMatchingValues = randomIntBetween(5, 100);
        final String[] matchingValues = new String[numMatchingValues];
        for (int i = 0; i < numMatchingValues; i++) {
            String prefix = randomUnicodeOfCodepointLengthBetween(0, 20);
            String suffix = randomUnicodeOfCodepointLengthBetween(0, 20);
            matchingValues[i] = prefix + containsTerm + suffix;
        }

        // tryContainsIterator is only implemented for the compressed binary doc values path
        var dvFormat = new ES819Version3TSDBDocValuesFormat(
            ESTestCase.randomIntBetween(2, 4096),
            ESTestCase.randomIntBetween(1, 512),
            random().nextBoolean(),
            BinaryDVCompressionMode.COMPRESSED_ZSTD_LEVEL_1,
            randomBoolean(),
            NUMERIC_LARGE_BLOCK_SHIFT,
            randomBoolean()
        );
        var compressedCodec = TestUtil.alwaysDocValuesFormat(dvFormat);

        var config = new IndexWriterConfig();
        config.setIndexSort(new Sort(new SortedNumericSortField(timestampField, SortField.Type.LONG, true)));
        config.setLeafSorter(DataStream.TIMESERIES_LEAF_READERS_SORTER);
        config.setMergePolicy(new LogByteSizeMergePolicy());
        config.setCodec(compressedCodec);

        try (var dir = newDirectory(); var iw = new IndexWriter(dir, config)) {
            int numDocs = 256 + random().nextInt(4096);
            for (int i = 0; i < numDocs; i++) {
                var d = new Document();
                d.add(SortedNumericDocValuesField.indexedField(timestampField, currentTimestamp));

                String value = randomBoolean()
                    ? matchingValues[random().nextInt(matchingValues.length)]
                    : randomUnicodeOfCodepointLengthBetween(1, 100);
                d.add(new BinaryDocValuesField(binaryField, new BytesRef(value)));

                iw.addDocument(d);
                if (i % 100 == 0) {
                    iw.commit();
                }
                currentTimestamp += 1000L;
            }
            iw.commit();
            iw.forceMerge(1);

            BytesRef containsTermRef = new BytesRef(containsTerm);

            try (var reader = DirectoryReader.open(iw)) {
                assertEquals(1, reader.leaves().size());
                assertEquals(numDocs, reader.maxDoc());
                var leafReader = reader.leaves().getFirst().reader();

                // Build expected set of matching doc IDs by reading actual binary values
                Set<Integer> expectedDocIds = new HashSet<>();
                {
                    var refDV = getTSDBBinaryValues(leafReader, binaryField);
                    for (int docId = 0; docId < numDocs; docId++) {
                        assertTrue(refDV.advanceExact(docId));
                        if (BinaryDocValuesContainsTermQuery.contains(refDV.binaryValue(), containsTermRef)) {
                            expectedDocIds.add(docId);
                        }
                    }
                }
                assertFalse("expected some matching docs", expectedDocIds.isEmpty());

                // Test tryContainsIterator via nextDoc
                var binaryDV = getTSDBBinaryValues(leafReader, binaryField);
                DocIdSetIterator containsIter = binaryDV.tryContainsIterator(containsTermRef);
                assertNotNull(containsIter);
                assertEquals(-1, containsIter.docID());

                Set<Integer> actualDocIds = new HashSet<>();
                int doc;
                while ((doc = containsIter.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                    assertTrue("Iterator returned unexpected doc " + doc, expectedDocIds.contains(doc));
                    actualDocIds.add(doc);
                }
                assertEquals("Iterator should return exactly the matching docs", expectedDocIds, actualDocIds);

                // Test advance past existing docs
                binaryDV = getTSDBBinaryValues(leafReader, binaryField);
                containsIter = binaryDV.tryContainsIterator(containsTermRef);
                assertNotNull(containsIter);
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, containsIter.advance(numDocs));

                // Test with a term that no doc contains:iterator should be immediately exhausted
                String notFoundTerm = randomUnicodeOfCodepointLengthBetween(101, 200);
                binaryDV = getTSDBBinaryValues(leafReader, binaryField);
                containsIter = binaryDV.tryContainsIterator(new BytesRef(notFoundTerm));
                assertNotNull(containsIter);
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, containsIter.nextDoc());

                // Test advance to specific matching docs
                binaryDV = getTSDBBinaryValues(leafReader, binaryField);
                containsIter = binaryDV.tryContainsIterator(containsTermRef);
                for (int expected : expectedDocIds.stream().sorted().toList()) {
                    int result = containsIter.advance(expected);
                    assertEquals("advance(" + expected + ") should land on that doc", expected, result);
                }
            }
        }
    }
}
