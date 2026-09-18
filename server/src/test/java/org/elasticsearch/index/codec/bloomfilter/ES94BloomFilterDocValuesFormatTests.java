/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.InvertableType;
import org.apache.lucene.document.LongField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.LogMergePolicy;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SerialMergeScheduler;
import org.apache.lucene.index.StandardDirectoryReader;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.analysis.MockAnalyzer;
import org.apache.lucene.tests.codecs.asserting.AssertingCodec;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.codec.bloomfilter.ES94BloomFilterDocValuesFormat.DEFAULT_LOW_BITS_PER_DOC;
import static org.elasticsearch.index.codec.bloomfilter.ES94BloomFilterDocValuesFormat.MAX_BLOOM_FILTER_SIZE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class ES94BloomFilterDocValuesFormatTests extends ESTestCase {
    public void testBloomFilterFieldIsNotStoredAndBloomFilterCanBeChecked() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            conf.setCodec(new TestCodec(new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME)));
            conf.setMergePolicy(newLogMergePolicy());
            // We want to have at most 1 segment
            conf.setMaxBufferedDocs(200);
            conf.setUseCompoundFile(randomBoolean());
            // We don't use the RandomIndexWriter because we want to control the settings so we get
            // deterministic test runs
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                List<BytesRef> indexedIds = indexDocs(writer, 50);

                assertBloomFilterTestsPositiveForExistingDocs(writer, indexedIds);
            }
        }
    }

    public void testBloomFilterMerges() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            final boolean randomBloomFilterSizes = randomBoolean();
            final boolean optimizedMergeEnabled = randomBoolean();
            conf.setCodec(
                new TestCodec(
                    new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME, optimizedMergeEnabled) {
                        @Override
                        public int bloomFilterSizeInBytesForNewSegment(int numDocs) {
                            if (randomBloomFilterSizes) {
                                // Between 32b and 64kb
                                return 1 << randomIntBetween(5, 16);
                            } else {
                                return super.bloomFilterSizeInBytesForNewSegment(numDocs);
                            }
                        }
                    }
                )
            );
            conf.setMergePolicy(newLogMergePolicy());
            var maxBufferedDocs = randomIntBetween(2, 10);
            conf.setMaxBufferedDocs(maxBufferedDocs);
            conf.setUseCompoundFile(randomBoolean());
            // We don't use the RandomIndexWriter because we want to control the settings so we get
            // deterministic test runs
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                List<BytesRef> indexedIds = indexDocs(writer, 200);

                writer.forceMerge(1);

                assertBloomFilterTestsPositiveForExistingDocs(writer, indexedIds);
            }
        }
    }

    public void testBloomFilterSizing() {
        var bloomFilterFormat = new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME);

        // The bloom filter size gets rounded up to the closest power of 2 (HIGH_BPD = 128 bits/doc)
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(10), is(equalTo(256)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(12), is(equalTo(256)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(14), is(equalTo(256)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(100), is(equalTo(2048)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(1_000), is(equalTo(16384)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(10_000), is(equalTo(262144)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(160_000), is(equalTo(4194304)));

        // Size scales with document count
        assertThat(
            bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(100),
            greaterThan(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(10))
        );
        assertThat(
            bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(1_000),
            greaterThan(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(100))
        );

        // Capped at MAX_BLOOM_FILTER_SIZE for large segment sizes
        assertThat(
            bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(Integer.MAX_VALUE),
            is(equalTo((int) MAX_BLOOM_FILTER_SIZE.getBytes()))
        );
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(10_000_000), is(equalTo((int) MAX_BLOOM_FILTER_SIZE.getBytes())));

        // Boundary: largest doc count at which the flat large-segment formula (LOW_BPD bits/doc) hits the cap
        int maxDocsBeforeCap = (int) (MAX_BLOOM_FILTER_SIZE.getBytes() * Byte.SIZE / DEFAULT_LOW_BITS_PER_DOC);
        assertThat(
            bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(maxDocsBeforeCap),
            is(equalTo((int) MAX_BLOOM_FILTER_SIZE.getBytes()))
        );
        assertThat(
            bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(maxDocsBeforeCap - 1),
            is(lessThanOrEqualTo((int) MAX_BLOOM_FILTER_SIZE.getBytes()))
        );
    }

    public void testTaperContinuityAtRegimeBoundaries() {
        var bloomFilterFormat = new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME);

        // Sweep the full taper range (160K–320K) in steps of 1K and verify that no adjacent pair
        // of sizes differs by more than 2x. Power-of-two rounding means sizes are not strictly
        // monotone, but a jump larger than 2x would indicate a sizing cliff.
        final int taperStart = 160_000;
        final int taperEnd = 320_000;
        final int step = 1_000;
        int prevSize = bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(taperStart);
        for (int n = taperStart + step; n <= taperEnd + step; n += step) {
            int size = bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(n);
            assertThat(
                "size ratio between n=" + n + " and n=" + (n - step) + " should be ≤ 2x",
                (double) Math.max(size, prevSize) / Math.min(size, prevSize),
                lessThanOrEqualTo(2.0)
            );
            prevSize = size;
        }
    }

    public void testPowerOfTwoRoundingOnlyLowersSaturation() {
        var bloomFilterFormat = new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME);
        // k = DEFAULT_NUM_HASH_FUNCTIONS = 4, HIGH_BPD = 128.0 bits/doc
        // Saturation before rounding: 1 - e^(-k / HIGH_BPD) = 1 - e^(-4/128) ≈ 3.1%
        final int k = 4;
        final double maxSaturation = 1.0 - Math.exp(-k / 128.0);

        for (int n : new int[] { 1, 10, 100, 1_000, 10_000, 100_000, 160_000 }) {
            int sizeBytes = bloomFilterFormat.bloomFilterSizeInBytesForNewSegment(n);
            long sizeBits = (long) sizeBytes * Byte.SIZE;
            double actualSaturation = 1.0 - Math.exp(-(double) k * n / sizeBits);
            assertThat("saturation for n=" + n, actualSaturation, lessThanOrEqualTo(maxSaturation));
        }
    }

    public void testBloomFilterSizeForMergedSegment() {
        var bloomFilterFormat = new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME);

        // Merged size is the max of input sizes
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForMergedSegment(List.of(32, 64, 128)), is(equalTo(128)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForMergedSegment(List.of(128, 64, 32)), is(equalTo(128)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForMergedSegment(List.of(32, 64, 128, 256)), is(equalTo(256)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForMergedSegment(List.of(64, 64, 64)), is(equalTo(64)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForMergedSegment(List.of(32, 128)), is(equalTo(128)));
        assertThat(bloomFilterFormat.bloomFilterSizeInBytesForMergedSegment(List.of(64)), is(equalTo(64)));

        // Capped at MAX_BLOOM_FILTER_SIZE
        assertThat(
            bloomFilterFormat.bloomFilterSizeInBytesForMergedSegment(List.of((int) MAX_BLOOM_FILTER_SIZE.getBytes(), 64)),
            is(equalTo((int) MAX_BLOOM_FILTER_SIZE.getBytes()))
        );
    }

    public void testMergedBloomFilterUsesMaxSize() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            // Use different fixed bloom filter sizes per segment to verify the merged result uses the max
            final int smallSize = 128;
            final int largeSize = 4096;
            final AtomicInteger flushCount = new AtomicInteger();
            conf.setCodec(new TestCodec(new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME, true) {
                @Override
                public int bloomFilterSizeInBytesForNewSegment(int numDocs) {
                    return (flushCount.getAndIncrement() % 2 == 0) ? smallSize : largeSize;
                }
            }));
            LogMergePolicy mergePolicy = newLogMergePolicy();
            mergePolicy.setMergeFactor(1000);
            conf.setMergePolicy(mergePolicy);
            conf.setMaxBufferedDocs(10);
            conf.setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                List<BytesRef> indexedIds = indexDocs(writer, 50);

                try (var directoryReader = StandardDirectoryReader.open(writer)) {
                    assertThat(directoryReader.leaves().size(), is(greaterThan(1)));
                }

                writer.forceMerge(1);

                try (var directoryReader = StandardDirectoryReader.open(writer)) {
                    assertThat(directoryReader.leaves().size(), is(equalTo(1)));
                    for (LeafReaderContext leaf : directoryReader.leaves()) {
                        var bloomFilter = (BloomFilter) leaf.reader().getBinaryDocValues(IdFieldMapper.NAME);

                        assertThat(bloomFilter.sizeInBytes(), is(equalTo((long) largeSize)));

                        for (BytesRef indexedId : indexedIds) {
                            assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, indexedId), is(true));
                        }
                    }
                }
            }
        }
    }

    public void testBloomFilterWithZeroedBitSetDoesNotReturnFalseNegatives() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            AtomicBoolean skipPopulatingBitSet = new AtomicBoolean(true);
            conf.setCodec(new TestCodec(new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME, true) {
                @Override
                BitSetBuffer createBitSetBuffer(int sizeInBytes) {
                    return new BitSetBuffer(BigArrays.NON_RECYCLING_INSTANCE, sizeInBytes) {
                        @Override
                        void set(int position, byte value) {
                            // Skip populating the bitset so we can simulate that the bitset ended up full of zeroes
                            if (skipPopulatingBitSet.get()) {
                                return;
                            }
                            super.set(position, value);
                        }

                        @Override
                        void set(long index, byte[] buf, int offset, int len) {
                            // Skip populating the bitset so we can simulate that the bitset ended up full of zeroes
                            if (skipPopulatingBitSet.get()) {
                                return;
                            }
                            super.set(index, buf, offset, len);
                        }
                    };
                }

                @Override
                int getCurrentFormatVersion() {
                    // We only check if a bloom filter is full of zeros if it was written with a version that had the bug
                    return ES94BloomFilterDocValuesFormat.VERSION_START;
                }
            }));
            LogMergePolicy mergePolicy = newLogMergePolicy();
            mergePolicy.setMergeFactor(1000);
            conf.setMergePolicy(mergePolicy);
            conf.setMaxBufferedDocs(10);
            conf.setUseCompoundFile(false);
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                List<BytesRef> indexedIds = indexDocs(writer, 50);
                var randomIds = randomList(10, 20, () -> new BytesRef(randomByteArrayOfLength(randomIntBetween(1, 20))));

                try (var directoryReader = StandardDirectoryReader.open(writer)) {
                    assertThat(directoryReader.leaves().size(), is(greaterThan(1)));

                    for (LeafReaderContext leaf : directoryReader.leaves()) {
                        var bloomFilter = (BloomFilter) leaf.reader().getBinaryDocValues(IdFieldMapper.NAME);

                        assertThat(bloomFilter.sizeInBytes(), is(greaterThan(0L)));

                        // If the bitset is full of zeros, the bloom filter will return true for all values
                        for (BytesRef indexedId : indexedIds) {
                            assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, indexedId), is(true));
                        }
                        for (BytesRef randomId : randomIds) {
                            assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, randomId), is(true));
                        }
                    }
                }

                // Given that some bitsets are full of 0s, we'll rebuild the bloom filters from scratch in the next merge based on the terms
                skipPopulatingBitSet.set(false);
                writer.forceMerge(1);

                try (var directoryReader = StandardDirectoryReader.open(writer)) {
                    assertThat(directoryReader.leaves().size(), is(equalTo(1)));
                    for (LeafReaderContext leaf : directoryReader.leaves()) {
                        var bloomFilter = (BloomFilter) leaf.reader().getBinaryDocValues(IdFieldMapper.NAME);

                        assertThat(bloomFilter.sizeInBytes(), is(greaterThan(0L)));

                        for (BytesRef indexedId : indexedIds) {
                            assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, indexedId), is(true));
                        }
                        for (BytesRef randomId : randomIds) {
                            assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, randomId), is(false));
                        }
                    }
                }
            }
        }
    }

    public void testAtLeastOneValueIsRequiredToBuildABloomFilter() throws IOException {
        try (var directory = newDirectory()) {
            Analyzer analyzer = new MockAnalyzer(random());
            IndexWriterConfig conf = newIndexWriterConfig(analyzer);
            conf.setCodec(new TestCodec(new ES94BloomFilterDocValuesFormat(BigArrays.NON_RECYCLING_INSTANCE, IdFieldMapper.NAME, true) {
                @Override
                public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
                    var delegate = super.fieldsConsumer(state);
                    // DocValuesConsumer provides a default #merge implementation that iterates over all the fields instead of going through
                    // the ES94BloomFilterDocValuesFormat#merge implementation which takes into account the underlying data structure.
                    return new DocValuesConsumer() {
                        @Override
                        public void close() throws IOException {
                            delegate.close();
                        }

                        @Override
                        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                            delegate.addNumericField(field, valuesProducer);
                        }

                        @Override
                        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                            delegate.addBinaryField(field, valuesProducer);
                        }

                        @Override
                        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                            delegate.addSortedField(field, valuesProducer);
                        }

                        @Override
                        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                            delegate.addSortedNumericField(field, valuesProducer);
                        }

                        @Override
                        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
                            delegate.addSortedSetField(field, valuesProducer);
                        }
                    };
                }
            }));
            LogMergePolicy mergePolicy = newLogMergePolicy();
            mergePolicy.setMergeFactor(1000);
            conf.setMergePolicy(mergePolicy);
            conf.setMaxBufferedDocs(10);
            conf.setUseCompoundFile(false);
            // Use the serial merge scheduler to execute the merge in the test thread
            conf.setMergeScheduler(new SerialMergeScheduler());
            try (IndexWriter writer = new IndexWriter(directory, conf)) {
                List<BytesRef> indexedIds = indexDocs(writer, 50);

                try (var directoryReader = StandardDirectoryReader.open(writer)) {
                    assertThat(directoryReader.leaves().size(), is(greaterThan(1)));

                    var foundIds = new HashSet<>();
                    for (LeafReaderContext leaf : directoryReader.leaves()) {
                        var bloomFilter = (BloomFilter) leaf.reader().getBinaryDocValues(IdFieldMapper.NAME);
                        assertThat(bloomFilter.sizeInBytes(), is(greaterThan(0L)));

                        for (BytesRef indexedId : indexedIds) {
                            if (bloomFilter.mayContainValue(IdFieldMapper.NAME, indexedId)) {
                                foundIds.add(indexedId);
                            }
                        }
                    }
                    assertThat(foundIds.containsAll(indexedIds), is(true));
                }

                expectThrows(IllegalStateException.class, () -> writer.forceMerge(1));
            }
        }
    }

    public void testIsAllZeros() throws IOException {
        // Empty input.
        assertThat(isAllZeros(new byte[0]), is(true));

        // Zero-filled input of random length.
        byte[] zeros = new byte[randomIntBetween(1, 2048)];
        assertThat(isAllZeros(zeros), is(true));

        // A non-zero byte inside a long-aligned region (length is a multiple of Long.BYTES) must be detected.
        byte[] longAligned = new byte[randomIntBetween(1, 256) * Long.BYTES];
        longAligned[randomIntBetween(0, longAligned.length - 1)] = (byte) randomIntBetween(1, 255);
        assertThat(isAllZeros(longAligned), is(false));

        // A non-zero byte in the trailing remainder (length isn't a multiple of Long.BYTES) must be detected.
        byte[] trailingRemainder = new byte[randomIntBetween(1, 256) * Long.BYTES + randomIntBetween(1, Long.BYTES - 1)];
        trailingRemainder[trailingRemainder.length - 1] = (byte) randomIntBetween(1, 255);
        assertThat(isAllZeros(trailingRemainder), is(false));

        // Randomized coverage across both outcomes.
        byte[] randomized = new byte[randomIntBetween(0, 2048)];
        boolean hasNonZeroByte = randomized.length > 0 && randomBoolean();
        if (hasNonZeroByte) {
            randomized[randomIntBetween(0, randomized.length - 1)] = (byte) randomIntBetween(1, 255);
        }
        assertThat(isAllZeros(randomized), is(hasNonZeroByte == false));
    }

    private static boolean isAllZeros(byte[] bytes) throws IOException {
        try (Directory directory = new ByteBuffersDirectory()) {
            try (IndexOutput out = directory.createOutput("bitset", IOContext.DEFAULT)) {
                out.writeBytes(bytes, 0, bytes.length);
            }
            try (IndexInput in = directory.openInput("bitset", IOContext.DEFAULT)) {
                return ES94BloomFilterDocValuesFormat.isAllZeros(in.randomAccessSlice(0, bytes.length));
            }
        }
    }

    private static List<BytesRef> indexDocs(IndexWriter writer, int minimumDocs) throws IOException {
        List<BytesRef> indexedIds = new ArrayList<>();
        var docCount = atLeast(minimumDocs);
        for (int i = 0; i < docCount; i++) {
            Document doc = new Document();
            var id = UUIDs.randomBase64UUID();
            indexedIds.add(new BytesRef(id));
            doc.add(new IdField(id));
            doc.add(new StringField("host", "host-" + i, Field.Store.YES));
            doc.add(new LongField("counter", i, Field.Store.YES));
            writer.addDocument(doc);
        }
        return indexedIds;
    }

    private void assertBloomFilterTestsPositiveForExistingDocs(IndexWriter writer, List<BytesRef> indexedIds) throws IOException {
        try (var directoryReader = StandardDirectoryReader.open(writer)) {
            for (LeafReaderContext leaf : directoryReader.leaves()) {
                var bloomFilter = getBloomFilter(leaf);
                // the bloom filter reader is null only if the _id field is not stored during indexing
                assertThat(bloomFilter, is(not(nullValue())));

                for (BytesRef indexedId : indexedIds) {
                    assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, indexedId), is(true));
                }
                assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, new BytesRef("random")), is(oneOf(true, false)));

                assertThat(bloomFilter.mayContainValue(IdFieldMapper.NAME, new BytesRef("12345")), is(oneOf(true, false)));

            }

            var storedFields = directoryReader.storedFields();
            for (int docId = 0; docId < indexedIds.size(); docId++) {
                var document = storedFields.document(docId);
                // The _id field is not actually stored, just used to build the bloom filter
                assertThat(document.get(IdFieldMapper.NAME), nullValue());
                assertThat(document.get("host"), not(nullValue()));
                assertThat(document.get("host"), is(equalTo("host-" + docId)));
                assertThat(document.get("counter"), not(nullValue()));
                assertThat(document.getField("counter").storedValue().getLongValue(), is(equalTo((long) docId)));
            }
        }
    }

    private BloomFilter getBloomFilter(LeafReaderContext leafReaderContext) throws IOException {
        LeafReader reader = leafReaderContext.reader();
        var binaryDocValues = reader.getBinaryDocValues(IdFieldMapper.NAME);

        assertThat(binaryDocValues, is(instanceOf(BloomFilter.class)));
        return (BloomFilter) binaryDocValues;
    }

    static class TestCodec extends AssertingCodec {
        private final ES94BloomFilterDocValuesFormat bloomFilterDocValuesFormat;

        TestCodec(ES94BloomFilterDocValuesFormat bloomFilterDocValuesFormat) {
            this.bloomFilterDocValuesFormat = bloomFilterDocValuesFormat;
        }

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            if (field.equals(IdFieldMapper.NAME)) {
                return bloomFilterDocValuesFormat;
            }
            return super.getDocValuesFormatForField(field);
        }
    }

    // The test relies on the Id field being actually indexed so we can
    // rebuild the bloom filter from the terms without having to rely
    // on the synthetic id terms. That's why we use a new custom type
    // instead of SyntheticIdField. Additionally, we cannot use a regular
    // StringField since we expect it to have doc values configured.
    static class IdField extends Field {

        private static final FieldType TYPE;

        static {
            TYPE = new FieldType();
            TYPE.setIndexOptions(IndexOptions.DOCS);
            TYPE.setDocValuesType(DocValuesType.BINARY);
            TYPE.setTokenized(false);
            TYPE.setOmitNorms(true);
            TYPE.setStored(false);
            TYPE.freeze();
        }

        private final BytesRef binaryValue;

        IdField(String id) {
            super(IdFieldMapper.NAME, TYPE);
            this.binaryValue = new BytesRef(id);
        }

        @Override
        public InvertableType invertableType() {
            return InvertableType.BINARY;
        }

        @Override
        public BytesRef binaryValue() {
            return binaryValue;
        }
    }
}
