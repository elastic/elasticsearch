/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bloomfilter;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.codecs.FieldsProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.MappedMultiFields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.ReaderSlice;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.FilterDocValuesProducer;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.IntSupplier;

import static org.elasticsearch.index.codec.bloomfilter.BloomFilterHashFunctions.MurmurHash3.hash64;

/**
 * A doc values format that builds a Bloom filter for a specific field to enable fast
 * existence checks.
 *
 * <p>The field used to build the Bloom filter is not stored, as only its presence
 * needs to be tracked for filtering purposes. This reduces storage overhead while maintaining
 * the ability to quickly determine if a segment might contain the field.
 *
 * <p><b>File formats</b>
 *
 * <p>Bloom filter doc values are represented by two files:
 *
 * <ol>
 *   <li>
 *       <p>Bloom filter data file (extension .sfbf). This file stores the bloom
 *       filter bitset.
 *   <li>
 *       <p>A bloom filter meta file (extension .sfbfm). This file stores metadata about the
 *       bloom filters stored in the bloom filter data file. The in-memory representation can
 *       be found in {@link BloomFilterMetadata}.
 * </ol>
 */
public class ES94BloomFilterDocValuesFormat extends DocValuesFormat {
    public static final String FORMAT_NAME = "ES94BloomFilterDocValuesFormat";
    public static final String STORED_FIELDS_BLOOM_FILTER_EXTENSION = "sfbf";
    public static final String STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION = "sfbfm";
    private static final int VERSION_START = 0;
    private static final int VERSION_CURRENT = VERSION_START;

    // We use prime numbers with the Kirsch-Mitzenmacher technique to obtain multiple hashes from two hash functions
    private static final int[] PRIMES = new int[] { 2, 5, 11, 17, 23, 29, 41, 47, 53, 59, 71 };
    private static final int DEFAULT_NUM_HASH_FUNCTIONS = 7;
    private static final ByteSizeValue MAX_BLOOM_FILTER_SIZE = ByteSizeValue.ofMb(8);
    public static final ByteSizeValue DEFAULT_BLOOM_FILTER_SIZE = ByteSizeValue.ofMb(1);

    private final BigArrays bigArrays;
    private final String bloomFilterFieldName;
    private final int numHashFunctions;
    private final int bloomFilterSizeInBits;

    // Public constructor SPI use for reads only
    public ES94BloomFilterDocValuesFormat() {
        super(FORMAT_NAME);
        bigArrays = null;
        bloomFilterFieldName = null;
        numHashFunctions = 0;
        bloomFilterSizeInBits = 0;
    }

    public ES94BloomFilterDocValuesFormat(BigArrays bigArrays, ByteSizeValue bloomFilterSize, String bloomFilterFieldName) {
        super(FORMAT_NAME);
        this.bigArrays = bigArrays;
        this.bloomFilterFieldName = bloomFilterFieldName;
        this.numHashFunctions = DEFAULT_NUM_HASH_FUNCTIONS;

        if (bloomFilterSize.getBytes() <= 0) {
            throw new IllegalArgumentException("bloom filter size must be greater than 0");
        }

        this.bloomFilterSizeInBits = closestPowerOfTwoBloomFilterSizeInBits(bloomFilterSize);
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        assert bigArrays != null;
        assert numHashFunctions > 0;
        assert bloomFilterSizeInBits > 0;
        return new Writer(state, bigArrays, numHashFunctions, this::getBloomFilterSizeInBits, bloomFilterFieldName);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new Reader(state);
    }

    int getBloomFilterSizeInBits() {
        return bloomFilterSizeInBits;
    }

    static int closestPowerOfTwoBloomFilterSizeInBits(ByteSizeValue bloomFilterSize) {
        var closestPowerOfTwoBloomFilterSizeInBytes = Long.highestOneBit(bloomFilterSize.getBytes());
        if (closestPowerOfTwoBloomFilterSizeInBytes > MAX_BLOOM_FILTER_SIZE.getBytes()) {
            throw new IllegalArgumentException(
                "bloom filter size ["
                    + bloomFilterSize
                    + "] is too large; "
                    + "must be "
                    + MAX_BLOOM_FILTER_SIZE
                    + " or less (rounded to nearest power of two)"
            );
        }
        return Math.toIntExact(Math.multiplyExact(closestPowerOfTwoBloomFilterSizeInBytes, Byte.SIZE));
    }

    static class Writer extends DocValuesConsumer {
        private final int numHashFunctions;
        private final String bloomFilterFieldName;
        private final List<Closeable> toClose = new ArrayList<>(3);

        private final IndexOutput metadataOut;
        private final IndexOutput bloomFilterDataOut;
        private final int bitsetSizeInBits;
        private final int bitSetSizeInBytes;
        private final ByteArray buffer;
        private final int[] hashes;
        private boolean closed;

        Writer(
            SegmentWriteState state,
            BigArrays bigArrays,
            int numHashFunctions,
            IntSupplier defaultBloomFilterSizeInBitsSupplier,
            String bloomFilterFieldName
        ) throws IOException {
            final SegmentInfo segmentInfo = state.segmentInfo;
            final IOContext context = state.context;
            assert numHashFunctions <= PRIMES.length
                : "Number of hash functions must be <= " + PRIMES.length + " but was " + numHashFunctions;

            this.numHashFunctions = numHashFunctions;
            this.hashes = new int[numHashFunctions];
            this.bloomFilterFieldName = bloomFilterFieldName;

            boolean success = false;
            try {
                int bloomFilterSizeInBits = defaultBloomFilterSizeInBitsSupplier.getAsInt();
                assert isPowerOfTwo(bloomFilterSizeInBits) : "Bloom filter size is not a power of 2: " + bloomFilterSizeInBits;
                this.bitsetSizeInBits = bloomFilterSizeInBits;
                this.bitSetSizeInBytes = bitsetSizeInBits / Byte.SIZE;
                this.buffer = bigArrays.newByteArray(bitSetSizeInBytes);
                toClose.add(buffer);

                metadataOut = state.directory.createOutput(bloomFilterMetadataFileName(segmentInfo, state.segmentSuffix), context);
                toClose.add(metadataOut);
                CodecUtil.writeIndexHeader(metadataOut, FORMAT_NAME, VERSION_CURRENT, segmentInfo.getId(), state.segmentSuffix);

                bloomFilterDataOut = state.directory.createOutput(bloomFilterFileName(segmentInfo, state.segmentSuffix), context);
                toClose.add(bloomFilterDataOut);

                CodecUtil.writeIndexHeader(bloomFilterDataOut, FORMAT_NAME, VERSION_CURRENT, segmentInfo.getId(), state.segmentSuffix);
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(toClose);
                }
            }
        }

        @Override
        public void mergeNumericField(FieldInfo mergeFieldInfo, MergeState mergeState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mergeBinaryField(FieldInfo mergeFieldInfo, MergeState mergeState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mergeSortedNumericField(FieldInfo mergeFieldInfo, MergeState mergeState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mergeSortedField(FieldInfo fieldInfo, MergeState mergeState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void mergeSortedSetField(FieldInfo mergeFieldInfo, MergeState mergeState) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
            assert field.name.equals(bloomFilterFieldName) : "Expected field " + bloomFilterFieldName + " but got " + field.name;
            var values = valuesProducer.getBinary(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                BytesRef value = values.binaryValue();
                addToBloomFilter(value);
            }
        }

        private void addToBloomFilter(BytesRef value) {
            // TODO: consider merging the hashing with the bit array population
            var valueHashes = hashValue(value, hashes);
            for (int hash : valueHashes) {
                final int posInBitArray = hash & (bitsetSizeInBits - 1);
                final int pos = posInBitArray >> 3; // div 8
                final int mask = 1 << (posInBitArray & 7); // mod 8
                final byte val = (byte) (buffer.get(pos) | mask);
                buffer.set(pos, val);
            }
        }

        private void flush() throws IOException {
            BloomFilterMetadata bloomFilterMetadata = new BloomFilterMetadata(
                bloomFilterDataOut.getFilePointer(),
                bitsetSizeInBits,
                numHashFunctions
            );

            if (buffer.hasArray()) {
                bloomFilterDataOut.writeBytes(buffer.array(), 0, bitSetSizeInBytes);
            } else {
                BytesReference.fromByteArray(buffer, bitSetSizeInBytes)
                    .writeTo(
                        // do not close the stream as it would close bloomFilterDataOut
                        new IndexOutputOutputStream(bloomFilterDataOut)
                    );
            }
            CodecUtil.writeFooter(bloomFilterDataOut);

            bloomFilterMetadata.writeTo(metadataOut);
            CodecUtil.writeFooter(metadataOut);
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            if (useOptimizedMerge(mergeState)) {
                mergeOptimized(mergeState);
            } else {
                rebuildBloomFilterFromSegments(mergeState);
            }
        }

        /**
         * Determines whether bloom filters can be merged using a bitwise OR operation.
         *
         * <p>Fast merging is possible when all segments in the merge state satisfy two conditions:
         * <ul>
         *   <li>Each segment has an associated bloom filter
         *   <li>All bloom filters have identical dimensions (same bit array size)
         * </ul>
         *
         * <p>When these conditions are met, the bloom filters can be efficiently combined by
         * performing a bitwise OR across their underlying bitsets, avoiding the need to
         * re-hash and re-insert elements.
         *
         * @param mergeState the merge state containing segments to be merged
         * @return {@code true} if all segments have compatible bloom filters that can be
         *         merged via bitwise OR; {@code false} otherwise
         */
        private boolean useOptimizedMerge(MergeState mergeState) throws IOException {
            int expectedBloomFilterSize = -1;
            for (int i = 0; i < mergeState.docValuesProducers.length; i++) {
                final FieldInfo fieldInfo = mergeState.fieldInfos[i].fieldInfo(bloomFilterFieldName);
                if (fieldInfo == null) {
                    continue;
                }
                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[i];
                if (docValuesProducer instanceof FilterDocValuesProducer filterDocValuesProducer) {
                    docValuesProducer = filterDocValuesProducer.getIn();
                }

                if (docValuesProducer instanceof Reader == false) {
                    return false;
                }

                var reader = (Reader) docValuesProducer;

                if (expectedBloomFilterSize == -1) {
                    expectedBloomFilterSize = reader.bloomFilterMetadata.sizeInBits();
                }

                if (reader.bloomFilterMetadata.sizeInBits() != expectedBloomFilterSize) {
                    return false;
                }
            }
            return true;
        }

        private void mergeOptimized(MergeState mergeState) throws IOException {
            assert useOptimizedMerge(mergeState);

            if (mergeState.fieldsProducers.length == 0) {
                return;
            }

            for (int readerIdx = 0; readerIdx < mergeState.docValuesProducers.length; readerIdx++) {
                final FieldInfo fieldInfo = mergeState.fieldInfos[readerIdx].fieldInfo(bloomFilterFieldName);
                if (fieldInfo == null) {
                    continue;
                }
                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[readerIdx];
                if (docValuesProducer instanceof FilterDocValuesProducer filterDocValuesProducer) {
                    docValuesProducer = filterDocValuesProducer.getIn();
                }

                if (docValuesProducer instanceof Reader == false) {
                    throw new IllegalStateException("Expected a Reader but got " + docValuesProducer);
                }

                var reader = (Reader) docValuesProducer;
                reader.checkIntegrity();

                BloomFilterFieldReader bloomFilterFieldReader = reader.createBloomFilterReader();
                assert bloomFilterFieldReader.getBloomFilterBitSetSizeInBits() == bitsetSizeInBits
                    : "Expected a bloom filter bitset size "
                        + bitsetSizeInBits
                        + " but got "
                        + bloomFilterFieldReader.getBloomFilterBitSetSizeInBits();

                RandomAccessInput bloomFilterData = bloomFilterFieldReader.bloomFilterIn;
                for (int i = 0; i < bitSetSizeInBytes; i++) {
                    var existingBloomFilterByte = bloomFilterData.readByte(i);
                    var resultingBloomFilterByte = buffer.get(i);
                    // TODO: Consider merging more than a byte at a time to speed up the process
                    buffer.set(i, (byte) (existingBloomFilterByte | resultingBloomFilterByte));
                }
            }
        }

        private void rebuildBloomFilterFromSegments(MergeState mergeState) throws IOException {
            final List<Fields> fields = new ArrayList<>();
            final List<ReaderSlice> slices = new ArrayList<>();

            int docBase = 0;
            for (int readerIndex = 0; readerIndex < mergeState.fieldsProducers.length; readerIndex++) {
                final FieldsProducer f = mergeState.fieldsProducers[readerIndex];

                final int maxDoc = mergeState.maxDocs[readerIndex];
                if (f != null) {
                    f.checkIntegrity();
                    slices.add(new ReaderSlice(docBase, maxDoc, readerIndex));
                    fields.add(f);
                }
                docBase += maxDoc;
            }

            Fields mergedFields = new MappedMultiFields(
                mergeState,
                new MultiFields(fields.toArray(Fields.EMPTY_ARRAY), slices.toArray(ReaderSlice.EMPTY_ARRAY))
            );

            var terms = mergedFields.terms(bloomFilterFieldName);
            if (terms == null) {
                return;
            }

            // TODO: use terms.docCount to compute an optimal bloom filter size
            final TermsEnum termsEnum = terms.iterator();
            while (true) {
                final BytesRef term = termsEnum.next();
                if (term == null) {
                    break;
                }
                addToBloomFilter(term);
            }
        }

        @Override
        public void close() throws IOException {
            if (closed) {
                return;
            }

            closed = true;
            flush();
            IOUtils.close(toClose);
        }

        @Override
        public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throwUnsupportedOperationException();
        }

        @Override
        public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) {
            throwUnsupportedOperationException();
        }

        @Override
        public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) {
            throwUnsupportedOperationException();
        }

        @Override
        public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) {
            throwUnsupportedOperationException();
        }

        private void throwUnsupportedOperationException() {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    static class Reader extends DocValuesProducer {
        private final IndexInput bloomFilterData;
        private final BloomFilterMetadata bloomFilterMetadata;

        Reader(SegmentReadState state) throws IOException {
            final Directory directory = state.directory;
            final SegmentInfo si = state.segmentInfo;
            final String segmentSuffix = state.segmentSuffix;
            final IOContext context = state.context;

            IndexInput bloomFilterData = null;
            boolean success = false;
            try (var metaInput = directory.openChecksumInput(bloomFilterMetadataFileName(si, segmentSuffix))) {
                var metadataVersion = CodecUtil.checkIndexHeader(
                    metaInput,
                    FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );
                BloomFilterMetadata bloomFilterMetadata = BloomFilterMetadata.readFrom(metaInput);
                CodecUtil.checkFooter(metaInput);

                bloomFilterData = directory.openInput(bloomFilterFileName(si, segmentSuffix), context);
                var bloomFilterDataVersion = CodecUtil.checkIndexHeader(
                    bloomFilterData,
                    FORMAT_NAME,
                    VERSION_START,
                    VERSION_CURRENT,
                    si.getId(),
                    segmentSuffix
                );

                if (metadataVersion != bloomFilterDataVersion) {
                    throw new CorruptIndexException(
                        "Format versions mismatch: meta=" + metadataVersion + ", data=" + bloomFilterDataVersion,
                        bloomFilterData
                    );
                }
                CodecUtil.retrieveChecksum(bloomFilterData);

                this.bloomFilterData = bloomFilterData;
                this.bloomFilterMetadata = bloomFilterMetadata;
                success = true;
            } finally {
                if (success == false) {
                    IOUtils.closeWhileHandlingException(bloomFilterData);
                }
            }
        }

        @Override
        public void checkIntegrity() throws IOException {
            bloomFilterData.prefetch(0, bloomFilterMetadata.sizeInBytes());
            CodecUtil.checksumEntireFile(bloomFilterData);
        }

        @Override
        public void close() throws IOException {
            bloomFilterData.close();
        }

        @Override
        public BinaryDocValues getBinary(FieldInfo field) {
            return createBloomFilterReader();
        }

        private BloomFilterFieldReader createBloomFilterReader() {
            try {
                // Ensure that the page cache is pre-populated
                bloomFilterData.prefetch(bloomFilterMetadata.fileOffset(), bloomFilterMetadata.sizeInBytes());
                return new BloomFilterFieldReader(
                    bloomFilterData.randomAccessSlice(bloomFilterMetadata.fileOffset(), bloomFilterMetadata.sizeInBytes()),
                    bloomFilterMetadata.sizeInBits(),
                    bloomFilterMetadata.numHashFunctions()
                );
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public NumericDocValues getNumeric(FieldInfo field) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public SortedDocValues getSorted(FieldInfo field) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public SortedNumericDocValues getSortedNumeric(FieldInfo field) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public SortedSetDocValues getSortedSet(FieldInfo field) {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public DocValuesSkipper getSkipper(FieldInfo field) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }

    static class BloomFilterFieldReader extends BinaryDocValues implements BloomFilter {
        private final RandomAccessInput bloomFilterIn;
        private final int bloomFilterBitSetSizeInBits;
        private final int[] hashes;

        private BloomFilterFieldReader(RandomAccessInput bloomFilterIn, int bloomFilterBitSetSizeInBits, int numHashFunctions) {
            this.bloomFilterIn = bloomFilterIn;
            this.bloomFilterBitSetSizeInBits = bloomFilterBitSetSizeInBits;
            this.hashes = new int[numHashFunctions];
        }

        public boolean mayContainValue(String field, BytesRef value) throws IOException {
            var valueHashes = hashValue(value, hashes);
            // TODO: consider merging the hashing with the bit array reads

            for (int hash : valueHashes) {
                final int posInBitArray = hash & (bloomFilterBitSetSizeInBits - 1);
                final int pos = posInBitArray >> 3; // div 8
                final int mask = 1 << (posInBitArray & 7); // mod 8
                final byte bits = bloomFilterIn.readByte(pos);
                if ((bits & mask) == 0) {
                    return false;
                }
            }
            return true;
        }

        int getBloomFilterBitSetSizeInBits() {
            return bloomFilterBitSetSizeInBits;
        }

        @Override
        public int docID() {
            return -1;
        }

        @Override
        public int nextDoc() {
            return NO_MORE_DOCS;
        }

        @Override
        public int advance(int target) {
            return NO_MORE_DOCS;
        }

        @Override
        public long cost() {
            return 0;
        }

        @Override
        public boolean advanceExact(int target) {
            return false;
        }

        @Override
        public BytesRef binaryValue() {
            return null;
        }

        @Override
        public void close() throws IOException {}
    }

    record BloomFilterMetadata(long fileOffset, int sizeInBits, int numHashFunctions) {
        BloomFilterMetadata {
            assert isPowerOfTwo(sizeInBits);
        }

        int sizeInBytes() {
            return sizeInBits / Byte.SIZE;
        }

        void writeTo(IndexOutput indexOut) throws IOException {
            indexOut.writeVLong(fileOffset);
            indexOut.writeVInt(sizeInBits);
            indexOut.writeVInt(numHashFunctions);
        }

        static BloomFilterMetadata readFrom(IndexInput in) throws IOException {
            final long fileOffset = in.readVLong();
            final int bloomFilterSizeInBits = in.readVInt();
            final int numOfHashFunctions = in.readVInt();
            return new BloomFilterMetadata(fileOffset, bloomFilterSizeInBits, numOfHashFunctions);
        }
    }

    private static int[] hashValue(BytesRef value, int[] outputs) {
        long hash64 = hash64(value.bytes, value.offset, value.length);
        // First use output splitting to get two hash values out of a single hash function
        int upperHalf = (int) (hash64 >> Integer.SIZE);
        int lowerHalf = (int) hash64;
        // Then use the Kirsch-Mitzenmacher technique to obtain multiple hashes efficiently
        for (int i = 0; i < outputs.length; i++) {
            // Use prime numbers as the constant for the KM technique so these don't have a common gcd
            outputs[i] = (lowerHalf + PRIMES[i] * upperHalf) & 0x7FFF_FFFF; // Clears sign bit, gives positive 31-bit values
        }
        return outputs;
    }

    private static boolean isPowerOfTwo(int value) {
        return (value & (value - 1)) == 0;
    }

    private static String bloomFilterMetadataFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION);
    }

    private static String bloomFilterFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_BLOOM_FILTER_EXTENSION);
    }
}
