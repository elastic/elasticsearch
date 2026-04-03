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
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.lucene.store.IndexOutputOutputStream;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.ByteArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.Numbers.isPowerOfTwo;
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
    public static final int DEFAULT_NUM_HASH_FUNCTIONS = 4;
    public static final int MAX_NUM_HASH_FUNCTIONS = PRIMES.length;
    // Bloom filter sizing uses a three-regime strategy based on document count:
    //
    // Small (≤ DEFAULT_SMALL_SEGMENT_MAX_DOCS): DEFAULT_HIGH_BITS_PER_DOC bits/doc — nominal saturation ~3.1%, typically
    // lower after power-of-two rounding, keeping FP rates low even after OR-merging many filters.
    // Taper (DEFAULT_SMALL_SEGMENT_MAX_DOCS – DEFAULT_LARGE_SEGMENT_MIN_DOCS): bits/doc interpolates linearly from
    // DEFAULT_HIGH_BITS_PER_DOC down to DEFAULT_LOW_BITS_PER_DOC, avoiding a sharp quality cliff at the boundary.
    // Large (≥ DEFAULT_LARGE_SEGMENT_MIN_DOCS): DEFAULT_LOW_BITS_PER_DOC bits/doc flat — at this scale the hard cap at
    // MAX_BLOOM_FILTER_SIZE dominates; higher bits/doc would waste storage without proportional
    // FP-rate benefit.
    //
    // The bloom filter size is always capped at MAX_BLOOM_FILTER_SIZE and rounded up to the next
    // power of two in bytes, so actual saturation is always ≤ the nominal target.
    public static final int MIN_SEGMENT_DOCS = 1;
    public static final int DEFAULT_SMALL_SEGMENT_MAX_DOCS = 160_000;
    public static final int DEFAULT_LARGE_SEGMENT_MIN_DOCS = 320_000;
    // Bits per document for small segments. With k = DEFAULT_NUM_HASH_FUNCTIONS hash functions,
    // the nominal saturation is s = 1 - e^(-k/bpd) ≈ 3.1%. The theoretically exact value for
    // 2% saturation (-k/ln(1-s) with s=0.02) is ~198 bits/doc; 128 is a practical compromise
    // that uses less storage. Power-of-two rounding always inflates the allocated filter (actual
    // bpd ≥ 128), bringing effective saturation to roughly 1.9–3.1% depending on where the
    // doc count falls relative to power-of-two boundaries.
    public static final double DEFAULT_HIGH_BITS_PER_DOC = 128.0;
    // Bits per document for large segments. Also used to determine the doc-count cap at which the
    // flat formula first hits MAX_BLOOM_FILTER_SIZE: cap = MAX_BLOOM_FILTER_SIZE_bytes * 8 / DEFAULT_LOW_BITS_PER_DOC.
    public static final double DEFAULT_LOW_BITS_PER_DOC = 24.0;
    public static final ByteSizeValue MAX_BLOOM_FILTER_SIZE = ByteSizeValue.ofMb(8);
    // Allowed range for the bits-per-doc settings. The lower bound of 8.0 keeps the false-positive
    // rate under ~2.5% even at k=4; below that the filter degrades quickly into noise.
    public static final double MIN_BITS_PER_DOC = 8.0;
    public static final double MAX_BITS_PER_DOC = 256.0;
    // Minimum permitted max_size. Smaller values leave too few bits to track even a single small
    // segment without extreme saturation.
    public static final ByteSizeValue MIN_BLOOM_FILTER_SIZE = ByteSizeValue.ofKb(64);

    private final BigArrays bigArrays;
    private final String bloomFilterFieldName;
    private final boolean optimizedMergeEnabled;
    private final int numHashFunctions;
    private final int smallSegmentMaxDocs;
    private final int largeSegmentMinDocs;
    private final double highBitsPerDoc;
    private final double lowBitsPerDoc;
    private final ByteSizeValue maxBloomFilterSize;

    // Public constructor SPI use for reads only
    public ES94BloomFilterDocValuesFormat() {
        super(FORMAT_NAME);
        bigArrays = null;
        bloomFilterFieldName = null;
        numHashFunctions = 0;
        optimizedMergeEnabled = true;
        smallSegmentMaxDocs = DEFAULT_SMALL_SEGMENT_MAX_DOCS;
        largeSegmentMinDocs = DEFAULT_LARGE_SEGMENT_MIN_DOCS;
        highBitsPerDoc = DEFAULT_HIGH_BITS_PER_DOC;
        lowBitsPerDoc = DEFAULT_LOW_BITS_PER_DOC;
        maxBloomFilterSize = MAX_BLOOM_FILTER_SIZE;
    }

    public ES94BloomFilterDocValuesFormat(BigArrays bigArrays, String bloomFilterFieldName) {
        this(bigArrays, bloomFilterFieldName, true);
    }

    ES94BloomFilterDocValuesFormat(BigArrays bigArrays, String bloomFilterFieldName, boolean optimizedMergeEnabled) {
        this(
            bigArrays,
            bloomFilterFieldName,
            optimizedMergeEnabled,
            DEFAULT_NUM_HASH_FUNCTIONS,
            DEFAULT_SMALL_SEGMENT_MAX_DOCS,
            DEFAULT_LARGE_SEGMENT_MIN_DOCS,
            DEFAULT_HIGH_BITS_PER_DOC,
            DEFAULT_LOW_BITS_PER_DOC,
            MAX_BLOOM_FILTER_SIZE
        );
    }

    public ES94BloomFilterDocValuesFormat(
        BigArrays bigArrays,
        String bloomFilterFieldName,
        boolean optimizedMergeEnabled,
        int numHashFunctions,
        int smallSegmentMaxDocs,
        int largeSegmentMinDocs,
        double highBitsPerDoc,
        double lowBitsPerDoc,
        ByteSizeValue maxBloomFilterSize
    ) {
        super(FORMAT_NAME);
        this.bigArrays = bigArrays;
        this.bloomFilterFieldName = bloomFilterFieldName;
        this.optimizedMergeEnabled = optimizedMergeEnabled;
        this.numHashFunctions = numHashFunctions;
        this.smallSegmentMaxDocs = smallSegmentMaxDocs;
        this.largeSegmentMinDocs = largeSegmentMinDocs;
        this.highBitsPerDoc = highBitsPerDoc;
        this.lowBitsPerDoc = lowBitsPerDoc;
        this.maxBloomFilterSize = maxBloomFilterSize;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        assert bigArrays != null;
        assert numHashFunctions > 0;
        assert numHashFunctions <= PRIMES.length : "Number of hash functions must be <= " + PRIMES.length + " but was " + numHashFunctions;
        return new Writer(state);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        return new Reader(state);
    }

    static int closestPowerOfTwoBloomFilterSizeInBytes(int bloomFilterSizeInBytes) {
        assert bloomFilterSizeInBytes > 0 : "Bloom filter size must be > 0 but was " + bloomFilterSizeInBytes;

        var closestPowerOfTwoBloomFilterSizeInBytes = Integer.highestOneBit(bloomFilterSizeInBytes);
        // Round up to the next power of two if it's necessary
        if (closestPowerOfTwoBloomFilterSizeInBytes < bloomFilterSizeInBytes) {
            closestPowerOfTwoBloomFilterSizeInBytes <<= 1;
        }
        if (closestPowerOfTwoBloomFilterSizeInBytes > MAX_BLOOM_FILTER_SIZE.getBytes()) {
            throw new IllegalArgumentException(
                "bloom filter size ["
                    + ByteSizeValue.ofBytes(bloomFilterSizeInBytes)
                    + "] is too large; "
                    + "must be "
                    + MAX_BLOOM_FILTER_SIZE
                    + " or less (rounded to nearest power of two)"
            );
        }
        return Math.toIntExact(closestPowerOfTwoBloomFilterSizeInBytes);
    }

    class Writer extends DocValuesConsumer {
        private IndexOutput metadataOut;
        private IndexOutput bloomFilterDataOut;
        private final SegmentWriteState state;
        // Lazy initialized
        private BitSetBuffer bitSetBuffer;

        Writer(SegmentWriteState state) throws IOException {
            this.state = state;
            final SegmentInfo segmentInfo = state.segmentInfo;
            final IOContext context = state.context;

            final List<Closeable> toClose = new ArrayList<>(2);
            boolean success = false;
            try {
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
            // Capture maxDoc at flush time when the final document count is known
            var numDocs = state.segmentInfo.maxDoc();
            initBitSetBufferForNewSegment(numDocs);

            var values = valuesProducer.getBinary(field);
            for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
                BytesRef value = values.binaryValue();
                addToBloomFilter(value);
            }
        }

        private void addToBloomFilter(BytesRef value) {
            assert bitSetBuffer != null;

            long hash64 = hash64(value.bytes, value.offset, value.length);
            // First use output splitting to get two hash values out of a single hash function
            int upperHalf = (int) (hash64 >> Integer.SIZE);
            int lowerHalf = (int) hash64;
            // Then use the Kirsch-Mitzenmacher technique to obtain multiple hashes efficiently
            for (int i = 0; i < numHashFunctions; i++) {
                // Use prime numbers as the constant for the KM technique so these don't have a common gcd
                final int hash = (lowerHalf + PRIMES[i] * upperHalf) & 0x7FFF_FFFF; // Clears sign bit, gives positive 31-bit values

                final int posInBitArray = hash & (bitSetBuffer.sizeInBits - 1);
                final int pos = posInBitArray >> 3; // div 8
                final int mask = 1 << (posInBitArray & 7); // mod 8
                final byte val = (byte) (bitSetBuffer.get(pos) | mask);
                bitSetBuffer.set(pos, val);
            }
        }

        @Override
        public void merge(MergeState mergeState) throws IOException {
            BloomFilterReaders bloomFilterReaders = new BloomFilterReaders(mergeState, bloomFilterFieldName);
            if (optimizedMergeEnabled && bloomFilterReaders.supportsOptimizedMerge()) {
                mergeOptimized(bloomFilterReaders);
            } else {
                rebuildBloomFilterFromSegments(mergeState);
            }
        }

        /**
         * Merges bloom filters from multiple segments using a hybrid fold/expand strategy.
         * <p>
         * When merging filters of different sizes (all powers of 2):
         * <ul>
         *   <li><b>Fold</b>: Larger filters are folded down by OR-ing overlapping regions</li>
         *   <li><b>Expand</b>: Smaller filters are expanded by duplicating the bit pattern</li>
         * </ul>
         * This preserves correctness (no false negatives) while allowing efficient bitwise merging
         * without re-hashing elements.
         */
        private void mergeOptimized(BloomFilterReaders bloomFilterReaders) throws IOException {
            if (bloomFilterReaders.maxCount() == 0) {
                return;
            }

            List<Integer> bloomFilterSizes = bloomFilterReaders.sizesInBytes();
            initBitSetBufferForMerge(bloomFilterSizes);

            final var pageSizeInBytes = PageCacheRecycler.PAGE_SIZE_IN_BYTES;
            final var sourcePageScratch = new BytesRef(pageSizeInBytes);
            final var targetPageScratch = new BytesRef(pageSizeInBytes);
            final var firstBloomFilter = new AtomicBoolean(true);
            bloomFilterReaders.forEach(bloomFilterFieldReader -> {
                assert bitSetBuffer != null;
                bloomFilterFieldReader.checkIntegrity();
                final int targetBitSetSizeInBytes = bitSetBuffer.sizeInBytes;

                RandomAccessInput bloomFilterData = bloomFilterFieldReader.bloomFilterIn;
                final int sourceSizeInBytes = bloomFilterFieldReader.getBloomFilterBitSetSizeInBytes();

                if (sourceSizeInBytes >= targetBitSetSizeInBytes) {
                    // Fold: source is larger (or equal), so we partition it into chunks
                    // and OR each chunk into the target. This is equivalent to:
                    // target[i] |= source[i] | source[i + targetSize] | source[i + 2*targetSize] | ...
                    // which matches how hash(x) mod targetSize would map bits from the larger filter.
                    int foldFactor = sourceSizeInBytes / targetBitSetSizeInBytes;
                    for (int i = 0; i < foldFactor; i++) {
                        orRegion(
                            bloomFilterData,
                            i * targetBitSetSizeInBytes,
                            0,
                            targetBitSetSizeInBytes,
                            sourcePageScratch,
                            targetPageScratch,
                            firstBloomFilter.get() && i == 0
                        );
                    }
                } else {
                    // Expand: source is smaller, so we duplicate its bit pattern across the target.
                    // This is equivalent to:
                    // target[i] |= source[i mod sourceSize]
                    // which ensures that queries using hash(x) mod targetSize will still find bits
                    // that were set using hash(x) mod sourceSize, since for powers of 2:
                    // (hash mod targetSize) mod sourceSize == hash mod sourceSize
                    int expandFactor = targetBitSetSizeInBytes / sourceSizeInBytes;
                    for (int i = 0; i < expandFactor; i++) {
                        orRegion(
                            bloomFilterData,
                            0,
                            i * sourceSizeInBytes,
                            sourceSizeInBytes,
                            sourcePageScratch,
                            targetPageScratch,
                            firstBloomFilter.get()
                        );
                    }
                }
                firstBloomFilter.set(false);
            });
        }

        private void orRegion(
            RandomAccessInput source,
            int sourceOffset,
            int targetOffset,
            int length,
            BytesRef sourcePageScratch,
            BytesRef targetPageScratch,
            boolean firstPass
        ) throws IOException {
            assert sourcePageScratch.bytes.length == PageCacheRecycler.PAGE_SIZE_IN_BYTES
                : sourcePageScratch.bytes.length + " vs " + PageCacheRecycler.PAGE_SIZE_IN_BYTES;
            assert targetPageScratch.bytes.length == PageCacheRecycler.PAGE_SIZE_IN_BYTES
                : targetPageScratch.bytes.length + " vs " + PageCacheRecycler.PAGE_SIZE_IN_BYTES;

            // throwaway, just to call bitSetBuffer.get()
            BytesRef scratchRef = new BytesRef();

            int offset = 0;
            while (offset < length) {
                int pageLen = Math.min(PageCacheRecycler.PAGE_SIZE_IN_BYTES, length - offset);

                source.readBytes(sourceOffset + offset, sourcePageScratch.bytes, 0, pageLen);
                var materialized = bitSetBuffer.get(targetOffset + offset, pageLen, scratchRef);
                assert materialized == false : "Unexpected materialized array";

                if (firstPass) {
                    // If we're just processing the first bloom filter the first pass, we can just copy the
                    // bytes from the source bloom filter into the new bloom filter and skip all the OR operations.
                    bitSetBuffer.set(targetOffset + offset, sourcePageScratch.bytes, 0, pageLen);
                } else {
                    // Unfortunately we have to copy the bytes that we read from bitSetBuffer since the
                    // BigArrays ByteArray just provides a view from the page that shouldn't be mutated
                    // (this mostly apply to the initial empty pages which are shared across all the byte buffers).
                    System.arraycopy(scratchRef.bytes, scratchRef.offset, targetPageScratch.bytes, 0, pageLen);

                    int i = 0;
                    for (; i + Long.BYTES <= pageLen; i += Long.BYTES) {
                        long existing = (long) BitUtil.VH_LE_LONG.get(sourcePageScratch.bytes, i);
                        long current = (long) BitUtil.VH_LE_LONG.get(targetPageScratch.bytes, i);
                        BitUtil.VH_LE_LONG.set(targetPageScratch.bytes, i, existing | current);
                    }

                    // OR any remaining bytes if length isn't a multiple of 8.
                    // In practice this only applies for segments with 1 document where the bloom filter size is 4 bytes
                    for (; i < pageLen; i++) {
                        targetPageScratch.bytes[i] |= sourcePageScratch.bytes[i];
                    }

                    bitSetBuffer.set(targetOffset + offset, targetPageScratch.bytes, 0, pageLen);
                }

                offset += pageLen;
            }
        }

        private void rebuildBloomFilterFromSegments(MergeState mergeState) throws IOException {
            final List<Fields> fields = new ArrayList<>();
            final List<ReaderSlice> slices = new ArrayList<>();

            var docCount = Arrays.stream(mergeState.maxDocs).sum();
            initBitSetBufferForNewSegment(docCount);

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
            try {
                if (Assertions.ENABLED) {
                    boolean allNull = (bitSetBuffer == null && bloomFilterDataOut == null && metadataOut == null);
                    boolean allSet = (bitSetBuffer != null && bloomFilterDataOut != null && metadataOut != null);
                    assert allNull || allSet : bitSetBuffer + " vs " + bloomFilterDataOut + " vs " + metadataOut;
                }
                try {
                    BloomFilterMetadata bloomFilterMetadata = null;
                    if (bloomFilterDataOut != null) {
                        bloomFilterMetadata = new BloomFilterMetadata(
                            bloomFilterDataOut.getFilePointer(),
                            bitSetBuffer == null ? 0 : bitSetBuffer.sizeInBits,
                            numHashFunctions
                        );

                        if (bitSetBuffer != null) {
                            bitSetBuffer.writeTo(bloomFilterDataOut);
                        }
                        CodecUtil.writeFooter(bloomFilterDataOut);
                    }
                    if (metadataOut != null) {
                        bloomFilterMetadata.writeTo(metadataOut);
                        CodecUtil.writeFooter(metadataOut);
                    }
                } catch (Throwable t) {
                    IOUtils.closeWhileHandlingException(bitSetBuffer, bloomFilterDataOut, metadataOut);
                    throw t;
                }
                IOUtils.close(bitSetBuffer, bloomFilterDataOut, metadataOut);
            } finally {
                metadataOut = bloomFilterDataOut = null;
                bitSetBuffer = null;
            }
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

        private void initBitSetBufferForNewSegment(int numDocs) {
            int sizeInBytes = bloomFilterSizeInBytesForNewSegment(numDocs);
            initBitSetBuffer(sizeInBytes);
        }

        private void initBitSetBufferForMerge(List<Integer> bloomFilterSizes) {
            var sizeInBytes = bloomFilterSizeInBytesForMergedSegment(bloomFilterSizes);
            initBitSetBuffer(sizeInBytes);
        }

        private void initBitSetBuffer(int sizeInBytes) {
            assert isPowerOfTwo(sizeInBytes);
            if (bitSetBuffer != null) {
                throw new IllegalStateException("BitSetBuffer already exists");
            }

            this.bitSetBuffer = new BitSetBuffer(bigArrays, sizeInBytes);
        }
    }

    static class BitSetBuffer implements Closeable {
        private final int sizeInBits;
        private final int sizeInBytes;
        private final ByteArray buffer;

        BitSetBuffer(BigArrays bigArrays, int sizeInBytes) {
            assert sizeInBytes > 0;
            assert sizeInBytes <= MAX_BLOOM_FILTER_SIZE.getBytes();
            assert isPowerOfTwo(sizeInBytes) : "Expected a power of two size but got " + sizeInBytes;

            this.sizeInBits = Math.multiplyExact(sizeInBytes, Byte.SIZE);
            this.sizeInBytes = sizeInBytes;
            this.buffer = bigArrays.newByteArray(sizeInBytes);
        }

        byte get(int position) {
            return buffer.get(position);
        }

        boolean get(long index, int length, BytesRef bytesRef) {
            return buffer.get(index, length, bytesRef);
        }

        void set(int position, byte value) {
            buffer.set(position, value);
        }

        void set(long index, byte[] buf, int offset, int len) {
            buffer.set(index, buf, offset, len);
        }

        public void writeTo(IndexOutput indexOut) throws IOException {
            if (buffer.hasArray()) {
                indexOut.writeBytes(buffer.array(), 0, sizeInBytes);
            } else {
                BytesReference.fromByteArray(buffer, sizeInBytes)
                    .writeTo(
                        // do not close the stream as it would close indexOut
                        new IndexOutputOutputStream(indexOut)
                    );
            }
        }

        @Override
        public void close() throws IOException {
            buffer.close();
        }
    }

    record BloomFilterReaders(MergeState mergeState, String bloomFilterFieldName) {
        void forEach(CheckedConsumer<BloomFilterFieldReader, IOException> consumer) throws IOException {
            for (int readerIdx = 0; readerIdx < mergeState.docValuesProducers.length; readerIdx++) {
                final FieldInfo fieldInfo = mergeState.fieldInfos[readerIdx].fieldInfo(bloomFilterFieldName);
                if (fieldInfo == null) {
                    continue;
                }
                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[readerIdx];
                var binaryDocValues = docValuesProducer.getBinary(fieldInfo);
                if (binaryDocValues instanceof BloomFilterFieldReader == false) {
                    throw new IllegalStateException("Expected a BloomFilterFieldReader but got " + binaryDocValues.getClass().getName());
                }

                BloomFilterFieldReader bloomFilterFieldReader = (BloomFilterFieldReader) binaryDocValues;
                consumer.accept(bloomFilterFieldReader);
            }
        }

        /**
         * Returns true if all segments have bloom filter readers, allowing for an optimized merge
         * via bitwise OR (fold/expand) rather than rebuilding from the bloomFilterField terms.
         */
        boolean supportsOptimizedMerge() throws IOException {
            for (int readerIdx = 0; readerIdx < mergeState.docValuesProducers.length; readerIdx++) {
                final FieldInfo fieldInfo = mergeState.fieldInfos[readerIdx].fieldInfo(bloomFilterFieldName);
                if (fieldInfo == null) {
                    return false;
                }
                DocValuesProducer docValuesProducer = mergeState.docValuesProducers[readerIdx];
                var binaryDocValues = docValuesProducer.getBinary(fieldInfo);
                if (binaryDocValues instanceof BloomFilterFieldReader == false) {
                    return false;
                }
            }

            return mergeState.docValuesProducers.length > 0;
        }

        List<Integer> sizesInBytes() throws IOException {
            List<Integer> sizes = new ArrayList<>();
            forEach(reader -> sizes.add(reader.getBloomFilterBitSetSizeInBytes()));
            return sizes;
        }

        int maxCount() {
            return mergeState.docValuesProducers.length;
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
                    bloomFilterMetadata.numHashFunctions(),
                    this::checkIntegrity
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
        private final int numHashFunctions;
        private final CheckedRunnable<IOException> checkIntegrityFn;
        // Lazily computed and cached. -1.0 is the sentinel for "not yet computed" (saturation is
        // always in [0.0, 1.0]). Two threads may both compute this concurrently, but the result is
        // identical (the filter is immutable), so the race is benign — the cost is redundant I/O,
        // not incorrect results. volatile ensures the write is visible once complete.
        private volatile double cachedSaturation = -1.0;

        private BloomFilterFieldReader(
            RandomAccessInput bloomFilterIn,
            int bloomFilterBitSetSizeInBits,
            int numHashFunctions,
            CheckedRunnable<IOException> checkIntegrityFn
        ) {
            this.bloomFilterIn = bloomFilterIn;
            this.bloomFilterBitSetSizeInBits = bloomFilterBitSetSizeInBits;
            this.numHashFunctions = numHashFunctions;
            this.checkIntegrityFn = checkIntegrityFn;
        }

        public boolean mayContainValue(String field, BytesRef value) throws IOException {
            long hash64 = hash64(value.bytes, value.offset, value.length);
            // First use output splitting to get two hash values out of a single hash function
            int upperHalf = (int) (hash64 >> Integer.SIZE);
            int lowerHalf = (int) hash64;
            // Then use the Kirsch-Mitzenmacher technique to obtain multiple hashes efficiently
            for (int i = 0; i < numHashFunctions; i++) {
                // Use prime numbers as the constant for the KM technique so these don't have a common gcd
                final int hash = (lowerHalf + PRIMES[i] * upperHalf) & 0x7FFF_FFFF; // Clears sign bit, gives positive 31-bit values

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

        int getBloomFilterBitSetSizeInBytes() {
            return Math.divideExact(bloomFilterBitSetSizeInBits, Byte.SIZE);
        }

        @Override
        public long sizeInBytes() {
            return getBloomFilterBitSetSizeInBytes();
        }

        @Override
        public double saturation() throws IOException {
            if (cachedSaturation >= 0.0) {
                return cachedSaturation;
            }
            final int sizeInBytes = getBloomFilterBitSetSizeInBytes();
            long setBits = 0;
            final byte[] scratch = new byte[PageCacheRecycler.PAGE_SIZE_IN_BYTES];
            int remaining = sizeInBytes;
            int offset = 0;
            while (remaining > 0) {
                int pageLen = Math.min(PageCacheRecycler.PAGE_SIZE_IN_BYTES, remaining);
                bloomFilterIn.readBytes(offset, scratch, 0, pageLen);
                for (int i = 0; i < pageLen; i++) {
                    setBits += Integer.bitCount(scratch[i] & 0xFF);
                }
                offset += pageLen;
                remaining -= pageLen;
            }
            cachedSaturation = (double) setBits / bloomFilterBitSetSizeInBits;
            return cachedSaturation;
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

        void checkIntegrity() throws IOException {
            checkIntegrityFn.run();
        }
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

    private static String bloomFilterMetadataFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_METADATA_BLOOM_FILTER_EXTENSION);
    }

    private static String bloomFilterFileName(SegmentInfo segmentInfo, String segmentSuffix) {
        return IndexFileNames.segmentFileName(segmentInfo.name, segmentSuffix, STORED_FIELDS_BLOOM_FILTER_EXTENSION);
    }

    /**
     * Computes the bloom filter size in bytes for a newly created segment.
     *
     * <p>The sizing strategy balances false-positive accuracy against storage cost
     * depending on the segment's document count:
     *
     * <ul>
     *   <li><b>Small segments (≤ {@value #DEFAULT_SMALL_SEGMENT_MAX_DOCS} docs by default)</b> —
     *       Sized at {@value #DEFAULT_HIGH_BITS_PER_DOC} bits per document by default. Nominal saturation is
     *       ~3.1% (s = 1 − e^(−k/bpd) with k=4); power-of-two rounding typically
     *       inflates the actual filter, bringing effective saturation to ~1.9–3.1%
     *       depending on the document count.
     *
     *   <li><b>Mid-range segments ({@value #DEFAULT_SMALL_SEGMENT_MAX_DOCS} –
     *       {@value #DEFAULT_LARGE_SEGMENT_MIN_DOCS} docs by default)</b> — Bits per document tapers linearly
     *       from {@value #DEFAULT_HIGH_BITS_PER_DOC} down to {@value #DEFAULT_LOW_BITS_PER_DOC}. This avoids a
     *       sharp cliff in filter quality between adjacent segment sizes while
     *       gradually trading accuracy for a smaller storage footprint.
     *
     *   <li><b>Large segments (≥ {@value #DEFAULT_LARGE_SEGMENT_MIN_DOCS} docs by default)</b> — Sized at a
     *       flat {@value #DEFAULT_LOW_BITS_PER_DOC} bits per document. At this scale the hard cap
     *       at {@link #MAX_BLOOM_FILTER_SIZE} dominates; the filter cannot grow
     *       proportionally with doc count regardless of the bits-per-doc ratio.
     * </ul>
     *
     * <p>The result is always passed through {@link #boundAndRoundBloomFilterSizeInBytes}
     * to enforce minimum/maximum size limits and power-of-two rounding.
     *
     * @param numDocs the number of documents in the new segment, must be positive
     * @return bloom filter size in bytes, as a power of two
     */
    public int bloomFilterSizeInBytesForNewSegment(int numDocs) {
        assert numDocs > 0 : "Unexpected number of docs " + numDocs;
        assert MAX_BLOOM_FILTER_SIZE.getBytes() <= Integer.MAX_VALUE : MAX_BLOOM_FILTER_SIZE;

        double bitsPerDoc;
        if (numDocs <= smallSegmentMaxDocs) {
            bitsPerDoc = highBitsPerDoc;
        } else if (numDocs >= largeSegmentMinDocs) {
            bitsPerDoc = lowBitsPerDoc;
        } else {
            double taperFraction = (double) (numDocs - smallSegmentMaxDocs) / (largeSegmentMinDocs - smallSegmentMaxDocs);
            bitsPerDoc = highBitsPerDoc + taperFraction * (lowBitsPerDoc - highBitsPerDoc);
        }

        long sizeInBytes = Math.max(1, (long) (numDocs * bitsPerDoc) / Byte.SIZE);
        return boundAndRoundBloomFilterSizeInBytes(sizeInBytes);
    }

    int bloomFilterSizeInBytesForMergedSegment(List<Integer> segmentSizes) {
        assert segmentSizes.isEmpty() == false : "Expected at least one segment size";

        // Use the max size to preserve the precision of the largest input filter.
        return boundAndRoundBloomFilterSizeInBytes(segmentSizes.stream().mapToInt(Integer::intValue).max().orElseThrow());
    }

    private int boundAndRoundBloomFilterSizeInBytes(long idealSizeInBytes) {
        long boundedSize = Math.min(maxBloomFilterSize.getBytes(), idealSizeInBytes);
        return closestPowerOfTwoBloomFilterSizeInBytes(Math.toIntExact(boundedSize));
    }
}
